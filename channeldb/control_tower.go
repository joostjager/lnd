package channeldb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/coreos/bbolt"
	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/multimutex"
	"github.com/lightningnetwork/lnd/routing/route"
)

var (
	// ErrAlreadyPaid signals we have already paid this payment hash.
	ErrAlreadyPaid = errors.New("invoice is already paid")

	// ErrPaymentInFlight signals that payment for this payment hash is
	// already "in flight" on the network.
	ErrPaymentInFlight = errors.New("payment is in transition")

	// ErrPaymentNotInitiated is returned  if payment wasn't initiated in
	// switch.
	ErrPaymentNotInitiated = errors.New("payment isn't initiated")

	// ErrPaymentAlreadySucceeded is returned in the event we attempt to
	// change the status of a payment already succeeded.
	ErrPaymentAlreadySucceeded = errors.New("payment is already succeeded")

	// ErrPaymentAlreadyFailed is returned in the event we attempt to
	// re-fail a failed payment.
	ErrPaymentAlreadyFailed = errors.New("payment has already failed")

	// ErrUnknownPaymentStatus is returned when we do not recognize the
	// existing state of a payment.
	ErrUnknownPaymentStatus = errors.New("unknown payment status")

	// errNoAttemptInfo is returned when no attempt info is stored yet.
	errNoAttemptInfo = errors.New("unable to find attempt info for " +
		"inflight payment")
)

// ControlTower tracks all outgoing payments made, whose primary purpose is to
// prevent duplicate payments to the same payment hash. In production, a
// persistent implementation is preferred so that tracking can survive across
// restarts. Payments are transitioned through various payment states, and the
// ControlTower interface provides access to driving the state transitions.
type ControlTower interface {
	// InitPayment atomically moves the payment into the InFlight state.
	// This method checks that no suceeded payment exist for this payment
	// hash.
	InitPayment(lntypes.Hash, *PaymentCreationInfo) error

	// RegisterAttempt atomically records the provided PaymentAttemptInfo.
	RegisterAttempt(lntypes.Hash, *PaymentAttemptInfo) error

	// Success transitions a payment into the Succeeded state. After
	// invoking this method, InitPayment should always return an error to
	// prevent us from making duplicate payments to the same payment hash.
	// The provided preimage is atomically saved to the DB for record
	// keeping.
	Success(lntypes.Hash, lntypes.Preimage) error

	// Fail transitions a payment into the Failed state, and records the
	// reason the payment failed. After invoking this method, InitPayment
	// should return nil on its next call for this payment hash, allowing
	// the switch to make a subsequent payment.
	Fail(lntypes.Hash, FailureReason) error

	// FetchInFlightPayments returns all payments with status InFlight.
	FetchInFlightPayments() ([]*InFlightPayment, error)

	// SubscribePayment subscribes to updates for the payment with the given
	// hash. It returns a boolean indicating whether the payment is still in
	// flight and a channel that provides the final outcome of the payment.
	SubscribePayment(paymentHash lntypes.Hash) (bool, chan PaymentEvent,
		error)
}

// paymentControl is persistent implementation of ControlTower to restrict
// double payment sending.
type paymentControl struct {
	db *DB

	subscribers    map[lntypes.Hash][]chan PaymentEvent
	subscribersMtx sync.Mutex
	hashMutex      *multimutex.HashMutex
}

// PaymentEvent is the struct describing the events received by payment
// subscribers.
type PaymentEvent struct {
	Status        PaymentStatus
	Preimage      lntypes.Preimage
	Route         *route.Route
	FailureReason FailureReason
}

// NewPaymentControl creates a new instance of the paymentControl.
func NewPaymentControl(db *DB) ControlTower {
	return &paymentControl{
		db:          db,
		subscribers: make(map[lntypes.Hash][]chan PaymentEvent),
		hashMutex:   multimutex.NewHashMutex(),
	}
}

// InitPayment checks or records the given PaymentCreationInfo with the DB,
// making sure it does not already exist as an in-flight payment. Then this
// method returns successfully, the payment is guranteeed to be in the InFlight
// state.
func (p *paymentControl) InitPayment(paymentHash lntypes.Hash,
	info *PaymentCreationInfo) error {

	var b bytes.Buffer
	if err := serializePaymentCreationInfo(&b, info); err != nil {
		return err
	}
	infoBytes := b.Bytes()

	var updateErr error
	err := p.db.Batch(func(tx *bbolt.Tx) error {
		// Reset the update error, to avoid carrying over an error
		// from a previous execution of the batched db transaction.
		updateErr = nil

		bucket, err := createPaymentBucket(tx, paymentHash)
		if err != nil {
			return err
		}

		// Get the existing status of this payment, if any.
		paymentStatus := fetchPaymentStatus(bucket)

		switch paymentStatus {

		// We allow retrying failed payments.
		case StatusFailed:

		// This is a new payment that is being initialized for the
		// first time.
		case StatusUnknown:

		// We already have an InFlight payment on the network. We will
		// disallow any new payments.
		case StatusInFlight:
			updateErr = ErrPaymentInFlight
			return nil

		// We've already succeeded a payment to this payment hash,
		// forbid the switch from sending another.
		case StatusSucceeded:
			updateErr = ErrAlreadyPaid
			return nil

		default:
			updateErr = ErrUnknownPaymentStatus
			return nil
		}

		// Obtain a new sequence number for this payment. This is used
		// to sort the payments in order of creation, and also acts as
		// a unique identifier for each payment.
		sequenceNum, err := nextPaymentSequence(tx)
		if err != nil {
			return err
		}

		err = bucket.Put(paymentSequenceKey, sequenceNum)
		if err != nil {
			return err
		}

		// Add the payment info to the bucket, which contains the
		// static information for this payment
		err = bucket.Put(paymentCreationInfoKey, infoBytes)
		if err != nil {
			return err
		}

		// We'll delete any lingering attempt info to start with, in
		// case we are initializing a payment that was attempted
		// earlier, but left in a state where we could retry.
		err = bucket.Delete(paymentAttemptInfoKey)
		if err != nil {
			return err
		}

		// Also delete any lingering failure info now that we are
		// re-attempting.
		return bucket.Delete(paymentFailInfoKey)
	})
	if err != nil {
		return nil
	}

	return updateErr
}

// RegisterAttempt atomically records the provided PaymentAttemptInfo to the
// DB.
func (p *paymentControl) RegisterAttempt(paymentHash lntypes.Hash,
	attempt *PaymentAttemptInfo) error {

	// Serialize the information before opening the db transaction.
	var a bytes.Buffer
	if err := serializePaymentAttemptInfo(&a, attempt); err != nil {
		return err
	}
	attemptBytes := a.Bytes()

	var updateErr error
	err := p.db.Batch(func(tx *bbolt.Tx) error {
		// Reset the update error, to avoid carrying over an error
		// from a previous execution of the batched db transaction.
		updateErr = nil

		bucket, err := fetchPaymentBucket(tx, paymentHash)
		if err == ErrPaymentNotInitiated {
			updateErr = ErrPaymentNotInitiated
			return nil
		} else if err != nil {
			return err
		}

		// We can only register attempts for payments that are
		// in-flight.
		if err := ensureInFlight(bucket); err != nil {
			updateErr = err
			return nil
		}

		// Add the payment attempt to the payments bucket.
		return bucket.Put(paymentAttemptInfoKey, attemptBytes)
	})
	if err != nil {
		return err
	}

	return updateErr
}

// Success transitions a payment into the Succeeded state. After invoking this
// method, InitPayment should always return an error to prevent us from making
// duplicate payments to the same payment hash. The provided preimage is
// atomically saved to the DB for record keeping.
func (p *paymentControl) Success(paymentHash lntypes.Hash,
	preimage lntypes.Preimage) error {

	// Lock hash to prevent the following race: payment status is updated to
	// succeeded, new subscriber is registered and will immediately receive
	// the succeeded status, this function sends out the succeeded
	// notification again to the same subscriber.
	p.hashMutex.Lock(paymentHash)
	defer p.hashMutex.Unlock(paymentHash)

	var (
		updateErr error
		attempt   *PaymentAttemptInfo
	)
	err := p.db.Batch(func(tx *bbolt.Tx) error {
		// Reset the update error, to avoid carrying over an error
		// from a previous execution of the batched db transaction.
		updateErr = nil

		bucket, err := fetchPaymentBucket(tx, paymentHash)
		if err == ErrPaymentNotInitiated {
			updateErr = ErrPaymentNotInitiated
			return nil
		} else if err != nil {
			return err
		}

		// We can only mark in-flight payments as succeeded.
		if err := ensureInFlight(bucket); err != nil {
			updateErr = err
			return nil
		}

		// Record the successful payment info atomically to the
		// payments record.
		err = bucket.Put(paymentSettleInfoKey, preimage[:])
		if err != nil {
			return err
		}

		// Retrieve attempt info for the notification.
		attempt, err = fetchPaymentAttempt(bucket)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	if updateErr == nil {
		// Notify subscribers of success event.
		p.notifyFinalEvent(
			paymentHash, PaymentEvent{
				Status:   StatusSucceeded,
				Preimage: preimage,
				Route:    &attempt.Route,
			},
		)
	}

	return updateErr

}

// Fail transitions a payment into the Failed state, and records the reason the
// payment failed. After invoking this method, InitPayment should return nil on
// its next call for this payment hash, allowing the switch to make a
// subsequent payment.
func (p *paymentControl) Fail(paymentHash lntypes.Hash,
	reason FailureReason) error {

	// Lock hash to prevent the race condition described in the Success
	// method.
	p.hashMutex.Lock(paymentHash)
	defer p.hashMutex.Unlock(paymentHash)

	var updateErr error
	err := p.db.Batch(func(tx *bbolt.Tx) error {
		// Reset the update error, to avoid carrying over an error
		// from a previous execution of the batched db transaction.
		updateErr = nil

		bucket, err := fetchPaymentBucket(tx, paymentHash)
		if err == ErrPaymentNotInitiated {
			updateErr = ErrPaymentNotInitiated
			return nil
		} else if err != nil {
			return err
		}

		// We can only mark in-flight payments as failed.
		if err := ensureInFlight(bucket); err != nil {
			updateErr = err
			return nil
		}

		// Put the failure reason in the bucket for record keeping.
		v := []byte{byte(reason)}
		return bucket.Put(paymentFailInfoKey, v)
	})
	if err != nil {
		return err
	}

	if updateErr == nil {
		// Notify subscribers of fail event.
		p.notifyFinalEvent(
			paymentHash, PaymentEvent{
				Status:        StatusFailed,
				FailureReason: reason,
			},
		)
	}

	return updateErr
}

// createPaymentBucket creates or fetches the sub-bucket assigned to this
// payment hash.
func createPaymentBucket(tx *bbolt.Tx, paymentHash lntypes.Hash) (
	*bbolt.Bucket, error) {

	payments, err := tx.CreateBucketIfNotExists(paymentsRootBucket)
	if err != nil {
		return nil, err
	}

	return payments.CreateBucketIfNotExists(paymentHash[:])
}

// fetchPaymentBucket fetches the sub-bucket assigned to this payment hash. If
// the bucket does not exist, it returns ErrPaymentNotInitiated.
func fetchPaymentBucket(tx *bbolt.Tx, paymentHash lntypes.Hash) (
	*bbolt.Bucket, error) {

	payments := tx.Bucket(paymentsRootBucket)
	if payments == nil {
		return nil, ErrPaymentNotInitiated
	}

	bucket := payments.Bucket(paymentHash[:])
	if bucket == nil {
		return nil, ErrPaymentNotInitiated
	}

	return bucket, nil

}

// nextPaymentSequence returns the next sequence number to store for a new
// payment.
func nextPaymentSequence(tx *bbolt.Tx) ([]byte, error) {
	payments, err := tx.CreateBucketIfNotExists(paymentsRootBucket)
	if err != nil {
		return nil, err
	}

	seq, err := payments.NextSequence()
	if err != nil {
		return nil, err
	}

	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, seq)
	return b, nil
}

// fetchPaymentStatus fetches the payment status of the payment. If the payment
// isn't found, it will default to "StatusUnknown".
func fetchPaymentStatus(bucket *bbolt.Bucket) PaymentStatus {
	if bucket.Get(paymentSettleInfoKey) != nil {
		return StatusSucceeded
	}

	if bucket.Get(paymentFailInfoKey) != nil {
		return StatusFailed
	}

	if bucket.Get(paymentCreationInfoKey) != nil {
		return StatusInFlight
	}

	return StatusUnknown
}

// ensureInFlight checks whether the payment found in the given bucket has
// status InFlight, and returns an error otherwise. This should be used to
// ensure we only mark in-flight payments as succeeded or failed.
func ensureInFlight(bucket *bbolt.Bucket) error {
	paymentStatus := fetchPaymentStatus(bucket)

	switch {

	// The payment was indeed InFlight, return.
	case paymentStatus == StatusInFlight:
		return nil

	// Our records show the payment as unknown, meaning it never
	// should have left the switch.
	case paymentStatus == StatusUnknown:
		return ErrPaymentNotInitiated

	// The payment succeeded previously.
	case paymentStatus == StatusSucceeded:
		return ErrPaymentAlreadySucceeded

	// The payment was already failed.
	case paymentStatus == StatusFailed:
		return ErrPaymentAlreadyFailed

	default:
		return ErrUnknownPaymentStatus
	}
}

// fetchPaymentAttempt fetches the payment attempt from the bucket.
func fetchPaymentAttempt(bucket *bbolt.Bucket) (*PaymentAttemptInfo, error) {
	attemptData := bucket.Get(paymentAttemptInfoKey)
	if attemptData == nil {
		return nil, errNoAttemptInfo
	}

	r := bytes.NewReader(attemptData)
	return deserializePaymentAttemptInfo(r)
}

// InFlightPayment is a wrapper around a payment that has status InFlight.
type InFlightPayment struct {
	// Info is the PaymentCreationInfo of the in-flight payment.
	Info *PaymentCreationInfo

	// Attempt contains information about the last payment attempt that was
	// made to this payment hash.
	//
	// NOTE: Might be nil.
	Attempt *PaymentAttemptInfo
}

// SubscribePayment subscribes to updates for the payment with the given hash.
// It returns a boolean indicating whether the payment is still in flight and a
// channel that provides the final outcome of the payment.
func (p *paymentControl) SubscribePayment(paymentHash lntypes.Hash) (
	bool, chan PaymentEvent, error) {

	// Create a channel with buffer size 1. For every payment there will be
	// exactly one event sent.
	c := make(chan PaymentEvent, 1)

	// Lock the hash to prevent a race condition with subscribing and an
	// update from router.
	p.hashMutex.Lock(paymentHash)
	defer p.hashMutex.Unlock(paymentHash)

	var inFlight bool

	err := p.db.View(func(tx *bbolt.Tx) error {
		bucket, err := fetchPaymentBucket(tx, paymentHash)
		if err != nil {
			return err
		}

		var event PaymentEvent

		status := fetchPaymentStatus(bucket)
		switch status {

		// Payment is currently in flight. Register this subscriber and
		// return without writing a result to the channel yet.
		case StatusInFlight:
			p.subscribersMtx.Lock()

			subscribers := p.subscribers[paymentHash]
			subscribers = append(subscribers, c)
			p.subscribers[paymentHash] = subscribers
			p.subscribersMtx.Unlock()

			inFlight = true
			return nil

		// Payment already succeeded. It is not necessary to register as
		// a subscriber, because we can send the result on the channel
		// immediately.
		case StatusSucceeded:
			event.Status = StatusSucceeded

			preimageSlice := bucket.Get(paymentSettleInfoKey)
			if preimageSlice == nil {
				return errors.New("missing preimage")
			}
			var err error
			event.Preimage, err = lntypes.MakePreimage(preimageSlice)
			if err != nil {
				return err
			}

			a, err := fetchPaymentAttempt(bucket)
			if err != nil {
				return err
			}
			event.Route = &a.Route

		// Payment already failed. It is not necessary to register as a
		// subscriber, because we can send the result on the channel
		// immediately.
		case StatusFailed:
			failureReasonSlice := bucket.Get(paymentFailInfoKey)
			if failureReasonSlice == nil {
				return errors.New("missing failure reason")
			}

			if len(failureReasonSlice) != 1 {
				return errors.New("invalid failure reason data")
			}

			event.Status = StatusFailed
			event.FailureReason = FailureReason(
				failureReasonSlice[0],
			)

		default:
			return errors.New("unknown payment status")
		}

		// Write immediate result to the channel.
		c <- event
		close(c)

		return nil
	})
	if err != nil {
		return false, nil, err
	}

	return inFlight, c, nil
}

// notifyFinalEvent sends a final payment event to all subscribers of this
// payment. The channel will be closed after this.
func (p *paymentControl) notifyFinalEvent(paymentHash lntypes.Hash,
	event PaymentEvent) {

	// Get all subscribers for this hash. As there is only a single outcome,
	// the subscriber list can be cleared.
	p.subscribersMtx.Lock()
	list, ok := p.subscribers[paymentHash]
	if !ok {
		p.subscribersMtx.Unlock()
		return
	}
	delete(p.subscribers, paymentHash)
	p.subscribersMtx.Unlock()

	// Notify all subscribers of the event. The subscriber channel is
	// buffered, so it cannot block here.
	for _, subscriber := range list {
		subscriber <- event
		close(subscriber)
	}

}

// FetchInFlightPayments returns all payments with status InFlight.
func (p *paymentControl) FetchInFlightPayments() ([]*InFlightPayment, error) {
	var inFlights []*InFlightPayment
	err := p.db.View(func(tx *bbolt.Tx) error {
		payments := tx.Bucket(paymentsRootBucket)
		if payments == nil {
			return nil
		}

		return payments.ForEach(func(k, _ []byte) error {
			bucket := payments.Bucket(k)
			if bucket == nil {
				return fmt.Errorf("non bucket element")
			}

			// If the status is not InFlight, we can return early.
			paymentStatus := fetchPaymentStatus(bucket)
			if paymentStatus != StatusInFlight {
				return nil
			}

			var (
				inFlight = &InFlightPayment{}
				err      error
			)

			// Get the CreationInfo.
			b := bucket.Get(paymentCreationInfoKey)
			if b == nil {
				return fmt.Errorf("unable to find creation " +
					"info for inflight payment")
			}

			r := bytes.NewReader(b)
			inFlight.Info, err = deserializePaymentCreationInfo(r)
			if err != nil {
				return err
			}

			// Now get the attempt info. It could be that there is
			// no attempt info yet.
			inFlight.Attempt, err = fetchPaymentAttempt(bucket)
			if err != nil && err != errNoAttemptInfo {
				return err
			}

			inFlights = append(inFlights, inFlight)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	return inFlights, nil
}
