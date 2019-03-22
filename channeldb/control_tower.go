package channeldb

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/coreos/bbolt"
	"github.com/lightningnetwork/lnd/lntypes"
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

	// ErrPaymentAlreadyCompleted is returned in the event we attempt to
	// recomplete a completed payment.
	ErrPaymentAlreadyCompleted = errors.New("payment is already completed")

	// ErrPaymentAlreadyFailed is returned in the event we attempt to
	// re-fail a failed payment.
	ErrPaymentAlreadyFailed = errors.New("payment has already failed")

	// ErrUnknownPaymentStatus is returned when we do not recognize the
	// existing state of a payment.
	ErrUnknownPaymentStatus = errors.New("unknown payment status")
)

// ControlTower tracks all outgoing payments made, whose primary purpose is to
// prevent duplicate payments to the same payment hash. In production, a
// persistent implementation is preferred so that tracking can survive across
// restarts. Payments are transitioned through various payment states, and the
// ControlTower interface provides access to driving the state transitions.
type ControlTower interface {
	// InitPayment atomically moves the payment into the InFlight state.
	// This method checks that no completed payment exist for this payment
	// hash.
	InitPayment(lntypes.Hash, *CreationInfo) error

	// RegisterAttempt atomically records the provided AttemptInfo.
	RegisterAttempt(lntypes.Hash, *AttemptInfo) error

	// Success transitions a payment into the Completed state. After
	// invoking this method, InitPayment should always return an error to
	// prevent us from making duplicate payments to the same payment hash.
	// The provided preimage is atomically saved to the DB for record
	// keeping.
	Success(lntypes.Hash, lntypes.Preimage) error

	// Fail transitions a payment into the Failed state. After invoking
	// this method, InitPayment should return nil on its next call for this
	// payment hash, allowing the switch to make a subsequent payment.
	Fail(lntypes.Hash) error

	// FetchInFlightPayments returns all payments with status InFlight.
	FetchInFlightPayments() ([]*InFlightPayment, error)

	SubscribePayment(paymentHash lntypes.Hash) (*PaymentSubscriber, error)
}

// paymentControl is persistent implementation of ControlTower to restrict
// double payment sending.
type paymentControl struct {
	db *DB

	subscribers      map[lntypes.Hash]map[int]*PaymentSubscriber
	subscribersMtx   sync.Mutex
	nextSubscriberID int
}

// PaymentEvent are the events received by payment subscribers.
type PaymentEvent struct {
	Status   PaymentStatus
	Preimage lntypes.Preimage
	Route    *route.Route
}

// NewPaymentControl creates a new instance of the paymentControl.
func NewPaymentControl(db *DB) ControlTower {
	return &paymentControl{
		db:          db,
		subscribers: make(map[lntypes.Hash]map[int]*PaymentSubscriber),
	}
}

// PaymentSubscriber contains the subscriber state.
type PaymentSubscriber struct {
	PaymentHash lntypes.Hash
	Events      chan PaymentEvent

	id int
	p  *paymentControl
}

// Cancel cancels the subscription.
func (p *PaymentSubscriber) Cancel() error {
	p.p.subscribersMtx.Lock()
	defer p.p.subscribersMtx.Unlock()

	subscribers, ok := p.p.subscribers[p.PaymentHash]
	if !ok {
		return errors.New("unknown subscriber")
	}
	delete(subscribers, p.id)

	return nil
}

// InitPayment checks or records the given CreationInfo with the DB, making
// sure it does not already exist as an in-flight payment. Then this method
// returns successfully, the payment is guranteeed to be in the InFlight state.
func (p *paymentControl) InitPayment(paymentHash lntypes.Hash,
	info *CreationInfo) error {

	var b bytes.Buffer
	if err := serializeCreationInfo(&b, info); err != nil {
		return err
	}
	infoBytes := b.Bytes()

	var takeoffErr error
	err := p.db.Batch(func(tx *bbolt.Tx) error {
		bucket, err := fetchPaymentBucket(tx, paymentHash)
		if err != nil {
			return err
		}

		// Get the existing status of this payment, if any.
		paymentStatus := fetchPaymentStatus(bucket)

		// Reset the takeoff error, to avoid carrying over an error
		// from a previous execution of the batched db transaction.
		takeoffErr = nil

		switch paymentStatus {

		// We allow retrying failed payments.
		case StatusFailed:

		// This is a new payment that is being initialized for the
		// first time.
		case StatusGrounded:

		// We already have an InFlight payment on the network. We will
		// disallow any new payments.
		case StatusInFlight:
			takeoffErr = ErrPaymentInFlight
			return nil

		// We've already completed a payment to this payment hash,
		// forbid the switch from sending another.
		case StatusCompleted:
			takeoffErr = ErrAlreadyPaid
			return nil

		default:
			takeoffErr = ErrUnknownPaymentStatus
			return nil
		}

		// We'll move it into the InFlight state.
		paymentStatus = StatusInFlight
		err = bucket.Put(paymentStatusKey, StatusInFlight.Bytes())
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

		return nil
	})
	if err != nil {
		return nil
	}

	return takeoffErr
}

// RegisterAttempt atomically records the provided AttemptInfo to the DB.
func (p *paymentControl) RegisterAttempt(paymentHash lntypes.Hash,
	attempt *AttemptInfo) error {

	// Serialize the information before opening the db transaction.
	var a bytes.Buffer
	if err := serializeAttemptInfo(&a, attempt); err != nil {
		return err
	}
	attemptBytes := a.Bytes()

	return p.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := fetchPaymentBucket(tx, paymentHash)
		if err != nil {
			return err
		}

		// Add the payment attempt to the payments bucket.
		err = bucket.Put(paymentAttemptInfoKey, attemptBytes)
		if err != nil {
			return err
		}

		// TODO: Not necessary?
		p.notifySubscribers(
			paymentHash, PaymentEvent{
				Status: StatusInFlight,
			},
		)

		return nil
	})
}

// Success transitions a payment into the Completed state. After invoking this
// method, InitPayment should always return an error to prevent us from making
// duplicate payments to the same payment hash. The provided preimage is
// atomically saved to the DB for record keeping.
func (p *paymentControl) Success(paymentHash lntypes.Hash,
	preimage lntypes.Preimage) error {

	var updateErr error
	err := p.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := fetchPaymentBucket(tx, paymentHash)
		if err != nil {
			return err
		}

		// Get the existing status, if any.
		paymentStatus := fetchPaymentStatus(bucket)

		// Reset the update error, to avoid carrying over an error
		// from a previous execution of the batched db transaction.
		updateErr = nil

		switch {

		// Our records show the payment as still being grounded,
		// meaning it never should have left the switch.
		case paymentStatus == StatusGrounded:
			updateErr = ErrPaymentNotInitiated
			return nil

		// Though our records show the payment as failed, meaning we
		// didn't expect this result, we permit this transition to not
		// lose potentially crucial data.
		case paymentStatus == StatusFailed:

		// A successful response was received for an InFlight payment,
		// mark it as completed to prevent sending to this payment hash
		// again.
		case paymentStatus == StatusInFlight:

		// The payment was completed previously, alert the caller that
		// this may be a duplicate call.
		case paymentStatus == StatusCompleted:
			updateErr = ErrPaymentAlreadyCompleted
			return nil

		default:
			updateErr = ErrUnknownPaymentStatus
			return nil
		}

		// Record the successful payment info atomically to the
		// payments record.
		err = bucket.Put(paymentSettleInfoKey, preimage[:])
		if err != nil {
			return err
		}

		err = bucket.Put(paymentStatusKey, StatusCompleted.Bytes())
		if err != nil {
			return err
		}

		attempt, err := fetchPaymentAttempt(bucket)
		if err != nil {
			return err
		}

		p.notifySubscribers(
			paymentHash, PaymentEvent{
				Status:   StatusCompleted,
				Preimage: preimage,
				Route:    &attempt.Route,
			},
		)

		return nil
	})
	if err != nil {
		return err
	}

	return updateErr

}

// Fail transitions a payment into the Failed state. After invoking this
// method, InitPayment should return nil on its next call for this payment
// hash, allowing the switch to make a subsequent payment.
func (p *paymentControl) Fail(paymentHash lntypes.Hash) error {
	var updateErr error
	err := p.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := fetchPaymentBucket(tx, paymentHash)
		if err != nil {
			return err
		}

		paymentStatus := fetchPaymentStatus(bucket)

		switch {

		// Our records show the payment as still being grounded,
		// meaning it never should have left the switch.
		case paymentStatus == StatusGrounded:
			updateErr = ErrPaymentNotInitiated
			return nil

		// A failed response was received for an InFlight payment, mark
		// it as Failed to allow subsequent attempts.
		case paymentStatus == StatusInFlight:

		// The payment was completed previously, and we are now
		// reporting that it has failed. Leave the status as completed,
		// but alert the user that something is wrong.
		case paymentStatus == StatusCompleted:
			updateErr = ErrPaymentAlreadyCompleted
			return nil

		// The payment was already failed, and we are now reporting that
		// it has failed again. Leave the status as failed, but alert
		// the user that something is wrong.
		case paymentStatus == StatusFailed:
			updateErr = ErrPaymentAlreadyFailed
			return nil

		default:
			updateErr = ErrUnknownPaymentStatus
			return nil
		}

		// A failed response was received for an InFlight payment, mark
		// it as Failed to allow subsequent attempts.
		err = bucket.Put(paymentStatusKey, StatusFailed.Bytes())
		if err != nil {
			return err
		}

		// TODO: Specify fail reason
		p.notifySubscribers(
			paymentHash, PaymentEvent{
				Status: StatusFailed,
			},
		)

		return nil
	})
	if err != nil {
		return err
	}

	return updateErr
}

// fetchPaymentBucket fetches or creates the sub-bucket assigned to this
// payment hash.
func fetchPaymentBucket(tx *bbolt.Tx, paymentHash lntypes.Hash) (
	*bbolt.Bucket, error) {

	payments, err := tx.CreateBucketIfNotExists(sentPaymentsBucket)
	if err != nil {
		return nil, err
	}

	return payments.CreateBucketIfNotExists(paymentHash[:])
}

// fetchPaymentStatus fetches the payment status from the bucket.  If the
// status isn't found, it will default to "StatusGrounded".
func fetchPaymentStatus(bucket *bbolt.Bucket) PaymentStatus {
	// The default status for all payments that aren't recorded in
	// database.
	var paymentStatus = StatusGrounded

	paymentStatusBytes := bucket.Get(paymentStatusKey)
	if paymentStatusBytes != nil {
		paymentStatus.FromBytes(paymentStatusBytes)
	}

	return paymentStatus
}

// fetchPaymentAttempt fetches the payment attempt from the bucket.
func fetchPaymentAttempt(bucket *bbolt.Bucket) (*AttemptInfo, error) {
	attemptData := bucket.Get(paymentAttemptInfoKey)
	if attemptData == nil {
		return nil, fmt.Errorf("unable to find attempt " +
			"info for inflight payment")
	}

	r := bytes.NewReader(attemptData)
	attempt, err := deserializeAttemptInfo(r)
	if err != nil {
		return nil, err
	}

	return attempt, nil
}

// InFlightPayment is a wrapper around a payment that has status InFlight.
type InFlightPayment struct {
	// PaymentHash is the hash of the in-flight payment.
	Info *CreationInfo

	// Attempt contains information about the last payment attempt that was
	// made to this payment hash.
	//
	// NOTE: Might be nil.
	Attempt *AttemptInfo
}

// SubscribePayment returns a payment status update channel.
func (p *paymentControl) SubscribePayment(paymentHash lntypes.Hash) (
	*PaymentSubscriber, error) {

	var subscriber *PaymentSubscriber

	err := p.db.View(func(tx *bbolt.Tx) error {
		payments := tx.Bucket(sentPaymentsBucket)
		if payments == nil {
			return errors.New("no sent payments bucket")
		}

		bucket := payments.Bucket(paymentHash[:])
		if bucket == nil {
			return errors.New("unknown payment")
		}

		var event PaymentEvent

		status := fetchPaymentStatus(bucket)
		switch status {

		case StatusGrounded:
			return nil

		case StatusCompleted:
			event.Status = StatusCompleted

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
			event.Route = &a.Route

		case StatusFailed:
			// TODO: Specify fail reason
			event.Status = StatusFailed

		case StatusInFlight:
			event.Status = StatusInFlight

		default:
			return errors.New("unknown payment status")
		}

		p.subscribersMtx.Lock()

		id := p.nextSubscriberID
		p.nextSubscriberID++

		// Create a channel with capacity two. There should be no more than two
		// events for each payment, so sending on this channel should never
		// block.
		c := make(chan PaymentEvent, 2)

		subscriber = &PaymentSubscriber{
			PaymentHash: paymentHash,
			Events:      c,
			id:          id,
			p:           p,
		}

		subscribers, ok := p.subscribers[paymentHash]
		if !ok {
			subscribers = make(map[int]*PaymentSubscriber)
			p.subscribers[paymentHash] = subscribers
		}
		subscribers[id] = subscriber
		p.subscribersMtx.Unlock()

		// Send event inside the db transaction, to prevent race
		// conditions.
		c <- event

		return nil
	})
	if err != nil {
		return nil, err
	}

	return subscriber, nil
}

// notifySubscribers sends a payment event to all subscribers of this payment.
func (p *paymentControl) notifySubscribers(paymentHash lntypes.Hash,
	event PaymentEvent) {

	p.subscribersMtx.Lock()
	list, ok := p.subscribers[paymentHash]
	p.subscribersMtx.Unlock()
	if !ok {
		return
	}

	for _, subscriber := range list {
		subscriber.Events <- event
	}
}

// FetchInFlightPayments returns all payments with status InFlight.
func (p *paymentControl) FetchInFlightPayments() ([]*InFlightPayment, error) {
	var inFlights []*InFlightPayment
	err := p.db.View(func(tx *bbolt.Tx) error {
		payments := tx.Bucket(sentPaymentsBucket)
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
			inFlight.Info, err = deserializeCreationInfo(r)
			if err != nil {
				return err
			}

			// Now get the attempt info.
			inFlight.Attempt, err = fetchPaymentAttempt(bucket)
			if err != nil {
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
