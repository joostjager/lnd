package channeldb

import (
	"io"
	"time"

	"github.com/lightningnetwork/lnd/lnwire"

	"github.com/btcsuite/btcd/btcec"
	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/routing/route"
)

// HTLCAttemptInfo contains static information about a specific HTLC attempt
// for a payment. This information is used by the router to handle any errors
// coming back after an attempt is made, and to query the switch about the
// status of the attempt.
type HTLCAttemptInfo struct {
	// AttemptID is the unique ID used for this attempt.
	AttemptID uint64

	// SessionKey is the ephemeral key used for this attempt.
	SessionKey *btcec.PrivateKey

	// Route is the route attempted to send the HTLC.
	Route route.Route

	// AttemptTime is the time at which this HTLC was attempted.
	AttemptTime time.Time
}

// HTLCAttempt contains information about a specific HTLC attempt for a given
// payment. It contains the HTLCAttemptInfo used to send the HTLC, as well
// as a timestamp and any known outcome of the attempt.
type HTLCAttempt struct {
	*HTLCAttemptInfo

	// Settle is the preimage of a successful payment. This serves as a
	// proof of payment. It will only be non-nil for settled payments.
	//
	// NOTE: Can be nil if payment is not settled.
	Settle *HTLCSettleInfo

	// Fail is a failure reason code indicating the reason the payment
	// failed. It is only non-nil for failed payments.
	//
	// NOTE: Can be nil if payment is not failed.
	Failure *HTLCFailInfo
}

// HTLCSettleInfo encapsulates the information that augments an HTLCAttempt in
// the event that the HTLC is successful.
type HTLCSettleInfo struct {
	// Preimage is the preimage of a successful HTLC. This serves as a proof
	// of payment.
	Preimage lntypes.Preimage

	// SettleTime is the time at which this HTLC was settled.
	SettleTime time.Time
}

// HTLCFailReason is the reason an htlc failed.
type HTLCFailReason byte

const (
	// HTLCFailUnknown is recorded for htlcs that failed with an unknown
	// reason.
	HTLCFailUnknown HTLCFailReason = 0

	// HTLCFailUnknown is recorded for htlcs that had a failure message that
	// couldn't be decrypted.
	HTLCFailUnreadble HTLCFailReason = 1

	// HTLCFailInternal is recorded for htlcs that failed because of an
	// internal error.
	HTLCFailInternal HTLCFailReason = 2

	// HTLCFailMessage is recorded for htlcs that failed with a network
	// failure message.
	HTLCFailMessage HTLCFailReason = 3
)

// HTLCFailInfo encapsulates the information that augments an HTLCAttempt in the
// event that the HTLC fails.
type HTLCFailInfo struct {
	// FailTime is the time at which this HTLC was failed.
	FailTime time.Time

	// Reason is the failure reason for this HTLC.
	Reason HTLCFailReason

	// Message is the wire message that failed this HTLC.
	Message lnwire.FailureMessage

	// The position in the path of the intermediate or final node that
	// generated the failure message. Position zero is the sender node.
	FailureSourceIndex uint32
}

// MPPayment is a wrapper around a payment's PaymentCreationInfo and
// HTLCAttempts. All payments will have the PaymentCreationInfo set, any
// HTLCs made in attempts to be completed will populated in the HTLCs slice.
// Each populated HTLCAttempt represents an attempted HTLC, each of which may
// have the associated Settle or Fail struct populated if the HTLC is no longer
// in-flight.
type MPPayment struct {
	// sequenceNum is a unique identifier used to sort the payments in
	// order of creation.
	sequenceNum uint64

	// Info holds all static information about this payment, and is
	// populated when the payment is initiated.
	Info *PaymentCreationInfo

	// HTLCs holds the information about individual HTLCs that we send in
	// order to make the payment.
	HTLCs []*HTLCAttempt

	// FailureReason is the failure reason code indicating the reason the
	// payment failed.
	//
	// NOTE: Will only be set once the daemon has given up on the payment
	// altogether.
	FailureReason *FailureReason

	// Status is the current PaymentStatus of this payment.
	Status PaymentStatus
}

func serializeHTLCSettleInfo(w io.Writer, s *HTLCSettleInfo) error {
	if _, err := w.Write(s.Preimage[:]); err != nil {
		return err
	}

	if err := serializeTime(w, s.SettleTime); err != nil {
		return err
	}

	return nil
}

func deserializeHTLCSettleInfo(r io.Reader) (*HTLCSettleInfo, error) {
	s := &HTLCSettleInfo{}
	if _, err := io.ReadFull(r, s.Preimage[:]); err != nil {
		return nil, err
	}

	var err error
	s.SettleTime, err = deserializeTime(r)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func serializeHTLCFailInfo(w io.Writer, f *HTLCFailInfo) error {
	if err := serializeTime(w, f.FailTime); err != nil {
		return err
	}

	if err := lnwire.EncodeFailureMessage(w, f.Message, 0); err != nil {
		return err
	}

	return WriteElements(w, byte(f.Reason), f.FailureSourceIndex)
}

func deserializeHTLCFailInfo(r io.Reader) (*HTLCFailInfo, error) {
	f := &HTLCFailInfo{}
	var err error
	f.FailTime, err = deserializeTime(r)
	if err != nil {
		return nil, err
	}

	f.Message, err = lnwire.DecodeFailureMessage(r, 0)
	if err != nil {
		return nil, err
	}

	var reason byte
	err = ReadElements(r, &reason, &f.FailureSourceIndex)
	if err != nil {
		return nil, err
	}
	f.Reason = HTLCFailReason(reason)

	return f, nil
}

func deserializeTime(r io.Reader) (time.Time, error) {
	var scratch [8]byte
	if _, err := io.ReadFull(r, scratch[:]); err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, int64(byteOrder.Uint64(scratch[:]))), nil
}

func serializeTime(w io.Writer, t time.Time) error {
	var scratch [8]byte
	byteOrder.PutUint64(scratch[:], uint64(t.UnixNano()))
	if _, err := w.Write(scratch[:]); err != nil {
		return err
	}

	return nil
}
