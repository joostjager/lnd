package channeldb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/btcsuite/btcd/wire"
	"github.com/coreos/bbolt"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/lightningnetwork/lnd/record"
	"github.com/lightningnetwork/lnd/routing/route"
	"github.com/lightningnetwork/lnd/tlv"
)

var (
	// paymentsRootBucket is the name of the top-level bucket within the
	// database that stores all data related to payments. Within this
	// bucket, each payment hash its own sub-bucket keyed by its payment
	// hash.
	//
	// Bucket hierarchy:
	//
	// root-bucket
	//      |
	//      |-- <paymenthash>
	//      |        |--mpp-sequence-key: <sequence number>
	//      |        |--mpp-creation-info-key: <creation info>
	//      |        |--mpp-fail-info-key: <(optional) fail info>
	//      |        |
	//      |        |--mpp-htlcs-bucket (shard-bucket)
	//      |        |        |
	//      |        |        |-- <htlc attempt ID>
	//      |        |        |       |--htlc-attempt-info-key: <htlc attempt info>
	//      |        |        |       |--htlc-attempt-time-key: <timestamp>
	//      |        |        |       |--htlc-settle-info-key: <(optional) settle info>
	//      |        |        |       |--htlc-fail-info-key: <(optional) fail info>
	//      |        |        |
	//      |        |        |-- <htlc attempt ID>
	//      |        |        |       |
	//      |        |       ...     ...
	//      |        |
	//      |        |
	//      |        |--duplicate-bucket (only for old, completed payments)
	//      |                 |
	//      |                 |-- <seq-num>
	//      |                 |       |--mpp-sequence-key: <sequence number>
	//      |                 |       |--mpp-creation-info-key: <creation info>
	//      |                 |       |--mpp-fail-info-key: <(optional) fail info>
	//      |                 |	  |
	//      |         	  |	  |-- <static-htlc-id>
	//      |                 |		|--htlc-attempt-info-key: <attempt info>
	//      |                 |       	|--htlc-attempt-time-key: <zero timestamp>
	//      |                 |       	|--htlc-settle-info-key: <(optional) settle info>
	//      |                 |       	|--htlc-fail-info-key: <(optional) fail info>
	//      |                 |
	//      |                 |-- <seq-num>
	//      |                 |       |
	//      |                ...     ...
	//      |
	//      |-- <paymenthash>
	//      |        |
	//      |       ...
	//     ...
	//
	paymentsRootBucket = []byte("payments-root-bucket")

	// paymentDuplicateBucket is the name of a optional sub-bucket within
	// the payment hash bucket, that is used to hold duplicate payments to
	// a payment hash. This is needed to support information from earlier
	// versions of lnd, where it was possible to pay to a payment hash more
	// than once.
	paymentDuplicateBucket = []byte("payment-duplicate-bucket")

	// mppSequenceKey is a key used in the payment's sub-bucket to store
	// the sequence number of the payment.
	mppSequenceKey = []byte("mpp-sequence-key")

	// mppCreationInfoKey is a key used in the payment's sub-bucket to
	// store the creation info of the MP payment.
	mppCreationInfoKey = []byte("mpp-creation-info")

	// mppFailInfoKey is a key used in the payment's sub-bucket to store
	// information about a terminal failure of the payment.
	mppFailInfoKey = []byte("mpp-fail-info")

	// mppHtlcsBucket is a bucket where we'll store the information
	// about the HTLCs that were attempted for a payment.
	mppHtlcsBucket = []byte("mpp-htlcs-bucket")

	// htlcAttemptInfoKey is a key used in a HTLC's sub-bucket to store the
	// info about the attempt that was done for the HTLC in question.
	htlcAttemptInfoKey = []byte("htlc-attempt-info")

	// htlcAttemptTimeKey is a key used in a HTLC's sub-bucket to store the
	// time the HTLC was attempted.
	htlcAttemptTimeKey = []byte("htlc-attempt-time")

	// htlcSettleInfoKey is a key used in a HTLC's sub-bucket to store the
	// settle info, if any.
	htlcSettleInfoKey = []byte("htlc-settle-info")

	// htlcFailInfoKey is a key used in a HTLC's sub-bucket to store
	// failure information, if any.
	htlcFailInfoKey = []byte("htlc-fail-info")
)

// FailureReason encodes the reason a payment ultimately failed.
type FailureReason byte

const (
	// FailureReasonTimeout indicates that the payment did timeout before a
	// successful payment attempt was made.
	FailureReasonTimeout FailureReason = 0

	// FailureReasonNoRoute indicates no successful route to the
	// destination was found during path finding.
	FailureReasonNoRoute FailureReason = 1

	// FailureReasonError indicates that an unexpected error happened during
	// payment.
	FailureReasonError FailureReason = 2

	// FailureReasonPaymentDetails indicates that either the hash is unknown
	// or the final cltv delta or amount is incorrect.
	FailureReasonPaymentDetails FailureReason = 3

	// FailureReasonInsufficientBalance indicates that we didn't have enough
	// balance to complete the payment.
	FailureReasonInsufficientBalance FailureReason = 4

	// TODO(halseth): cancel state.

	// TODO(joostjager): Add failure reasons for:
	// LocalLiquidityInsufficient, RemoteCapacityInsufficient.
)

// String returns a human readable FailureReason
func (r FailureReason) String() string {
	switch r {
	case FailureReasonTimeout:
		return "timeout"
	case FailureReasonNoRoute:
		return "no_route"
	case FailureReasonError:
		return "error"
	case FailureReasonPaymentDetails:
		return "incorrect_payment_details"
	case FailureReasonInsufficientBalance:
		return "insufficient_balance"
	}

	return "unknown"
}

// PaymentStatus represent current status of payment
type PaymentStatus byte

const (
	// StatusUnknown is the status where a payment has never been initiated
	// and hence is unknown.
	StatusUnknown PaymentStatus = 0

	// StatusInFlight is the status where a payment has been initiated, but
	// a response has not been received.
	StatusInFlight PaymentStatus = 1

	// StatusSucceeded is the status where a payment has been initiated and
	// the payment was completed successfully.
	StatusSucceeded PaymentStatus = 2

	// StatusFailed is the status where a payment has been initiated and a
	// failure result has come back.
	StatusFailed PaymentStatus = 3
)

// Bytes returns status as slice of bytes.
func (ps PaymentStatus) Bytes() []byte {
	return []byte{byte(ps)}
}

// FromBytes sets status from slice of bytes.
func (ps *PaymentStatus) FromBytes(status []byte) error {
	if len(status) != 1 {
		return errors.New("payment status is empty")
	}

	switch PaymentStatus(status[0]) {
	case StatusUnknown, StatusInFlight, StatusSucceeded, StatusFailed:
		*ps = PaymentStatus(status[0])
	default:
		return errors.New("unknown payment status")
	}

	return nil
}

// String returns readable representation of payment status.
func (ps PaymentStatus) String() string {
	switch ps {
	case StatusUnknown:
		return "Unknown"
	case StatusInFlight:
		return "In Flight"
	case StatusSucceeded:
		return "Succeeded"
	case StatusFailed:
		return "Failed"
	default:
		return "Unknown"
	}
}

// FetchPayments returns all sent payments found in the DB.
//
// nolint: dupl
func (db *DB) FetchPayments() ([]*MPPayment, error) {
	var payments []*MPPayment

	err := db.View(func(tx *bbolt.Tx) error {
		paymentsBucket := tx.Bucket(paymentsRootBucket)
		if paymentsBucket == nil {
			return nil
		}

		return paymentsBucket.ForEach(func(k, v []byte) error {
			bucket := paymentsBucket.Bucket(k)
			if bucket == nil {
				// We only expect sub-buckets to be found in
				// this top-level bucket.
				return fmt.Errorf("non bucket element in " +
					"payments bucket")
			}

			p, err := fetchPayment(bucket)
			if err != nil {
				return err
			}

			payments = append(payments, p)

			// For older versions of lnd, duplicate payments to a
			// payment has was possible. These will be found in a
			// sub-bucket indexed by their sequence number if
			// available.
			dup := bucket.Bucket(paymentDuplicateBucket)
			if dup == nil {
				return nil
			}

			return dup.ForEach(func(k, v []byte) error {
				subBucket := dup.Bucket(k)
				if subBucket == nil {
					// We one bucket for each duplicate to
					// be found.
					return fmt.Errorf("non bucket element" +
						"in duplicate bucket")
				}

				p, err := fetchPayment(subBucket)
				if err != nil {
					return err
				}

				payments = append(payments, p)
				return nil
			})
		})
	})
	if err != nil {
		return nil, err
	}

	// Before returning, sort the payments by their sequence number.
	sort.Slice(payments, func(i, j int) bool {
		return payments[i].sequenceNum < payments[j].sequenceNum
	})

	return payments, nil
}

func fetchPayment(bucket *bbolt.Bucket) (*MPPayment, error) {
	seqBytes := bucket.Get(mppSequenceKey)
	if seqBytes == nil {
		return nil, fmt.Errorf("sequence number not found")
	}

	sequenceNum := binary.BigEndian.Uint64(seqBytes)

	// Get the payment status.
	paymentStatus := fetchPaymentStatus(bucket)

	// Get the PaymentCreationInfo.
	b := bucket.Get(mppCreationInfoKey)
	if b == nil {
		return nil, fmt.Errorf("creation info not found")
	}

	r := bytes.NewReader(b)
	creationInfo, err := deserializeMPPaymentCreationInfo(r)
	if err != nil {
		return nil, err

	}

	var htlcs []*HTLCAttempt
	htlcsBucket := bucket.Bucket(mppHtlcsBucket)
	if htlcsBucket != nil {
		// Get the payment attempts. This can be empty.
		htlcs, err = fetchHTLCAttempts(htlcsBucket)
		if err != nil {
			return nil, err
		}
	}

	// Get failure reason if available.
	var failureReason *FailureReason
	b = bucket.Get(mppFailInfoKey)
	if b != nil {
		reason := FailureReason(b[0])
		failureReason = &reason
	}

	return &MPPayment{
		sequenceNum:   sequenceNum,
		Info:          creationInfo,
		HTLCs:         htlcs,
		FailureReason: failureReason,
		Status:        paymentStatus,
	}, nil
}

// fetchHTLCAttempts retrives all HTLC attempts made for the payment found in
// the given bucket.
func fetchHTLCAttempts(bucket *bbolt.Bucket) ([]*HTLCAttempt, error) {
	var htlcs []*HTLCAttempt

	err := bucket.ForEach(func(k, _ []byte) error {
		htlcBucket := bucket.Bucket(k)
		htlc := &HTLCAttempt{}
		var err error

		htlc.HTLCAttemptInfo, err = fetchHTLCHTLCAttemptInfo(
			htlcBucket,
		)
		if err != nil {
			return err
		}

		htlc.AttemptTime, err = fetchHTLCAttemptTime(htlcBucket)
		if err != nil {
			return err
		}

		// Settle info might be nil.
		htlc.Settle, err = fetchHTLCSettleInfo(htlcBucket)
		if err != nil {
			return err
		}

		// Failure info might be nil.
		htlc.Failure, err = fetchHTLCFailInfo(htlcBucket)
		if err != nil {
			return err
		}

		htlcs = append(htlcs, htlc)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return htlcs, nil
}

// fetchHTLCHTLCAttemptInfo fetches the payment attempt for this HTLC from
// the bucket.
func fetchHTLCHTLCAttemptInfo(bucket *bbolt.Bucket) (
	*HTLCAttemptInfo, error) {

	b := bucket.Get(htlcAttemptInfoKey)
	if b == nil {
		return nil, errNoAttemptInfo
	}

	r := bytes.NewReader(b)
	return deserializeHTLCAttemptInfo(r)
}

// fetchHTLCAttemptTime fetches the recorded time when the HTLC was first
// attempted.
func fetchHTLCAttemptTime(bucket *bbolt.Bucket) (time.Time, error) {
	b := bucket.Get(htlcAttemptTimeKey)
	if b == nil {
		return time.Time{}, fmt.Errorf("no attempt time found")
	}

	r := bytes.NewReader(b)
	return deserializeTime(r)
}

// fetchHTLCSettleInfo retrieves the settle info for the HTLC, if any.
//
// NOTE: Can be nil.
func fetchHTLCSettleInfo(bucket *bbolt.Bucket) (*HTLCSettleInfo, error) {
	b := bucket.Get(htlcSettleInfoKey)
	if b == nil {
		// Settle info is optional.
		return nil, nil
	}

	r := bytes.NewReader(b)
	return deserializeHTLCSettleInfo(r)
}

// fetchHTLCFailInfo retrieves the failure info for the HTLC, if any.
//
// NOTE: Can be nil
func fetchHTLCFailInfo(bucket *bbolt.Bucket) (*HTLCFailInfo, error) {
	b := bucket.Get(htlcFailInfoKey)
	if b == nil {
		// Fail info is optional.
		return nil, nil
	}

	r := bytes.NewReader(b)
	return deserializeHTLCFailInfo(r)
}

// DeletePayments deletes all completed and failed payments from the DB.
func (db *DB) DeletePayments() error {
	return db.Update(func(tx *bbolt.Tx) error {
		payments := tx.Bucket(paymentsRootBucket)
		if payments == nil {
			return nil
		}

		var deleteBuckets [][]byte
		err := payments.ForEach(func(k, _ []byte) error {
			bucket := payments.Bucket(k)
			if bucket == nil {
				// We only expect sub-buckets to be found in
				// this top-level bucket.
				return fmt.Errorf("non bucket element in " +
					"payments bucket")
			}

			// If the status is InFlight, we cannot safely delete
			// the payment information, so we return early.
			paymentStatus := fetchPaymentStatus(bucket)
			if paymentStatus == StatusInFlight {
				return nil
			}

			deleteBuckets = append(deleteBuckets, k)
			return nil
		})
		if err != nil {
			return err
		}

		for _, k := range deleteBuckets {
			if err := payments.DeleteBucket(k); err != nil {
				return err
			}
		}

		return nil
	})
}

// nolint: dupl
func serializeMPPaymentCreationInfo(w io.Writer, c *MPPaymentCreationInfo) error {
	var scratch [8]byte

	if _, err := w.Write(c.PaymentHash[:]); err != nil {
		return err
	}

	byteOrder.PutUint64(scratch[:], uint64(c.Value))
	if _, err := w.Write(scratch[:]); err != nil {
		return err
	}

	byteOrder.PutUint64(scratch[:], uint64(c.CreationTime.Unix()))
	if _, err := w.Write(scratch[:]); err != nil {
		return err
	}

	byteOrder.PutUint32(scratch[:4], uint32(len(c.PaymentRequest)))
	if _, err := w.Write(scratch[:4]); err != nil {
		return err
	}

	if _, err := w.Write(c.PaymentRequest[:]); err != nil {
		return err
	}

	return nil
}

func deserializeMPPaymentCreationInfo(r io.Reader) (*MPPaymentCreationInfo, error) {
	var scratch [8]byte

	c := &MPPaymentCreationInfo{}

	if _, err := io.ReadFull(r, c.PaymentHash[:]); err != nil {
		return nil, err
	}

	if _, err := io.ReadFull(r, scratch[:]); err != nil {
		return nil, err
	}
	c.Value = lnwire.MilliSatoshi(byteOrder.Uint64(scratch[:]))

	if _, err := io.ReadFull(r, scratch[:]); err != nil {
		return nil, err
	}
	c.CreationTime = time.Unix(int64(byteOrder.Uint64(scratch[:])), 0)

	if _, err := io.ReadFull(r, scratch[:4]); err != nil {
		return nil, err
	}

	reqLen := uint32(byteOrder.Uint32(scratch[:4]))
	payReq := make([]byte, reqLen)
	if reqLen > 0 {
		if _, err := io.ReadFull(r, payReq); err != nil {
			return nil, err
		}
	}
	c.PaymentRequest = payReq

	return c, nil
}

func serializeHTLCAttemptInfo(w io.Writer, a *HTLCAttemptInfo) error {
	if err := WriteElements(w, a.AttemptID, a.SessionKey); err != nil {
		return err
	}

	if err := SerializeRoute(w, a.Route); err != nil {
		return err
	}

	return nil
}

func deserializeHTLCAttemptInfo(r io.Reader) (*HTLCAttemptInfo, error) {
	a := &HTLCAttemptInfo{}
	err := ReadElements(r, &a.AttemptID, &a.SessionKey)
	if err != nil {
		return nil, err
	}
	a.Route, err = DeserializeRoute(r)
	if err != nil {
		return nil, err
	}
	return a, nil
}

func serializeHop(w io.Writer, h *route.Hop) error {
	if err := WriteElements(w,
		h.PubKeyBytes[:],
		h.ChannelID,
		h.OutgoingTimeLock,
		h.AmtToForward,
	); err != nil {
		return err
	}

	if err := binary.Write(w, byteOrder, h.LegacyPayload); err != nil {
		return err
	}

	// For legacy payloads, we don't need to write any TLV records, so
	// we'll write a zero indicating the our serialized TLV map has no
	// records.
	if h.LegacyPayload {
		return WriteElements(w, uint32(0))
	}

	// Gather all non-primitive TLV records so that they can be serialized
	// as a single blob.
	//
	// TODO(conner): add migration to unify all fields in a single TLV
	// blobs. The split approach will cause headaches down the road as more
	// fields are added, which we can avoid by having a single TLV stream
	// for all payload fields.
	var records []tlv.Record
	if h.MPP != nil {
		records = append(records, h.MPP.Record())
	}

	// Final sanity check to absolutely rule out custom records that are not
	// custom and write into the standard range.
	if err := h.CustomRecords.Validate(); err != nil {
		return err
	}

	// Convert custom records to tlv and add to the record list.
	// MapToRecords sorts the list, so adding it here will keep the list
	// canonical.
	tlvRecords := tlv.MapToRecords(h.CustomRecords)
	records = append(records, tlvRecords...)

	// Otherwise, we'll transform our slice of records into a map of the
	// raw bytes, then serialize them in-line with a length (number of
	// elements) prefix.
	mapRecords, err := tlv.RecordsToMap(records)
	if err != nil {
		return err
	}

	numRecords := uint32(len(mapRecords))
	if err := WriteElements(w, numRecords); err != nil {
		return err
	}

	for recordType, rawBytes := range mapRecords {
		if err := WriteElements(w, recordType); err != nil {
			return err
		}

		if err := wire.WriteVarBytes(w, 0, rawBytes); err != nil {
			return err
		}
	}

	return nil
}

// maxOnionPayloadSize is the largest Sphinx payload possible, so we don't need
// to read/write a TLV stream larger than this.
const maxOnionPayloadSize = 1300

func deserializeHop(r io.Reader) (*route.Hop, error) {
	h := &route.Hop{}

	var pub []byte
	if err := ReadElements(r, &pub); err != nil {
		return nil, err
	}
	copy(h.PubKeyBytes[:], pub)

	if err := ReadElements(r,
		&h.ChannelID, &h.OutgoingTimeLock, &h.AmtToForward,
	); err != nil {
		return nil, err
	}

	// TODO(roasbeef): change field to allow LegacyPayload false to be the
	// legacy default?
	err := binary.Read(r, byteOrder, &h.LegacyPayload)
	if err != nil {
		return nil, err
	}

	var numElements uint32
	if err := ReadElements(r, &numElements); err != nil {
		return nil, err
	}

	// If there're no elements, then we can return early.
	if numElements == 0 {
		return h, nil
	}

	tlvMap := make(map[uint64][]byte)
	for i := uint32(0); i < numElements; i++ {
		var tlvType uint64
		if err := ReadElements(r, &tlvType); err != nil {
			return nil, err
		}

		rawRecordBytes, err := wire.ReadVarBytes(
			r, 0, maxOnionPayloadSize, "tlv",
		)
		if err != nil {
			return nil, err
		}

		tlvMap[tlvType] = rawRecordBytes
	}

	// If the MPP type is present, remove it from the generic TLV map and
	// parse it back into a proper MPP struct.
	//
	// TODO(conner): add migration to unify all fields in a single TLV
	// blobs. The split approach will cause headaches down the road as more
	// fields are added, which we can avoid by having a single TLV stream
	// for all payload fields.
	mppType := uint64(record.MPPOnionType)
	if mppBytes, ok := tlvMap[mppType]; ok {
		delete(tlvMap, mppType)

		var (
			mpp    = &record.MPP{}
			mppRec = mpp.Record()
			r      = bytes.NewReader(mppBytes)
		)
		err := mppRec.Decode(r, uint64(len(mppBytes)))
		if err != nil {
			return nil, err
		}
		h.MPP = mpp
	}

	h.CustomRecords = tlvMap

	return h, nil
}

// SerializeRoute serializes a route.
func SerializeRoute(w io.Writer, r route.Route) error {
	if err := WriteElements(w,
		r.TotalTimeLock, r.TotalAmount, r.SourcePubKey[:],
	); err != nil {
		return err
	}

	if err := WriteElements(w, uint32(len(r.Hops))); err != nil {
		return err
	}

	for _, h := range r.Hops {
		if err := serializeHop(w, h); err != nil {
			return err
		}
	}

	return nil
}

// DeserializeRoute deserializes a route.
func DeserializeRoute(r io.Reader) (route.Route, error) {
	rt := route.Route{}
	if err := ReadElements(r,
		&rt.TotalTimeLock, &rt.TotalAmount,
	); err != nil {
		return rt, err
	}

	var pub []byte
	if err := ReadElements(r, &pub); err != nil {
		return rt, err
	}
	copy(rt.SourcePubKey[:], pub)

	var numHops uint32
	if err := ReadElements(r, &numHops); err != nil {
		return rt, err
	}

	var hops []*route.Hop
	for i := uint32(0); i < numHops; i++ {
		hop, err := deserializeHop(r)
		if err != nil {
			return rt, err
		}
		hops = append(hops, hop)
	}
	rt.Hops = hops

	return rt, nil
}
