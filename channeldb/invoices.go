package channeldb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/lightningnetwork/lnd/channeldb/kvdb"
	"github.com/lightningnetwork/lnd/htlcswitch/hop"
	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/lightningnetwork/lnd/record"
	"github.com/lightningnetwork/lnd/tlv"
)

var (
	// unknownPreimage is an all-zeroes preimage that indicates that the
	// preimage for this invoice is not yet known.
	unknownPreimage lntypes.Preimage

	// invoiceBucket is the name of the bucket within the database that
	// stores all data related to invoices no matter their final state.
	// Within the invoice bucket, each invoice is keyed by its invoice ID
	// which is a monotonically increasing uint32.
	invoiceBucket = []byte("invoices")

	// paymentHashIndexBucket is the name of the sub-bucket within the
	// invoiceBucket which indexes all invoices by their payment hash. The
	// payment hash is the sha256 of the invoice's payment preimage. This
	// index is used to detect duplicates, and also to provide a fast path
	// for looking up incoming HTLCs to determine if we're able to settle
	// them fully.
	//
	// maps: payHash => invoiceKey
	invoiceIndexBucket = []byte("paymenthashes")

	// payAddrIndexBucket is the name of the top-level bucket that maps
	// payment addresses to their invoice number. This can be used
	// to efficiently query or update non-legacy invoices. Note that legacy
	// invoices will not be included in this index since they all have the
	// same, all-zero payment address, however all newly generated invoices
	// will end up in this index.
	//
	// maps: payAddr => invoiceKey
	payAddrIndexBucket = []byte("pay-addr-index")

	// setIDIndexBucket is the name of the top-level bucket that maps set
	// ids to their invoice number. This can be used to efficiently query or
	// update AMP invoice. Note that legacy or MPP invoices will not be
	// included in this index, since their HTLCs do not have a set id.
	//
	// maps: setID => invoiceKey
	setIDIndexBucket = []byte("set-id-index")

	// numInvoicesKey is the name of key which houses the auto-incrementing
	// invoice ID which is essentially used as a primary key. With each
	// invoice inserted, the primary key is incremented by one. This key is
	// stored within the invoiceIndexBucket. Within the invoiceBucket
	// invoices are uniquely identified by the invoice ID.
	numInvoicesKey = []byte("nik")

	// addIndexBucket is an index bucket that we'll use to create a
	// monotonically increasing set of add indexes. Each time we add a new
	// invoice, this sequence number will be incremented and then populated
	// within the new invoice.
	//
	// In addition to this sequence number, we map:
	//
	//   addIndexNo => invoiceKey
	addIndexBucket = []byte("invoice-add-index")

	// settleIndexBucket is an index bucket that we'll use to create a
	// monotonically increasing integer for tracking a "settle index". Each
	// time an invoice is settled, this sequence number will be incremented
	// as populate within the newly settled invoice.
	//
	// In addition to this sequence number, we map:
	//
	//   settleIndexNo => invoiceKey
	settleIndexBucket = []byte("invoice-settle-index")

	// ErrInvoiceAlreadySettled is returned when the invoice is already
	// settled.
	ErrInvoiceAlreadySettled = errors.New("invoice already settled")

	// ErrInvoiceAlreadyCanceled is returned when the invoice is already
	// canceled.
	ErrInvoiceAlreadyCanceled = errors.New("invoice already canceled")

	// ErrInvoiceAlreadyAccepted is returned when the invoice is already
	// accepted.
	ErrInvoiceAlreadyAccepted = errors.New("invoice already accepted")

	// ErrInvoiceStillOpen is returned when the invoice is still open.
	ErrInvoiceStillOpen = errors.New("invoice still open")

	// ErrInvoiceCannotOpen is returned when an attempt is made to move an
	// invoice to the open state.
	ErrInvoiceCannotOpen = errors.New("cannot move invoice to open")

	// ErrInvoiceCannotAccept is returned when an attempt is made to accept
	// an invoice while the invoice is not in the open state.
	ErrInvoiceCannotAccept = errors.New("cannot accept invoice")

	// ErrInvoicePreimageMismatch is returned when the preimage doesn't
	// match the invoice hash.
	ErrInvoicePreimageMismatch = errors.New("preimage does not match")

	// ErrInvoiceHasHtlcs is returned when attempting to insert an invoice
	// that already has HTLCs.
	ErrInvoiceHasHtlcs = errors.New("cannot add invoice with htlcs")
)

// ErrDuplicateSetID is an error returned when attempting to adding an AMP HTLC
// to an invoice, but another invoice is already indexed by the same set id.
type ErrDuplicateSetID [32]byte

// Error returns a human-readable description of ErrDuplicateSetID.
func (e ErrDuplicateSetID) Error() string {
	return fmt.Sprintf("invoice with set_id=%x already exists", e)
}

const (
	// MaxMemoSize is maximum size of the memo field within invoices stored
	// in the database.
	MaxMemoSize = 1024

	// MaxPaymentRequestSize is the max size of a payment request for
	// this invoice.
	// TODO(halseth): determine the max length payment request when field
	// lengths are final.
	MaxPaymentRequestSize = 4096

	// A set of tlv type definitions used to serialize invoice htlcs to the
	// database.
	//
	// NOTE: A migration should be added whenever this list changes. This
	// prevents against the database being rolled back to an older
	// format where the surrounding logic might assume a different set of
	// fields are known.
	chanIDType       tlv.Type = 1
	htlcIDType       tlv.Type = 3
	amtType          tlv.Type = 5
	acceptHeightType tlv.Type = 7
	acceptTimeType   tlv.Type = 9
	resolveTimeType  tlv.Type = 11
	expiryHeightType tlv.Type = 13
	htlcStateType    tlv.Type = 15
	mppTotalAmtType  tlv.Type = 17
	htlcAMPType      tlv.Type = 19
	htlcHashType     tlv.Type = 21
	htlcPreimageType tlv.Type = 23

	// A set of tlv type definitions used to serialize invoice bodiees.
	//
	// NOTE: A migration should be added whenever this list changes. This
	// prevents against the database being rolled back to an older
	// format where the surrounding logic might assume a different set of
	// fields are known.
	memoType        tlv.Type = 0
	payReqType      tlv.Type = 1
	createTimeType  tlv.Type = 2
	settleTimeType  tlv.Type = 3
	addIndexType    tlv.Type = 4
	settleIndexType tlv.Type = 5
	preimageType    tlv.Type = 6
	valueType       tlv.Type = 7
	cltvDeltaType   tlv.Type = 8
	expiryType      tlv.Type = 9
	paymentAddrType tlv.Type = 10
	featuresType    tlv.Type = 11
	invStateType    tlv.Type = 12
	amtPaidType     tlv.Type = 13
	hodlInvoiceType tlv.Type = 14
)

// InvoiceRef is a composite identifier for invoices. Invoices can be referenced
// by various combinations of payment hash and payment addr, in certain contexts
// only some of these are known. An InvoiceRef and its constructors thus
// encapsulate the valid combinations of query parameters that can be supplied
// to LookupInvoice and UpdateInvoice.
type InvoiceRef struct {
	// payHash is the payment hash of the target invoice. All invoices are
	// currently indexed by payment hash. This value will be used as a
	// fallback when no payment address is known.
	payHash lntypes.Hash

	// payAddr is the payment addr of the target invoice. Newer invoices
	// (0.11 and up) are indexed by payment address in addition to payment
	// hash, but pre 0.8 invoices do not have one at all. When this value is
	// known it will be used as the primary identifier, falling back to
	// payHash if no value is known.
	payAddr *[32]byte

	// setID is the optional set id for an AMP payment. This can be used to
	// lookup or update the invoice knowing only this value. Queries by set
	// id are only used to facilitate user-facing requests, e.g. lookup,
	// settle or cancel an AMP invoice. The regular update flow from the
	// invoice registry will always query for the invoice by
	// payHash+payAddr.
	setID *[32]byte
}

// InvoiceRefByHash creates an InvoiceRef that queries for an invoice only by
// its payment hash.
func InvoiceRefByHash(payHash lntypes.Hash) InvoiceRef {
	return InvoiceRef{
		payHash: payHash,
	}
}

// InvoiceRefByHashAndAddr creates an InvoiceRef that first queries for an
// invoice by the provided payment address, falling back to the payment hash if
// the payment address is unknown.
func InvoiceRefByHashAndAddr(payHash lntypes.Hash,
	payAddr [32]byte) InvoiceRef {

	return InvoiceRef{
		payHash: payHash,
		payAddr: &payAddr,
	}
}

// InvoiceRefBySetID creates an InvoiceRef that queries the set id index for an
// invoice with the provided setID. If the invoice is not found, the query will
// not fallback to payHash or payAddr.
func InvoiceRefBySetID(setID [32]byte) InvoiceRef {
	return InvoiceRef{
		setID: &setID,
	}
}

// PayHash returns the target invoice's payment hash.
func (r InvoiceRef) PayHash() lntypes.Hash {
	return r.payHash
}

// PayAddr returns the optional payment address of the target invoice.
//
// NOTE: This value may be nil.
func (r InvoiceRef) PayAddr() *[32]byte {
	if r.payAddr != nil {
		addr := *r.payAddr
		return &addr
	}
	return nil
}

// SetID returns the optional set id of the target invoice.
//
// NOTE: This value may be nil.
func (r InvoiceRef) SetID() *[32]byte {
	if r.setID != nil {
		id := *r.setID
		return &id
	}
	return nil
}

// String returns a human-readable representation of an InvoiceRef.
func (r InvoiceRef) String() string {
	if r.payAddr != nil {
		return fmt.Sprintf("(pay_hash=%v, pay_addr=%x)", r.payHash, *r.payAddr)
	}
	return fmt.Sprintf("(pay_hash=%v)", r.payHash)
}

// ContractState describes the state the invoice is in.
type ContractState uint8

const (
	// ContractOpen means the invoice has only been created.
	ContractOpen ContractState = 0

	// ContractSettled means the htlc is settled and the invoice has been paid.
	ContractSettled ContractState = 1

	// ContractCanceled means the invoice has been canceled.
	ContractCanceled ContractState = 2

	// ContractAccepted means the HTLC has been accepted but not settled yet.
	ContractAccepted ContractState = 3
)

// String returns a human readable identifier for the ContractState type.
func (c ContractState) String() string {
	switch c {
	case ContractOpen:
		return "Open"
	case ContractSettled:
		return "Settled"
	case ContractCanceled:
		return "Canceled"
	case ContractAccepted:
		return "Accepted"
	}

	return "Unknown"
}

// ContractTerm is a companion struct to the Invoice struct. This struct houses
// the necessary conditions required before the invoice can be considered fully
// settled by the payee.
type ContractTerm struct {
	// FinalCltvDelta is the minimum required number of blocks before htlc
	// expiry when the invoice is accepted.
	FinalCltvDelta int32

	// Expiry defines how long after creation this invoice should expire.
	Expiry time.Duration

	// PaymentPreimage is the preimage which is to be revealed in the
	// occasion that an HTLC paying to the hash of this preimage is
	// extended. Set to nil if the preimage isn't known yet.
	PaymentPreimage *lntypes.Preimage

	// Value is the expected amount of milli-satoshis to be paid to an HTLC
	// which can be satisfied by the above preimage.
	Value lnwire.MilliSatoshi

	// PaymentAddr is a randomly generated value include in the MPP record
	// by the sender to prevent probing of the receiver.
	PaymentAddr [32]byte

	// Features is the feature vectors advertised on the payment request.
	Features *lnwire.FeatureVector
}

// String returns a human-readable description of the prominent contract terms.
func (c ContractTerm) String() string {
	return fmt.Sprintf("amt=%v, expiry=%v, final_cltv_delta=%v", c.Value,
		c.Expiry, c.FinalCltvDelta)
}

// Invoice is a payment invoice generated by a payee in order to request
// payment for some good or service. The inclusion of invoices within Lightning
// creates a payment work flow for merchants very similar to that of the
// existing financial system within PayPal, etc.  Invoices are added to the
// database when a payment is requested, then can be settled manually once the
// payment is received at the upper layer. For record keeping purposes,
// invoices are never deleted from the database, instead a bit is toggled
// denoting the invoice has been fully settled. Within the database, all
// invoices must have a unique payment hash which is generated by taking the
// sha256 of the payment preimage.
type Invoice struct {
	// Memo is an optional memo to be stored along side an invoice.  The
	// memo may contain further details pertaining to the invoice itself,
	// or any other message which fits within the size constraints.
	Memo []byte

	// PaymentRequest is the encoded payment request for this invoice. For
	// spontaneous (keysend) payments, this field will be empty.
	PaymentRequest []byte

	// CreationDate is the exact time the invoice was created.
	CreationDate time.Time

	// SettleDate is the exact time the invoice was settled.
	SettleDate time.Time

	// Terms are the contractual payment terms of the invoice. Once all the
	// terms have been satisfied by the payer, then the invoice can be
	// considered fully fulfilled.
	//
	// TODO(roasbeef): later allow for multiple terms to fulfill the final
	// invoice: payment fragmentation, etc.
	Terms ContractTerm

	// AddIndex is an auto-incrementing integer that acts as a
	// monotonically increasing sequence number for all invoices created.
	// Clients can then use this field as a "checkpoint" of sorts when
	// implementing a streaming RPC to notify consumers of instances where
	// an invoice has been added before they re-connected.
	//
	// NOTE: This index starts at 1.
	AddIndex uint64

	// SettleIndex is an auto-incrementing integer that acts as a
	// monotonically increasing sequence number for all settled invoices.
	// Clients can then use this field as a "checkpoint" of sorts when
	// implementing a streaming RPC to notify consumers of instances where
	// an invoice has been settled before they re-connected.
	//
	// NOTE: This index starts at 1.
	SettleIndex uint64

	// State describes the state the invoice is in.
	State ContractState

	// AmtPaid is the final amount that we ultimately accepted for pay for
	// this invoice. We specify this value independently as it's possible
	// that the invoice originally didn't specify an amount, or the sender
	// overpaid.
	AmtPaid lnwire.MilliSatoshi

	// Htlcs records all htlcs that paid to this invoice. Some of these
	// htlcs may have been marked as canceled.
	Htlcs map[CircuitKey]*InvoiceHTLC

	// HodlInvoice indicates whether the invoice should be held in the
	// Accepted state or be settled right away.
	HodlInvoice bool
}

// HTLCSet returns the set of accepted HTLCs belonging to an invoice. Passing a
// nil setID will return all accepted HTLCs in the case of legacy or MPP.
// Otherwise, the returned set will be filtered by the populated setID which is
// used to retrieve AMP HTLC sets.
func (i *Invoice) HTLCSet(setID *[32]byte) map[CircuitKey]*InvoiceHTLC {
	htlcSet := make(map[CircuitKey]*InvoiceHTLC)
	for key, htlc := range i.Htlcs {
		// Only consider accepted mpp htlcs. It is possible that there
		// are htlcs registered in the invoice database that previously
		// timed out and are in the canceled state now.
		if htlc.State != HtlcStateAccepted {
			continue
		}

		if !htlc.IsInHTLCSet(setID) {
			continue
		}

		htlcSet[key] = htlc
	}

	return htlcSet
}

// HtlcState defines the states an htlc paying to an invoice can be in.
type HtlcState uint8

const (
	// HtlcStateAccepted indicates the htlc is locked-in, but not resolved.
	HtlcStateAccepted HtlcState = iota

	// HtlcStateCanceled indicates the htlc is canceled back to the
	// sender.
	HtlcStateCanceled

	// HtlcStateSettled indicates the htlc is settled.
	HtlcStateSettled
)

// InvoiceHTLC contains details about an htlc paying to this invoice.
type InvoiceHTLC struct {
	// Amt is the amount that is carried by this htlc.
	Amt lnwire.MilliSatoshi

	// MppTotalAmt is a field for mpp that indicates the expected total
	// amount.
	MppTotalAmt lnwire.MilliSatoshi

	// AcceptHeight is the block height at which the invoice registry
	// decided to accept this htlc as a payment to the invoice. At this
	// height, the invoice cltv delay must have been met.
	AcceptHeight uint32

	// AcceptTime is the wall clock time at which the invoice registry
	// decided to accept the htlc.
	AcceptTime time.Time

	// ResolveTime is the wall clock time at which the invoice registry
	// decided to settle the htlc.
	ResolveTime time.Time

	// Expiry is the expiry height of this htlc.
	Expiry uint32

	// State indicates the state the invoice htlc is currently in. A
	// canceled htlc isn't just removed from the invoice htlcs map, because
	// we need AcceptHeight to properly cancel the htlc back.
	State HtlcState

	// CustomRecords contains the custom key/value pairs that accompanied
	// the htlc.
	CustomRecords record.CustomSet

	// AMP is a copy of the AMP record presented in the onion payload
	// containing the information necessary to correlate and settle a
	// spontaneous HTLC set. Newly accepted legacy keysend payments will
	// also have this field set as we automatically promote them into an AMP
	// payment for internal processing.
	//
	// NOTE: This value will only be set for AMP HTLCs.
	AMP *record.AMP

	// Hash is an HTLC-level payment hash that is stored only for AMP
	// payments. This is done because an AMP HTLC will carry a different
	// payment hash from the invoice it might be satisfying, so we track the
	// payment hashes individually to able to compute whether or not the
	// reconstructred preimage correctly matches the HTLC's hash.
	//
	// NOTE: This value will only be set for AMP HTLCs.
	Hash *lntypes.Hash

	// Preimage is an HTLC-level preimage that satisfies the AMP HTLC's
	// Hash. The preimage can be derived either from secret share
	// reconstruction of the shares in the AMP payload, for transmitted
	// direcetly in the onion payload for the case of legacy keysend.
	//
	// NOTE: This value will only be set for AMP HTLCs.
	Preimage *lntypes.Preimage
}

// IsInHTLCSet returns true if this HTLC is part an HTLC set. If nil is passed,
// this method returns true if this is an MPP HTLC. Otherwise, it only returns
// true if the AMP HTLC's set id matches the populated setID.
func (h *InvoiceHTLC) IsInHTLCSet(setID *[32]byte) bool {
	wantAMPSet := setID != nil
	isAMPHtlc := h.AMP != nil

	// Non-AMP HTLCs cannot be part of AMP HTLC sets, and vice versa.
	if wantAMPSet != isAMPHtlc {
		return false
	}

	// Skip AMP HTLCs that have differing set ids.
	if isAMPHtlc && *setID != h.AMP.SetID() {
		return false
	}

	return true
}

// HtlcAcceptDesc describes the details of a newly accepted htlc.
type HtlcAcceptDesc struct {
	// AcceptHeight is the block height at which this htlc was accepted.
	AcceptHeight int32

	// Amt is the amount that is carried by this htlc.
	Amt lnwire.MilliSatoshi

	// MppTotalAmt is a field for mpp that indicates the expected total
	// amount.
	MppTotalAmt lnwire.MilliSatoshi

	// Expiry is the expiry height of this htlc.
	Expiry uint32

	// CustomRecords contains the custom key/value pairs that accompanied
	// the htlc.
	CustomRecords record.CustomSet

	// AMP is a copy of the AMP record presented in the onion payload
	// containing the information necessary to correlate and settle a
	// spontaneous HTLC set. Newly accepted legacy keysend payments will
	// also have this field set as we automatically promote them into an AMP
	// payment for internal processing.
	//
	// NOTE: This value will only be set for AMP HTLCs.
	AMP *record.AMP

	// Hash is an HTLC-level payment hash that is stored only for AMP
	// payments. This is done because an AMP HTLC will carry a different
	// payment hash from the invoice it might be satisfying, so we track the
	// payment hashes individually to able to compute whether or not the
	// reconstructred preimage correctly matches the HTLC's hash.
	//
	// NOTE: This value will only be set for AMP HTLCs.
	Hash *lntypes.Hash

	// Preimage is an HTLC-level preimage that satisfies the AMP HTLC's
	// Hash. The preimage can be derived either from secret share
	// reconstruction of the shares in the AMP payload, for transmitted
	// direcetly in the onion payload for the case of legacy keysend.
	//
	// NOTE: This value will only be set for AMP HTLCs.
	Preimage *lntypes.Preimage
}

// InvoiceUpdateDesc describes the changes that should be applied to the
// invoice.
type InvoiceUpdateDesc struct {
	// State is the new state that this invoice should progress to. If nil,
	// the state is left unchanged.
	State *InvoiceStateUpdateDesc

	// CancelHtlcs describes the htlcs that need to be canceled.
	CancelHtlcs map[CircuitKey]struct{}

	// AddHtlcs describes the newly accepted htlcs that need to be added to
	// the invoice.
	AddHtlcs map[CircuitKey]*HtlcAcceptDesc
}

// InvoiceStateUpdateDesc describes an invoice-level state transition.
type InvoiceStateUpdateDesc struct {
	// NewState is the new state that this invoice should progress to.
	NewState ContractState

	// Preimage must be set to the preimage when NewState is settled.
	Preimage *lntypes.Preimage

	// SetID identifies a specific set of HTLCs destined for the same
	// invoice as part of a larger AMP payment. This value will be nil for
	// legacy or MPP payments.
	SetID *[32]byte
}

// InvoiceUpdateCallback is a callback used in the db transaction to update the
// invoice.
type InvoiceUpdateCallback = func(invoice *Invoice) (*InvoiceUpdateDesc, error)

func validateInvoice(i *Invoice, paymentHash lntypes.Hash) error {
	// Avoid conflicts with all-zeroes magic value in the database.
	if paymentHash == unknownPreimage.Hash() {
		return fmt.Errorf("cannot use hash of all-zeroes preimage")
	}

	if len(i.Memo) > MaxMemoSize {
		return fmt.Errorf("max length a memo is %v, and invoice "+
			"of length %v was provided", MaxMemoSize, len(i.Memo))
	}
	if len(i.PaymentRequest) > MaxPaymentRequestSize {
		return fmt.Errorf("max length of payment request is %v, length "+
			"provided was %v", MaxPaymentRequestSize,
			len(i.PaymentRequest))
	}
	if i.Terms.Features == nil {
		return errors.New("invoice must have a feature vector")
	}

	if i.Terms.PaymentPreimage == nil && !i.HodlInvoice {
		return errors.New("non-hodl invoices must have a preimage")
	}

	if len(i.Htlcs) > 0 {
		return ErrInvoiceHasHtlcs
	}

	return nil
}

// IsPending returns ture if the invoice is in ContractOpen state.
func (i *Invoice) IsPending() bool {
	return i.State == ContractOpen || i.State == ContractAccepted
}

// AddInvoice inserts the targeted invoice into the database. If the invoice has
// *any* payment hashes which already exists within the database, then the
// insertion will be aborted and rejected due to the strict policy banning any
// duplicate payment hashes. A side effect of this function is that it sets
// AddIndex on newInvoice.
func (d *DB) AddInvoice(newInvoice *Invoice, paymentHash lntypes.Hash) (
	uint64, error) {

	if err := validateInvoice(newInvoice, paymentHash); err != nil {
		return 0, err
	}

	var invoiceAddIndex uint64
	err := kvdb.Update(d, func(tx kvdb.RwTx) error {
		invoices, err := tx.CreateTopLevelBucket(invoiceBucket)
		if err != nil {
			return err
		}

		invoiceIndex, err := invoices.CreateBucketIfNotExists(
			invoiceIndexBucket,
		)
		if err != nil {
			return err
		}
		addIndex, err := invoices.CreateBucketIfNotExists(
			addIndexBucket,
		)
		if err != nil {
			return err
		}

		// Ensure that an invoice an identical payment hash doesn't
		// already exist within the index.
		if invoiceIndex.Get(paymentHash[:]) != nil {
			return ErrDuplicateInvoice
		}

		payAddrIndex := tx.ReadWriteBucket(payAddrIndexBucket)
		if payAddrIndex.Get(newInvoice.Terms.PaymentAddr[:]) != nil {
			return ErrDuplicatePayAddr
		}

		// If the current running payment ID counter hasn't yet been
		// created, then create it now.
		var invoiceNum uint32
		invoiceCounter := invoiceIndex.Get(numInvoicesKey)
		if invoiceCounter == nil {
			var scratch [4]byte
			byteOrder.PutUint32(scratch[:], invoiceNum)
			err := invoiceIndex.Put(numInvoicesKey, scratch[:])
			if err != nil {
				return err
			}
		} else {
			invoiceNum = byteOrder.Uint32(invoiceCounter)
		}

		newIndex, err := putInvoice(
			invoices, invoiceIndex, payAddrIndex, addIndex,
			newInvoice, invoiceNum, paymentHash,
		)
		if err != nil {
			return err
		}

		invoiceAddIndex = newIndex
		return nil
	})
	if err != nil {
		return 0, err
	}

	return invoiceAddIndex, err
}

// InvoicesAddedSince can be used by callers to seek into the event time series
// of all the invoices added in the database. The specified sinceAddIndex
// should be the highest add index that the caller knows of. This method will
// return all invoices with an add index greater than the specified
// sinceAddIndex.
//
// NOTE: The index starts from 1, as a result. We enforce that specifying a
// value below the starting index value is a noop.
func (d *DB) InvoicesAddedSince(sinceAddIndex uint64) ([]Invoice, error) {
	var newInvoices []Invoice

	// If an index of zero was specified, then in order to maintain
	// backwards compat, we won't send out any new invoices.
	if sinceAddIndex == 0 {
		return newInvoices, nil
	}

	var startIndex [8]byte
	byteOrder.PutUint64(startIndex[:], sinceAddIndex)

	err := kvdb.View(d, func(tx kvdb.RTx) error {
		invoices := tx.ReadBucket(invoiceBucket)
		if invoices == nil {
			return nil
		}

		addIndex := invoices.NestedReadBucket(addIndexBucket)
		if addIndex == nil {
			return nil
		}

		// We'll now run through each entry in the add index starting
		// at our starting index. We'll continue until we reach the
		// very end of the current key space.
		invoiceCursor := addIndex.ReadCursor()

		// We'll seek to the starting index, then manually advance the
		// cursor in order to skip the entry with the since add index.
		invoiceCursor.Seek(startIndex[:])
		addSeqNo, invoiceKey := invoiceCursor.Next()

		for ; addSeqNo != nil && bytes.Compare(addSeqNo, startIndex[:]) > 0; addSeqNo, invoiceKey = invoiceCursor.Next() {

			// For each key found, we'll look up the actual
			// invoice, then accumulate it into our return value.
			invoice, err := fetchInvoice(invoiceKey, invoices)
			if err != nil {
				return err
			}

			newInvoices = append(newInvoices, invoice)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return newInvoices, nil
}

// LookupInvoice attempts to look up an invoice according to its 32 byte
// payment hash. If an invoice which can settle the HTLC identified by the
// passed payment hash isn't found, then an error is returned. Otherwise, the
// full invoice is returned. Before setting the incoming HTLC, the values
// SHOULD be checked to ensure the payer meets the agreed upon contractual
// terms of the payment.
func (d *DB) LookupInvoice(ref InvoiceRef) (Invoice, error) {
	var invoice Invoice
	err := kvdb.View(d, func(tx kvdb.RTx) error {
		invoices := tx.ReadBucket(invoiceBucket)
		if invoices == nil {
			return ErrNoInvoicesCreated
		}
		invoiceIndex := invoices.NestedReadBucket(invoiceIndexBucket)
		if invoiceIndex == nil {
			return ErrNoInvoicesCreated
		}
		payAddrIndex := tx.ReadBucket(payAddrIndexBucket)
		setIDIndex := tx.ReadBucket(setIDIndexBucket)

		// Retrieve the invoice number for this invoice using
		// the provided invoice reference.
		invoiceNum, err := fetchInvoiceNumByRef(
			invoiceIndex, payAddrIndex, setIDIndex, ref,
		)
		if err != nil {
			return err
		}

		// An invoice was found, retrieve the remainder of the invoice
		// body.
		i, err := fetchInvoice(invoiceNum, invoices)
		if err != nil {
			return err
		}
		invoice = i

		return nil
	})
	if err != nil {
		return invoice, err
	}

	return invoice, nil
}

// fetchInvoiceNumByRef retrieve the invoice number for the provided invoice
// reference. The payment address will be treated as the primary key, falling
// back to the payment hash if nothing is found for the payment address. An
// error is returned if the invoice is not found.
func fetchInvoiceNumByRef(invoiceIndex, payAddrIndex, setIDIndex kvdb.RBucket,
	ref InvoiceRef) ([]byte, error) {

	// If the set id is present, we only consult the set id index for this
	// invoice. This type of query is only used to facilitate user-facing
	// requests to lookup, settle or cancel an AMP invoice.
	setID := ref.SetID()
	if setID != nil {
		invoiceNumBySetID := setIDIndex.Get(setID[:])
		if invoiceNumBySetID == nil {
			return nil, ErrInvoiceNotFound
		}

		return invoiceNumBySetID, nil
	}

	payHash := ref.PayHash()
	payAddr := ref.PayAddr()

	var (
		invoiceNumByHash = invoiceIndex.Get(payHash[:])
		invoiceNumByAddr []byte
	)
	if payAddr != nil {
		invoiceNumByAddr = payAddrIndex.Get(payAddr[:])
	}

	switch {

	// If payment address and payment hash both reference an existing
	// invoice, ensure they reference the _same_ invoice.
	case invoiceNumByAddr != nil && invoiceNumByHash != nil:
		if !bytes.Equal(invoiceNumByAddr, invoiceNumByHash) {
			return nil, ErrInvRefEquivocation
		}

		return invoiceNumByAddr, nil

	// If we were only able to reference the invoice by hash, return the
	// corresponding invoice number. This can happen when no payment address
	// was provided, or if it didn't match anything in our records.
	case invoiceNumByHash != nil:
		return invoiceNumByHash, nil

	// Otherwise we don't know of the target invoice.
	default:
		return nil, ErrInvoiceNotFound
	}
}

// InvoiceWithPaymentHash is used to store an invoice and its corresponding
// payment hash. This struct is only used to store results of
// ChannelDB.FetchAllInvoicesWithPaymentHash() call.
type InvoiceWithPaymentHash struct {
	// Invoice holds the invoice as selected from the invoices bucket.
	Invoice Invoice

	// PaymentHash is the payment hash for the Invoice.
	PaymentHash lntypes.Hash
}

// FetchAllInvoicesWithPaymentHash returns all invoices and their payment hashes
// currently stored within the database. If the pendingOnly param is true, then
// only open or accepted invoices and their payment hashes will be returned,
// skipping all invoices that are fully settled or canceled. Note that the
// returned array is not ordered by add index.
func (d *DB) FetchAllInvoicesWithPaymentHash(pendingOnly bool) (
	[]InvoiceWithPaymentHash, error) {

	var result []InvoiceWithPaymentHash

	err := kvdb.View(d, func(tx kvdb.RTx) error {
		invoices := tx.ReadBucket(invoiceBucket)
		if invoices == nil {
			return ErrNoInvoicesCreated
		}

		invoiceIndex := invoices.NestedReadBucket(invoiceIndexBucket)
		if invoiceIndex == nil {
			// Mask the error if there's no invoice
			// index as that simply means there are no
			// invoices added yet to the DB. In this case
			// we simply return an empty list.
			return nil
		}

		return invoiceIndex.ForEach(func(k, v []byte) error {
			// Skip the special numInvoicesKey as that does not
			// point to a valid invoice.
			if bytes.Equal(k, numInvoicesKey) {
				return nil
			}

			if v == nil {
				return nil
			}

			invoice, err := fetchInvoice(v, invoices)
			if err != nil {
				return err
			}

			if pendingOnly && !invoice.IsPending() {
				return nil
			}

			invoiceWithPaymentHash := InvoiceWithPaymentHash{
				Invoice: invoice,
			}

			copy(invoiceWithPaymentHash.PaymentHash[:], k)
			result = append(result, invoiceWithPaymentHash)

			return nil
		})
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

// InvoiceQuery represents a query to the invoice database. The query allows a
// caller to retrieve all invoices starting from a particular add index and
// limit the number of results returned.
type InvoiceQuery struct {
	// IndexOffset is the offset within the add indices to start at. This
	// can be used to start the response at a particular invoice.
	IndexOffset uint64

	// NumMaxInvoices is the maximum number of invoices that should be
	// starting from the add index.
	NumMaxInvoices uint64

	// PendingOnly, if set, returns unsettled invoices starting from the
	// add index.
	PendingOnly bool

	// Reversed, if set, indicates that the invoices returned should start
	// from the IndexOffset and go backwards.
	Reversed bool
}

// InvoiceSlice is the response to a invoice query. It includes the original
// query, the set of invoices that match the query, and an integer which
// represents the offset index of the last item in the set of returned invoices.
// This integer allows callers to resume their query using this offset in the
// event that the query's response exceeds the maximum number of returnable
// invoices.
type InvoiceSlice struct {
	InvoiceQuery

	// Invoices is the set of invoices that matched the query above.
	Invoices []Invoice

	// FirstIndexOffset is the index of the first element in the set of
	// returned Invoices above. Callers can use this to resume their query
	// in the event that the slice has too many events to fit into a single
	// response.
	FirstIndexOffset uint64

	// LastIndexOffset is the index of the last element in the set of
	// returned Invoices above. Callers can use this to resume their query
	// in the event that the slice has too many events to fit into a single
	// response.
	LastIndexOffset uint64
}

// QueryInvoices allows a caller to query the invoice database for invoices
// within the specified add index range.
func (d *DB) QueryInvoices(q InvoiceQuery) (InvoiceSlice, error) {
	resp := InvoiceSlice{
		InvoiceQuery: q,
	}

	err := kvdb.View(d, func(tx kvdb.RTx) error {
		// If the bucket wasn't found, then there aren't any invoices
		// within the database yet, so we can simply exit.
		invoices := tx.ReadBucket(invoiceBucket)
		if invoices == nil {
			return ErrNoInvoicesCreated
		}

		// Get the add index bucket which we will use to iterate through
		// our indexed invoices.
		invoiceAddIndex := invoices.NestedReadBucket(addIndexBucket)
		if invoiceAddIndex == nil {
			return ErrNoInvoicesCreated
		}

		// Create a paginator which reads from our add index bucket with
		// the parameters provided by the invoice query.
		paginator := newPaginator(
			invoiceAddIndex.ReadCursor(), q.Reversed, q.IndexOffset,
			q.NumMaxInvoices,
		)

		// accumulateInvoices looks up an invoice based on the index we
		// are given, adds it to our set of invoices if it has the right
		// characteristics for our query and returns the number of items
		// we have added to our set of invoices.
		accumulateInvoices := func(_, indexValue []byte) (bool, error) {
			invoice, err := fetchInvoice(indexValue, invoices)
			if err != nil {
				return false, err
			}

			// Skip any settled or canceled invoices if the caller
			// is only interested in pending ones.
			if q.PendingOnly && !invoice.IsPending() {
				return false, nil
			}

			// At this point, we've exhausted the offset, so we'll
			// begin collecting invoices found within the range.
			resp.Invoices = append(resp.Invoices, invoice)
			return true, nil
		}

		// Query our paginator using accumulateInvoices to build up a
		// set of invoices.
		if err := paginator.query(accumulateInvoices); err != nil {
			return err
		}

		// If we iterated through the add index in reverse order, then
		// we'll need to reverse the slice of invoices to return them in
		// forward order.
		if q.Reversed {
			numInvoices := len(resp.Invoices)
			for i := 0; i < numInvoices/2; i++ {
				opposite := numInvoices - i - 1
				resp.Invoices[i], resp.Invoices[opposite] =
					resp.Invoices[opposite], resp.Invoices[i]
			}
		}

		return nil
	})
	if err != nil && err != ErrNoInvoicesCreated {
		return resp, err
	}

	// Finally, record the indexes of the first and last invoices returned
	// so that the caller can resume from this point later on.
	if len(resp.Invoices) > 0 {
		resp.FirstIndexOffset = resp.Invoices[0].AddIndex
		resp.LastIndexOffset = resp.Invoices[len(resp.Invoices)-1].AddIndex
	}

	return resp, nil
}

// UpdateInvoice attempts to update an invoice corresponding to the passed
// payment hash. If an invoice matching the passed payment hash doesn't exist
// within the database, then the action will fail with a "not found" error.
//
// The update is performed inside the same database transaction that fetches the
// invoice and is therefore atomic. The fields to update are controlled by the
// supplied callback.
func (d *DB) UpdateInvoice(ref InvoiceRef,
	callback InvoiceUpdateCallback) (*Invoice, error) {

	var updatedInvoice *Invoice
	err := kvdb.Update(d, func(tx kvdb.RwTx) error {
		invoices, err := tx.CreateTopLevelBucket(invoiceBucket)
		if err != nil {
			return err
		}
		invoiceIndex, err := invoices.CreateBucketIfNotExists(
			invoiceIndexBucket,
		)
		if err != nil {
			return err
		}
		settleIndex, err := invoices.CreateBucketIfNotExists(
			settleIndexBucket,
		)
		if err != nil {
			return err
		}
		payAddrIndex := tx.ReadBucket(payAddrIndexBucket)
		setIDIndex := tx.ReadWriteBucket(setIDIndexBucket)

		// Retrieve the invoice number for this invoice using the
		// provided invoice reference.
		invoiceNum, err := fetchInvoiceNumByRef(
			invoiceIndex, payAddrIndex, setIDIndex, ref,
		)
		if err != nil {
			return err
		}

		payHash := ref.PayHash()
		updatedInvoice, err = d.updateInvoice(
			payHash, invoices, settleIndex, setIDIndex,
			invoiceNum, callback,
		)

		return err
	})

	return updatedInvoice, err
}

// InvoicesSettledSince can be used by callers to catch up any settled invoices
// they missed within the settled invoice time series. We'll return all known
// settled invoice that have a settle index higher than the passed
// sinceSettleIndex.
//
// NOTE: The index starts from 1, as a result. We enforce that specifying a
// value below the starting index value is a noop.
func (d *DB) InvoicesSettledSince(sinceSettleIndex uint64) ([]Invoice, error) {
	var settledInvoices []Invoice

	// If an index of zero was specified, then in order to maintain
	// backwards compat, we won't send out any new invoices.
	if sinceSettleIndex == 0 {
		return settledInvoices, nil
	}

	var startIndex [8]byte
	byteOrder.PutUint64(startIndex[:], sinceSettleIndex)

	err := kvdb.View(d, func(tx kvdb.RTx) error {
		invoices := tx.ReadBucket(invoiceBucket)
		if invoices == nil {
			return nil
		}

		settleIndex := invoices.NestedReadBucket(settleIndexBucket)
		if settleIndex == nil {
			return nil
		}

		// We'll now run through each entry in the add index starting
		// at our starting index. We'll continue until we reach the
		// very end of the current key space.
		invoiceCursor := settleIndex.ReadCursor()

		// We'll seek to the starting index, then manually advance the
		// cursor in order to skip the entry with the since add index.
		invoiceCursor.Seek(startIndex[:])
		seqNo, invoiceKey := invoiceCursor.Next()

		for ; seqNo != nil && bytes.Compare(seqNo, startIndex[:]) > 0; seqNo, invoiceKey = invoiceCursor.Next() {

			// For each key found, we'll look up the actual
			// invoice, then accumulate it into our return value.
			invoice, err := fetchInvoice(invoiceKey, invoices)
			if err != nil {
				return err
			}

			settledInvoices = append(settledInvoices, invoice)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return settledInvoices, nil
}

func putInvoice(invoices, invoiceIndex, payAddrIndex, addIndex kvdb.RwBucket,
	i *Invoice, invoiceNum uint32, paymentHash lntypes.Hash) (
	uint64, error) {

	// Create the invoice key which is just the big-endian representation
	// of the invoice number.
	var invoiceKey [4]byte
	byteOrder.PutUint32(invoiceKey[:], invoiceNum)

	// Increment the num invoice counter index so the next invoice bares
	// the proper ID.
	var scratch [4]byte
	invoiceCounter := invoiceNum + 1
	byteOrder.PutUint32(scratch[:], invoiceCounter)
	if err := invoiceIndex.Put(numInvoicesKey, scratch[:]); err != nil {
		return 0, err
	}

	// Add the payment hash to the invoice index. This will let us quickly
	// identify if we can settle an incoming payment, and also to possibly
	// allow a single invoice to have multiple payment installations.
	err := invoiceIndex.Put(paymentHash[:], invoiceKey[:])
	if err != nil {
		return 0, err
	}
	err = payAddrIndex.Put(i.Terms.PaymentAddr[:], invoiceKey[:])
	if err != nil {
		return 0, err
	}

	// Next, we'll obtain the next add invoice index (sequence
	// number), so we can properly place this invoice within this
	// event stream.
	nextAddSeqNo, err := addIndex.NextSequence()
	if err != nil {
		return 0, err
	}

	// With the next sequence obtained, we'll updating the event series in
	// the add index bucket to map this current add counter to the index of
	// this new invoice.
	var seqNoBytes [8]byte
	byteOrder.PutUint64(seqNoBytes[:], nextAddSeqNo)
	if err := addIndex.Put(seqNoBytes[:], invoiceKey[:]); err != nil {
		return 0, err
	}

	i.AddIndex = nextAddSeqNo

	// Finally, serialize the invoice itself to be written to the disk.
	var buf bytes.Buffer
	if err := serializeInvoice(&buf, i); err != nil {
		return 0, err
	}

	if err := invoices.Put(invoiceKey[:], buf.Bytes()); err != nil {
		return 0, err
	}

	return nextAddSeqNo, nil
}

// serializeInvoice serializes an invoice to a writer.
//
// Note: this function is in use for a migration. Before making changes that
// would modify the on disk format, make a copy of the original code and store
// it with the migration.
func serializeInvoice(w io.Writer, i *Invoice) error {
	creationDateBytes, err := i.CreationDate.MarshalBinary()
	if err != nil {
		return err
	}

	settleDateBytes, err := i.SettleDate.MarshalBinary()
	if err != nil {
		return err
	}

	var fb bytes.Buffer
	err = i.Terms.Features.EncodeBase256(&fb)
	if err != nil {
		return err
	}
	featureBytes := fb.Bytes()

	preimage := [32]byte(unknownPreimage)
	if i.Terms.PaymentPreimage != nil {
		preimage = *i.Terms.PaymentPreimage
		if preimage == unknownPreimage {
			return errors.New("cannot use all-zeroes preimage")
		}
	}
	value := uint64(i.Terms.Value)
	cltvDelta := uint32(i.Terms.FinalCltvDelta)
	expiry := uint64(i.Terms.Expiry)

	amtPaid := uint64(i.AmtPaid)
	state := uint8(i.State)

	var hodlInvoice uint8
	if i.HodlInvoice {
		hodlInvoice = 1
	}

	tlvStream, err := tlv.NewStream(
		// Memo and payreq.
		tlv.MakePrimitiveRecord(memoType, &i.Memo),
		tlv.MakePrimitiveRecord(payReqType, &i.PaymentRequest),

		// Add/settle metadata.
		tlv.MakePrimitiveRecord(createTimeType, &creationDateBytes),
		tlv.MakePrimitiveRecord(settleTimeType, &settleDateBytes),
		tlv.MakePrimitiveRecord(addIndexType, &i.AddIndex),
		tlv.MakePrimitiveRecord(settleIndexType, &i.SettleIndex),

		// Terms.
		tlv.MakePrimitiveRecord(preimageType, &preimage),
		tlv.MakePrimitiveRecord(valueType, &value),
		tlv.MakePrimitiveRecord(cltvDeltaType, &cltvDelta),
		tlv.MakePrimitiveRecord(expiryType, &expiry),
		tlv.MakePrimitiveRecord(paymentAddrType, &i.Terms.PaymentAddr),
		tlv.MakePrimitiveRecord(featuresType, &featureBytes),

		// Invoice state.
		tlv.MakePrimitiveRecord(invStateType, &state),
		tlv.MakePrimitiveRecord(amtPaidType, &amtPaid),

		tlv.MakePrimitiveRecord(hodlInvoiceType, &hodlInvoice),
	)
	if err != nil {
		return err
	}

	var b bytes.Buffer
	if err = tlvStream.Encode(&b); err != nil {
		return err
	}

	err = binary.Write(w, byteOrder, uint64(b.Len()))
	if err != nil {
		return err
	}

	if _, err = w.Write(b.Bytes()); err != nil {
		return err
	}

	return serializeHtlcs(w, i.Htlcs)
}

// serializeHtlcs serializes a map containing circuit keys and invoice htlcs to
// a writer.
func serializeHtlcs(w io.Writer, htlcs map[CircuitKey]*InvoiceHTLC) error {
	for key, htlc := range htlcs {
		// Encode the htlc in a tlv stream.
		chanID := key.ChanID.ToUint64()
		amt := uint64(htlc.Amt)
		mppTotalAmt := uint64(htlc.MppTotalAmt)
		acceptTime := putNanoTime(htlc.AcceptTime)
		resolveTime := putNanoTime(htlc.ResolveTime)
		state := uint8(htlc.State)

		var records []tlv.Record
		records = append(records,
			tlv.MakePrimitiveRecord(chanIDType, &chanID),
			tlv.MakePrimitiveRecord(htlcIDType, &key.HtlcID),
			tlv.MakePrimitiveRecord(amtType, &amt),
			tlv.MakePrimitiveRecord(
				acceptHeightType, &htlc.AcceptHeight,
			),
			tlv.MakePrimitiveRecord(acceptTimeType, &acceptTime),
			tlv.MakePrimitiveRecord(resolveTimeType, &resolveTime),
			tlv.MakePrimitiveRecord(expiryHeightType, &htlc.Expiry),
			tlv.MakePrimitiveRecord(htlcStateType, &state),
			tlv.MakePrimitiveRecord(mppTotalAmtType, &mppTotalAmt),
		)

		if htlc.AMP != nil {
			setIDRecord := tlv.MakeDynamicRecord(
				htlcAMPType, htlc.AMP, htlc.AMP.PayloadSize,
				record.AMPEncoder, record.AMPDecoder,
			)
			records = append(records, setIDRecord)
		}
		if htlc.Hash != nil {
			hash32 := [32]byte(*htlc.Hash)
			hashRecord := tlv.MakePrimitiveRecord(
				htlcHashType, &hash32,
			)
			records = append(records, hashRecord)
		}
		if htlc.Preimage != nil {
			preimage32 := [32]byte(*htlc.Preimage)
			preimageRecord := tlv.MakePrimitiveRecord(
				htlcPreimageType, &preimage32,
			)
			records = append(records, preimageRecord)
		}

		// Convert the custom records to tlv.Record types that are ready
		// for serialization.
		customRecords := tlv.MapToRecords(htlc.CustomRecords)

		// Append the custom records. Their ids are in the experimental
		// range and sorted, so there is no need to sort again.
		records = append(records, customRecords...)

		tlvStream, err := tlv.NewStream(records...)
		if err != nil {
			return err
		}

		var b bytes.Buffer
		if err := tlvStream.Encode(&b); err != nil {
			return err
		}

		// Write the length of the tlv stream followed by the stream
		// bytes.
		err = binary.Write(w, byteOrder, uint64(b.Len()))
		if err != nil {
			return err
		}

		if _, err := w.Write(b.Bytes()); err != nil {
			return err
		}
	}

	return nil
}

// putNanoTime returns the unix nano time for the passed timestamp. A zero-value
// timestamp will be mapped to 0, since calling UnixNano in that case is
// undefined.
func putNanoTime(t time.Time) uint64 {
	if t.IsZero() {
		return 0
	}
	return uint64(t.UnixNano())
}

// getNanoTime returns a timestamp for the given number of nano seconds. If zero
// is provided, an zero-value time stamp is returned.
func getNanoTime(ns uint64) time.Time {
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, int64(ns))
}

func fetchInvoice(invoiceNum []byte, invoices kvdb.RBucket) (Invoice, error) {
	invoiceBytes := invoices.Get(invoiceNum)
	if invoiceBytes == nil {
		return Invoice{}, ErrInvoiceNotFound
	}

	invoiceReader := bytes.NewReader(invoiceBytes)

	return deserializeInvoice(invoiceReader)
}

func deserializeInvoice(r io.Reader) (Invoice, error) {
	var (
		preimageBytes [32]byte
		value         uint64
		cltvDelta     uint32
		expiry        uint64
		amtPaid       uint64
		state         uint8
		hodlInvoice   uint8

		creationDateBytes []byte
		settleDateBytes   []byte
		featureBytes      []byte
	)

	var i Invoice
	tlvStream, err := tlv.NewStream(
		// Memo and payreq.
		tlv.MakePrimitiveRecord(memoType, &i.Memo),
		tlv.MakePrimitiveRecord(payReqType, &i.PaymentRequest),

		// Add/settle metadata.
		tlv.MakePrimitiveRecord(createTimeType, &creationDateBytes),
		tlv.MakePrimitiveRecord(settleTimeType, &settleDateBytes),
		tlv.MakePrimitiveRecord(addIndexType, &i.AddIndex),
		tlv.MakePrimitiveRecord(settleIndexType, &i.SettleIndex),

		// Terms.
		tlv.MakePrimitiveRecord(preimageType, &preimageBytes),
		tlv.MakePrimitiveRecord(valueType, &value),
		tlv.MakePrimitiveRecord(cltvDeltaType, &cltvDelta),
		tlv.MakePrimitiveRecord(expiryType, &expiry),
		tlv.MakePrimitiveRecord(paymentAddrType, &i.Terms.PaymentAddr),
		tlv.MakePrimitiveRecord(featuresType, &featureBytes),

		// Invoice state.
		tlv.MakePrimitiveRecord(invStateType, &state),
		tlv.MakePrimitiveRecord(amtPaidType, &amtPaid),

		tlv.MakePrimitiveRecord(hodlInvoiceType, &hodlInvoice),
	)
	if err != nil {
		return i, err
	}

	var bodyLen int64
	err = binary.Read(r, byteOrder, &bodyLen)
	if err != nil {
		return i, err
	}

	lr := io.LimitReader(r, bodyLen)
	if err = tlvStream.Decode(lr); err != nil {
		return i, err
	}

	preimage := lntypes.Preimage(preimageBytes)
	if preimage != unknownPreimage {
		i.Terms.PaymentPreimage = &preimage
	}

	i.Terms.Value = lnwire.MilliSatoshi(value)
	i.Terms.FinalCltvDelta = int32(cltvDelta)
	i.Terms.Expiry = time.Duration(expiry)
	i.AmtPaid = lnwire.MilliSatoshi(amtPaid)
	i.State = ContractState(state)

	if hodlInvoice != 0 {
		i.HodlInvoice = true
	}

	err = i.CreationDate.UnmarshalBinary(creationDateBytes)
	if err != nil {
		return i, err
	}

	err = i.SettleDate.UnmarshalBinary(settleDateBytes)
	if err != nil {
		return i, err
	}

	rawFeatures := lnwire.NewRawFeatureVector()
	err = rawFeatures.DecodeBase256(
		bytes.NewReader(featureBytes), len(featureBytes),
	)
	if err != nil {
		return i, err
	}

	i.Terms.Features = lnwire.NewFeatureVector(
		rawFeatures, lnwire.Features,
	)

	i.Htlcs, err = deserializeHtlcs(r)
	return i, err
}

// deserializeHtlcs reads a list of invoice htlcs from a reader and returns it
// as a map.
func deserializeHtlcs(r io.Reader) (map[CircuitKey]*InvoiceHTLC, error) {
	htlcs := make(map[CircuitKey]*InvoiceHTLC)

	for {
		// Read the length of the tlv stream for this htlc.
		var streamLen int64
		if err := binary.Read(r, byteOrder, &streamLen); err != nil {
			if err == io.EOF {
				break
			}

			return nil, err
		}

		// Limit the reader so that it stops at the end of this htlc's
		// stream.
		htlcReader := io.LimitReader(r, streamLen)

		// Decode the contents into the htlc fields.
		var (
			htlc                    InvoiceHTLC
			key                     CircuitKey
			chanID                  uint64
			state                   uint8
			acceptTime, resolveTime uint64
			amt, mppTotalAmt        uint64
			amp                     = &record.AMP{}
			hash32                  = &[32]byte{}
			preimage32              = &[32]byte{}
		)
		tlvStream, err := tlv.NewStream(
			tlv.MakePrimitiveRecord(chanIDType, &chanID),
			tlv.MakePrimitiveRecord(htlcIDType, &key.HtlcID),
			tlv.MakePrimitiveRecord(amtType, &amt),
			tlv.MakePrimitiveRecord(
				acceptHeightType, &htlc.AcceptHeight,
			),
			tlv.MakePrimitiveRecord(acceptTimeType, &acceptTime),
			tlv.MakePrimitiveRecord(resolveTimeType, &resolveTime),
			tlv.MakePrimitiveRecord(expiryHeightType, &htlc.Expiry),
			tlv.MakePrimitiveRecord(htlcStateType, &state),
			tlv.MakePrimitiveRecord(mppTotalAmtType, &mppTotalAmt),
			tlv.MakeDynamicRecord(
				htlcAMPType, amp, amp.PayloadSize,
				record.AMPEncoder, record.AMPDecoder,
			),
			tlv.MakePrimitiveRecord(htlcHashType, hash32),
			tlv.MakePrimitiveRecord(htlcPreimageType, preimage32),
		)
		if err != nil {
			return nil, err
		}

		parsedTypes, err := tlvStream.DecodeWithParsedTypes(htlcReader)
		if err != nil {
			return nil, err
		}

		if _, ok := parsedTypes[htlcAMPType]; !ok {
			amp = nil
		}

		var preimage *lntypes.Preimage
		if _, ok := parsedTypes[htlcPreimageType]; ok {
			pimg := lntypes.Preimage(*preimage32)
			preimage = &pimg
		}

		var hash *lntypes.Hash
		if _, ok := parsedTypes[htlcHashType]; ok {
			h := lntypes.Hash(*hash32)
			hash = &h
		}

		key.ChanID = lnwire.NewShortChanIDFromInt(chanID)
		htlc.AcceptTime = getNanoTime(acceptTime)
		htlc.ResolveTime = getNanoTime(resolveTime)
		htlc.State = HtlcState(state)
		htlc.Amt = lnwire.MilliSatoshi(amt)
		htlc.MppTotalAmt = lnwire.MilliSatoshi(mppTotalAmt)
		htlc.AMP = amp
		htlc.Hash = hash
		htlc.Preimage = preimage

		// Reconstruct the custom records fields from the parsed types
		// map return from the tlv parser.
		htlc.CustomRecords = hop.NewCustomRecords(parsedTypes)

		htlcs[key] = &htlc
	}

	return htlcs, nil
}

// copySlice allocates a new slice and copies the source into it.
func copySlice(src []byte) []byte {
	dest := make([]byte, len(src))
	copy(dest, src)
	return dest
}

// copyInvoiceHTLC makes a deep copy of the supplied invoice HTLC.
func copyInvoiceHTLC(src *InvoiceHTLC) *InvoiceHTLC {
	result := *src

	// Make a copy of the CustomSet map.
	result.CustomRecords = make(record.CustomSet)
	for k, v := range src.CustomRecords {
		result.CustomRecords[k] = v
	}

	if src.Hash != nil {
		hash := *src.Hash
		result.Hash = &hash
	}
	if src.Preimage != nil {
		preimage := *src.Preimage
		result.Preimage = &preimage
	}

	return &result
}

// copyInvoice makes a deep copy of the supplied invoice.
func copyInvoice(src *Invoice) *Invoice {
	dest := Invoice{
		Memo:           copySlice(src.Memo),
		PaymentRequest: copySlice(src.PaymentRequest),
		CreationDate:   src.CreationDate,
		SettleDate:     src.SettleDate,
		Terms:          src.Terms,
		AddIndex:       src.AddIndex,
		SettleIndex:    src.SettleIndex,
		State:          src.State,
		AmtPaid:        src.AmtPaid,
		Htlcs: make(
			map[CircuitKey]*InvoiceHTLC, len(src.Htlcs),
		),
		HodlInvoice: src.HodlInvoice,
	}

	dest.Terms.Features = src.Terms.Features.Clone()

	if src.Terms.PaymentPreimage != nil {
		preimage := *src.Terms.PaymentPreimage
		dest.Terms.PaymentPreimage = &preimage
	}

	for k, v := range src.Htlcs {
		dest.Htlcs[k] = copyInvoiceHTLC(v)
	}

	return &dest
}

// updateInvoice fetches the invoice, obtains the update descriptor from the
// callback and applies the updates in a single db transaction.
func (d *DB) updateInvoice(hash lntypes.Hash, invoices,
	settleIndex, setIDIndex kvdb.RwBucket, invoiceNum []byte,
	callback InvoiceUpdateCallback) (*Invoice, error) {

	invoice, err := fetchInvoice(invoiceNum, invoices)
	if err != nil {
		return nil, err
	}

	// Create deep copy to prevent any accidental modification in the
	// callback.
	invoiceCopy := copyInvoice(&invoice)

	// Call the callback and obtain the update descriptor.
	update, err := callback(invoiceCopy)
	if err != nil {
		return &invoice, err
	}

	// If there is nothing to update, return early.
	if update == nil {
		return &invoice, nil
	}

	var setID *[32]byte
	if update.State != nil {
		setID = update.State.SetID
	}

	now := d.clock.Now()

	// Update invoice state if the update descriptor indicates an invoice
	// state change.
	if update.State != nil {
		err := updateInvoiceState(&invoice, hash, *update.State)
		if err != nil {
			return nil, err
		}

		if update.State.NewState == ContractSettled {
			err := setSettleMetaFields(
				settleIndex, invoiceNum, &invoice, now,
			)
			if err != nil {
				return nil, err
			}
		}
	}

	// Process add actions from update descriptor.
	for key, htlcUpdate := range update.AddHtlcs {
		if _, exists := invoice.Htlcs[key]; exists {
			return nil, fmt.Errorf("duplicate add of htlc %v", key)
		}

		// Force caller to supply htlc without custom records in a
		// consistent way.
		if htlcUpdate.CustomRecords == nil {
			return nil, errors.New("nil custom records map")
		}

		// If a newly added HTLC has an associated set id, use it to
		// index this invoice in the set id index. An error is returned
		// if we find the index already points to a different invoice.
		if htlcUpdate.AMP != nil {
			setID := htlcUpdate.AMP.SetID()
			setIDInvNum := setIDIndex.Get(setID[:])
			if setIDInvNum == nil {
				err = setIDIndex.Put(setID[:], invoiceNum)
				if err != nil {
					return nil, err
				}
			} else if !bytes.Equal(setIDInvNum, invoiceNum) {
				return nil, ErrDuplicateSetID(setID)
			}
		}

		htlc := &InvoiceHTLC{
			Amt:           htlcUpdate.Amt,
			MppTotalAmt:   htlcUpdate.MppTotalAmt,
			Expiry:        htlcUpdate.Expiry,
			AcceptHeight:  uint32(htlcUpdate.AcceptHeight),
			AcceptTime:    now,
			State:         HtlcStateAccepted,
			CustomRecords: htlcUpdate.CustomRecords,
			AMP:           htlcUpdate.AMP,
			Hash:          htlcUpdate.Hash,
			Preimage:      htlcUpdate.Preimage,
		}

		invoice.Htlcs[key] = htlc
	}

	// Align htlc states with invoice state and recalculate amount paid.
	var (
		amtPaid     lnwire.MilliSatoshi
		cancelHtlcs = update.CancelHtlcs
	)
	for key, htlc := range invoice.Htlcs {
		// Check whether this htlc needs to be canceled. If it does,
		// update the htlc state to Canceled.
		_, cancel := cancelHtlcs[key]
		if cancel {
			// Consistency check to verify that there is no overlap
			// between the add and cancel sets.
			if _, added := update.AddHtlcs[key]; added {
				return nil, fmt.Errorf("added htlc %v canceled",
					key)
			}

			err := cancelSingleHtlc(now, htlc, invoice.State)
			if err != nil {
				return nil, err
			}

			// Delete processed cancel action, so that we can check
			// later that there are no actions left.
			delete(cancelHtlcs, key)

			continue
		}

		// The invoice state may have changed and this could have
		// implications for the states of the individual htlcs. Align
		// the htlc state with the current invoice state.
		err := updateHtlc(now, htlc, invoice.State, setID)
		if err != nil {
			return nil, err
		}

		// Update the running amount paid to this invoice. We don't
		// include accepted htlcs when the invoice is still open.
		if invoice.State != ContractOpen &&
			(htlc.State == HtlcStateAccepted ||
				htlc.State == HtlcStateSettled) {

			amtPaid += htlc.Amt
		}
	}
	invoice.AmtPaid = amtPaid

	// Verify that we didn't get an action for htlcs that are not present on
	// the invoice.
	if len(cancelHtlcs) > 0 {
		return nil, errors.New("cancel action on non-existent htlc(s)")
	}

	// Reserialize and update invoice.
	var buf bytes.Buffer
	if err := serializeInvoice(&buf, &invoice); err != nil {
		return nil, err
	}

	if err := invoices.Put(invoiceNum[:], buf.Bytes()); err != nil {
		return nil, err
	}

	return &invoice, nil
}

// updateInvoiceState validates and processes an invoice state update.
func updateInvoiceState(invoice *Invoice, hash lntypes.Hash,
	update InvoiceStateUpdateDesc) error {

	// Returning to open is never allowed from any state.
	if update.NewState == ContractOpen {
		return ErrInvoiceCannotOpen
	}

	switch invoice.State {

	// Once a contract is accepted, we can only transition to settled or
	// canceled. Forbid transitioning back into this state. Otherwise this
	// state is identical to ContractOpen, so we fallthrough to apply the
	// same checks that we apply to open invoices.
	case ContractAccepted:
		if update.NewState == ContractAccepted {
			return ErrInvoiceCannotAccept
		}

		fallthrough

	// If a contract is open, permit a state transition to accepted, settled
	// or canceled. The only restriction is on transitioning to settled
	// where we ensure the preimage is valid.
	case ContractOpen:
		if update.NewState == ContractCanceled {
			invoice.State = update.NewState
			return nil
		}

		// For AMP invoices, there are no invoice-level preimage checks.
		// However, we still sanity check that we aren't trying to
		// settle an AMP invoice with a preimage.
		if update.SetID != nil {
			if update.Preimage != nil {
				return errors.New("AMP set cannot have preimage")
			}
			invoice.State = update.NewState
			return nil
		}

		switch {

		// Validate the supplied preimage for non-AMP invoices.
		case update.Preimage != nil:
			if update.Preimage.Hash() != hash {
				return ErrInvoicePreimageMismatch
			}
			invoice.Terms.PaymentPreimage = update.Preimage

		// Permit non-AMP invoices to be accepted without knowing the
		// preimage. When trying to settle will have to pass through the
		// above check in order to not hit the one below.
		case update.NewState == ContractAccepted:

		// Fail if we still don't have a preimage when transitioning to
		// settle the non-AMP invoice.
		case invoice.Terms.PaymentPreimage == nil:
			return errors.New("unknown preimage")
		}

		invoice.State = update.NewState

		return nil

	// Once settled, we are in a terminal state.
	case ContractSettled:
		return ErrInvoiceAlreadySettled

	// Once canceled, we are in a terminal state.
	case ContractCanceled:
		return ErrInvoiceAlreadyCanceled

	default:
		return errors.New("unknown state transition")
	}
}

// cancelSingleHtlc validates cancelation of a single htlc and update its state.
func cancelSingleHtlc(resolveTime time.Time, htlc *InvoiceHTLC,
	invState ContractState) error {

	// It is only possible to cancel individual htlcs on an open invoice.
	if invState != ContractOpen {
		return fmt.Errorf("htlc canceled on invoice in "+
			"state %v", invState)
	}

	// It is only possible if the htlc is still pending.
	if htlc.State != HtlcStateAccepted {
		return fmt.Errorf("htlc canceled in state %v",
			htlc.State)
	}

	htlc.State = HtlcStateCanceled
	htlc.ResolveTime = resolveTime

	return nil
}

// updateHtlc aligns the state of an htlc with the given invoice state.
func updateHtlc(resolveTime time.Time, htlc *InvoiceHTLC,
	invState ContractState, setID *[32]byte) error {

	trySettle := func(persist bool) error {
		if htlc.State != HtlcStateAccepted {
			return nil
		}

		// Settle the HTLC if it matches the settled set id. Since we
		// only allow settling of one HTLC set (for now) we cancel any
		// that do not match the set id.
		var htlcState HtlcState
		if htlc.IsInHTLCSet(setID) {
			// Non-AMP HTLCs can be settled immediately since we
			// already know the preimage is valid due to checks at
			// the invoice level. For AMP HTLCs, verify that the
			// per-HTLC preimage-hash pair is valid.
			if setID != nil && !htlc.Preimage.Matches(*htlc.Hash) {
				return fmt.Errorf("AMP preimage mismatch, "+
					"preimage=%v hash=%v", htlc.Preimage,
					htlc.Hash)
			}

			htlcState = HtlcStateSettled
		} else {
			htlcState = HtlcStateCanceled
		}

		// Only persist the changes if the invoice is moving to the
		// settled state.
		if persist {
			htlc.State = htlcState
			htlc.ResolveTime = resolveTime
		}

		return nil
	}

	if invState == ContractSettled {
		return trySettle(true)
	}

	// We should never find a settled HTLC on an invoice that isn't in
	// ContractSettled.
	if htlc.State == HtlcStateSettled {
		return fmt.Errorf("cannot have a settled htlc with "+
			"invoice in state %v", invState)
	}

	switch invState {

	case ContractCanceled:
		if htlc.State == HtlcStateAccepted {
			htlc.State = HtlcStateCanceled
			htlc.ResolveTime = resolveTime
		}
		return nil

	case ContractAccepted:
		return trySettle(false)

	case ContractOpen:
		return nil

	default:
		return errors.New("unknown state transition")
	}
}

// setSettleMetaFields updates the metadata associated with settlement of an
// invoice.
func setSettleMetaFields(settleIndex kvdb.RwBucket, invoiceNum []byte,
	invoice *Invoice, now time.Time) error {

	// Now that we know the invoice hasn't already been settled, we'll
	// update the settle index so we can place this settle event in the
	// proper location within our time series.
	nextSettleSeqNo, err := settleIndex.NextSequence()
	if err != nil {
		return err
	}

	var seqNoBytes [8]byte
	byteOrder.PutUint64(seqNoBytes[:], nextSettleSeqNo)
	if err := settleIndex.Put(seqNoBytes[:], invoiceNum); err != nil {
		return err
	}

	invoice.SettleDate = now
	invoice.SettleIndex = nextSettleSeqNo

	return nil
}
