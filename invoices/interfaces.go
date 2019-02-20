package invoices

import (
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/lnwire"
)

// InvoiceDatabase is an interface which represents the persistent subsystem
// which may search, lookup and settle invoices.
type InvoiceDatabase interface {
	// LookupInvoice attempts to look up an invoice according to its 32
	// byte payment hash. This method should also reutrn the min final CLTV
	// delta for this invoice. We'll use this to ensure that the HTLC
	// extended to us gives us enough time to settle as we prescribe.
	LookupInvoice(lntypes.Hash) (channeldb.Invoice, uint32, error)

	// NotifyExitHopHtlc attempts to mark an invoice corresponding to the
	// passed payment hash as fully settled.
	NotifyExitHopHtlc(payHash lntypes.Hash, paidAmount lnwire.MilliSatoshi,
		hodlChan chan<- interface{}) error

	// CancelInvoice attempts to cancel the invoice corresponding to the
	// passed payment hash.
	CancelInvoice(payHash lntypes.Hash) error
}
