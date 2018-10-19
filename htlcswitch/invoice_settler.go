package htlcswitch

import (
	"crypto/sha256"
	"fmt"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/contractcourt"
	"github.com/lightningnetwork/lnd/lnwire"
)

// InvoiceSettler handles settling and failing of invoices.
type InvoiceSettler struct {
	// Registry is a sub-system which responsible for managing the invoices
	// in thread-safe manner.
	registry InvoiceDatabase

	HtlcSwitch *Switch
}

// NewInvoiceSettler returns a new invoice settler instance.
func NewInvoiceSettler(registry InvoiceDatabase) *InvoiceSettler {
	return &InvoiceSettler{
		registry: registry,
	}
}

// Settle settles the htlc and marks the invoice as settled.
func (i *InvoiceSettler) Settle(preimage [32]byte) error {
	paymentHash := chainhash.Hash(sha256.Sum256(preimage[:]))

	invoice, _, err := i.registry.LookupInvoice(
		paymentHash,
	)
	if err != nil {
		return fmt.Errorf("unable to query invoice registry: "+
			" %v", err)
	}

	if invoice.Terms.State == channeldb.ContractSettled {
		return fmt.Errorf("invoice already settled")
	}

	// TODO: Implement hodl.BogusSettle?

	i.HtlcSwitch.ProcessContractResolution(
		contractcourt.ResolutionMsg{
			SourceChan: exitHop,
			HtlcIndex:  invoice.AddIndex,
			PreImage:   &preimage,
		},
	)

	// TODO: Set amount in accepted stage already
	err = i.registry.SettleInvoice(
		paymentHash, lnwire.MilliSatoshi(0),
	)
	if err != nil {
		log.Errorf("unable to settle invoice: %v", err)
	}

	log.Infof("Settled invoice %x", paymentHash)

	return nil
}

// Cancel fails the incoming HTLC with unknown payment hash.
func (i *InvoiceSettler) Cancel(paymentHash [32]byte) error {
	invoice, _, err := i.registry.LookupInvoice(
		paymentHash,
	)
	if err != nil {
		return fmt.Errorf("unable to query invoice registry: "+
			" %v", err)
	}

	if invoice.Terms.State == channeldb.ContractSettled {
		return fmt.Errorf("invoice already settled")
	}

	i.HtlcSwitch.ProcessContractResolution(
		contractcourt.ResolutionMsg{
			SourceChan: exitHop,
			HtlcIndex:  invoice.AddIndex,
			Failure:    lnwire.FailUnknownPaymentHash{},
		},
	)

	// TODO: Move invoice to canceled state / remove

	log.Infof("Canceled invoice %x", paymentHash)

	return nil
}

// handleIncoming is called from switch when a htlc comes in for which we are
// the exit hop.
func (i *InvoiceSettler) handleIncoming(pkt *htlcPacket) error {
	// We're the designated payment destination.  Therefore
	// we attempt to see if we have an invoice locally
	// which'll allow us to settle this htlc.
	invoiceHash := pkt.circuit.PaymentHash
	invoice, _, err := i.registry.LookupInvoice(
		invoiceHash,
	)

	// TODO: What happens when invoice is already settled?
	//
	// TODO: Concurrency, merge lookup and accept in single atomic
	// operation?

	// Notify the invoiceRegistry of the invoices we just
	// accepted this latest commitment update.
	err = i.registry.AcceptInvoice(
		invoiceHash, pkt.incomingAmount,
	)
	if err != nil {
		return fmt.Errorf("unable to accept invoice: %v", err)
	}

	// TODO: Deal with OutKey collision in case of pay more than once.
	i.HtlcSwitch.circuits.OpenCircuits(Keystone{
		InKey: pkt.circuit.Incoming,
		OutKey: CircuitKey{
			ChanID: exitHop,
			HtlcID: invoice.AddIndex,
		},
	})

	log.Infof("Accepted invoice %x", invoiceHash)

	return nil
}
