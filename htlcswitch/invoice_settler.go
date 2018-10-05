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

	// Notify the invoiceRegistry of the invoices we just
	// accepted this latest commitment update.
	err = i.registry.AcceptInvoice(
		invoiceHash, pkt.incomingAmount,
	)
	if err != nil {
		return fmt.Errorf("unable to accept invoice: %v", err)
	}

	i.HtlcSwitch.circuits.OpenCircuits(Keystone{
		InKey: pkt.circuit.Incoming,
		OutKey: CircuitKey{
			ChanID: exitHop,
			HtlcID: invoice.AddIndex,
		},
	})

	log.Infof("Accepted invoice %x", invoiceHash)

	// TODO: Return errors below synchronously or use async flow for
	// all cases?

	/*if err != nil {
		log.Errorf("unable to query invoice registry: "+
			" %v", err)
		failure := lnwire.FailUnknownPaymentHash{}

		i.ResolutionMsgs <- contractcourt.ResolutionMsg{
			SourceChan: exitHop,
			Failure:    failure,
		}
		l.sendHTLCError(
			pd.HtlcIndex, failure, obfuscator, pd.SourceRef,
		)

		needUpdate = true
		return
	}*/

	// If the invoice is already settled, we choose to
	// accept the payment to simplify failure recovery.
	//
	// NOTE: Though our recovery and forwarding logic is
	// predominately batched, settling invoices happens
	// iteratively. We may reject one of two payments
	// for the same rhash at first, but then restart and
	// reject both after seeing that the invoice has been
	// settled. Without any record of which one settles
	// first, it is ambiguous as to which one actually
	// settled the invoice. Thus, by accepting all
	// payments, we eliminate the race condition that can
	// lead to this inconsistency.
	//
	// TODO(conner): track ownership of settlements to
	// properly recover from failures? or add batch invoice
	// settlement

	/*if invoice.Terms.Settled {
		log.Warnf("Accepting duplicate payment for "+
			"hash=%x", invoiceHash)
	}*/

	// If we're not currently in debug mode, and the
	// extended htlc doesn't meet the value requested, then
	// we'll fail the htlc.  Otherwise, we settle this htlc
	// within our local state update log, then send the
	// update entry to the remote party.
	//
	// NOTE: We make an exception when the value requested
	// by the invoice is zero. This means the invoice
	// allows the payee to specify the amount of satoshis
	// they wish to send.  So since we expect the htlc to
	// have a different amount, we should not fail.
	/*if !l.cfg.DebugHTLC && invoice.Terms.Value > 0 &&
		pd.Amount < invoice.Terms.Value {

		log.Errorf("rejecting htlc due to incorrect "+
			"amount: expected %v, received %v",
			invoice.Terms.Value, pd.Amount)

		failure := lnwire.FailIncorrectPaymentAmount{}
		l.sendHTLCError(
			pd.HtlcIndex, failure, obfuscator, pd.SourceRef,
		)

		needUpdate = true
		return
	}*/

	// As we're the exit hop, we'll double check the
	// hop-payload included in the HTLC to ensure that it
	// was crafted correctly by the sender and matches the
	// HTLC we were extended.
	//
	// NOTE: We make an exception when the value requested
	// by the invoice is zero. This means the invoice
	// allows the payee to specify the amount of satoshis
	// they wish to send.  So since we expect the htlc to
	// have a different amount, we should not fail.
	/*if !l.cfg.DebugHTLC && invoice.Terms.Value > 0 &&
		fwdInfo.AmountToForward < invoice.Terms.Value {

		log.Errorf("Onion payload of incoming htlc(%x) "+
			"has incorrect value: expected %v, "+
			"got %v", pd.RHash, invoice.Terms.Value,
			fwdInfo.AmountToForward)

		failure := lnwire.FailIncorrectPaymentAmount{}
		l.sendHTLCError(
			pd.HtlcIndex, failure, obfuscator, pd.SourceRef,
		)

		needUpdate = true
		return
	}*/

	// We'll also ensure that our time-lock value has been
	// computed correctly.

	/*expectedHeight := heightNow + minCltvDelta
	switch {

	case !l.cfg.DebugHTLC && fwdInfo.OutgoingCTLV < expectedHeight:
		log.Errorf("Onion payload of incoming "+
			"htlc(%x) has incorrect time-lock: "+
			"expected %v, got %v",
			pd.RHash[:], expectedHeight,
			fwdInfo.OutgoingCTLV)

		failure := lnwire.NewFinalIncorrectCltvExpiry(
			fwdInfo.OutgoingCTLV,
		)
		l.sendHTLCError(
			pd.HtlcIndex, failure, obfuscator, pd.SourceRef,
		)

		needUpdate = true
		return

	case !l.cfg.DebugHTLC && pd.Timeout != fwdInfo.OutgoingCTLV:
		log.Errorf("HTLC(%x) has incorrect "+
			"time-lock: expected %v, got %v",
			pd.RHash[:], pd.Timeout,
			fwdInfo.OutgoingCTLV)

		failure := lnwire.NewFinalIncorrectCltvExpiry(
			fwdInfo.OutgoingCTLV,
		)
		l.sendHTLCError(
			pd.HtlcIndex, failure, obfuscator, pd.SourceRef,
		)

		needUpdate = true
		return
	}
	*/

	// TODO: Mark the invoice as accepted here

	// Execute sending resolution message in a go routine to prevent
	// deadlock. Eventually InvoiceSettler may need its own main loop to
	// receive events from the switch and rpcserver.
	//
	// Resolution is only possible when the preimage is known. Otherwise do
	// nothing yet and wait for InvoiceSettler.Settle to be called with the
	// preimage.

	return nil
}
