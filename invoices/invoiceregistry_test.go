package invoices

import (
	"crypto/sha256"
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/lnwire"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
)

var (
	testTimeout = 5 * time.Second

	preimage = [32]byte{
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
	}

	hash = sha256.Sum256(preimage[:])
)

// TestSettleInvoice tests settling of an invoice and related notifications.
func TestSettleInvoice(t *testing.T) {
	defer leaktest.Check(t)()

	cdb, cleanup, err := newDB()
	defer cleanup()

	// Instantiate and start the invoice registry.
	registry := NewRegistry(cdb, &chaincfg.MainNetParams)

	err = registry.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Stop()

	allSubscriptions := registry.SubscribeNotifications(0, 0)
	defer allSubscriptions.Cancel()

	// Subscribe to the not yet existing invoice.
	subscription := registry.SubscribeSingleInvoice(hash)
	defer subscription.Cancel()

	if subscription.hash != hash {
		t.Fatalf("expected subscription for provided hash")
	}

	// Add the invoice.
	invoice := &channeldb.Invoice{
		Terms: channeldb.ContractTerm{
			PaymentPreimage: preimage,
			Value:           lnwire.MilliSatoshi(100000),
		},
	}

	addIdx, err := registry.AddInvoice(invoice, hash)
	if err != nil {
		t.Fatal(err)
	}

	if addIdx != 1 {
		t.Fatalf(
			"expected addIndex to start with 1, but got %v", addIdx,
		)
	}

	// We expect the open state to be sent to the single invoice subscriber.
	select {
	case update := <-subscription.Updates:
		if update.Terms.State != channeldb.ContractOpen {
			t.Fatalf(
				"expected state ContractOpen, but got %v",
				update.Terms.State,
			)
		}
	case <-time.After(testTimeout):
		t.Fatal("no update received")
	}

	// We expect a new invoice notification to be sent out.
	select {
	case newInvoice := <-allSubscriptions.NewInvoices:
		if newInvoice.Terms.State != channeldb.ContractOpen {
			t.Fatalf(
				"expected state ContractOpen, but got %v",
				newInvoice.Terms.State,
			)
		}
	case <-time.After(testTimeout):
		t.Fatal("no update received")
	}

	// Settle invoice with a slightly higher amount.
	amtPaid := lnwire.MilliSatoshi(100500)
	err = registry.SettleInvoice(hash, amtPaid)
	if err != nil {
		t.Fatal(err)
	}

	// We expect the settled state to be sent to the single invoice
	// subscriber.
	select {
	case update := <-subscription.Updates:
		if update.Terms.State != channeldb.ContractSettled {
			t.Fatalf(
				"expected state ContractOpen, but got %v",
				update.Terms.State,
			)
		}
		if update.AmtPaid != amtPaid {
			t.Fatal("invoice AmtPaid incorrect")
		}
	case <-time.After(testTimeout):
		t.Fatal("no update received")
	}

	// We expect a settled notification to be sent out.
	select {
	case settledInvoice := <-allSubscriptions.SettledInvoices:
		if settledInvoice.Terms.State != channeldb.ContractSettled {
			t.Fatalf(
				"expected state ContractOpen, but got %v",
				settledInvoice.Terms.State,
			)
		}
	case <-time.After(testTimeout):
		t.Fatal("no update received")
	}

	// Try to settle again.
	err = registry.SettleInvoice(hash, amtPaid)
	if err != nil {
		t.Fatal("expected duplicate settle to succeed")
	}

	// Try to cancel.
	err = registry.CancelInvoice(hash)
	if err == nil {
		t.Fatal("expected cancelation of a settled invoice to fail")
	}
}

// TestCancelInvoice tests cancelation of an invoice and related notifications.
func TestCancelInvoice(t *testing.T) {
	defer leaktest.Check(t)()

	cdb, cleanup, err := newDB()
	defer cleanup()

	// Instantiate and start the invoice registry.
	registry := NewRegistry(cdb, &chaincfg.MainNetParams)

	err = registry.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Stop()

	allSubscriptions := registry.SubscribeNotifications(0, 0)
	defer allSubscriptions.Cancel()

	// Subscribe to the not yet existing invoice.
	subscription := registry.SubscribeSingleInvoice(hash)
	defer subscription.Cancel()

	if subscription.hash != hash {
		t.Fatalf("expected subscription for provided hash")
	}

	// Add the invoice.
	invoice := &channeldb.Invoice{
		Terms: channeldb.ContractTerm{
			PaymentPreimage: preimage,
			Value:           lnwire.MilliSatoshi(100000),
		},
	}

	_, err = registry.AddInvoice(invoice, hash)
	if err != nil {
		t.Fatal(err)
	}

	// We expect the open state to be sent to the single invoice subscriber.
	select {
	case update := <-subscription.Updates:
		if update.Terms.State != channeldb.ContractOpen {
			t.Fatalf(
				"expected state ContractOpen, but got %v",
				update.Terms.State,
			)
		}
	case <-time.After(testTimeout):
		t.Fatal("no update received")
	}

	// We expect a new invoice notification to be sent out.
	select {
	case newInvoice := <-allSubscriptions.NewInvoices:
		if newInvoice.Terms.State != channeldb.ContractOpen {
			t.Fatalf(
				"expected state ContractOpen, but got %v",
				newInvoice.Terms.State,
			)
		}
	case <-time.After(testTimeout):
		t.Fatal("no update received")
	}

	// Cancel invoice.
	err = registry.CancelInvoice(hash)
	if err != nil {
		t.Fatal(err)
	}

	// We expect the canceled state to be sent to the single invoice
	// subscriber.
	select {
	case update := <-subscription.Updates:
		if update.Terms.State != channeldb.ContractCanceled {
			t.Fatalf(
				"expected state ContractOpen, but got %v",
				update.Terms.State,
			)
		}
	case <-time.After(testTimeout):
		t.Fatal("no update received")
	}

	// We expect no cancel notification to be sent to all invoice
	// subscribers (backwards compatibility).

	// Try to cancel again.
	err = registry.CancelInvoice(hash)
	if err != nil {
		t.Fatal("expected cancelation of a canceled invoice to succeed")
	}
}

func newDB() (*channeldb.DB, func(), error) {
	// First, create a temporary directory to be used for the duration of
	// this test.
	tempDirName, err := ioutil.TempDir("", "channeldb")
	if err != nil {
		return nil, nil, err
	}

	// Next, create channeldb for the first time.
	cdb, err := channeldb.Open(tempDirName)
	if err != nil {
		return nil, nil, err
	}

	cleanUp := func() {
		cdb.Close()
		os.RemoveAll(tempDirName)
	}

	return cdb, cleanUp, nil
}
