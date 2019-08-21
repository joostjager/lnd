package channeldb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/btcsuite/btcd/chaincfg"
	bitcoinCfg "github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	bitcoinWire "github.com/btcsuite/btcd/wire"
	"github.com/coreos/bbolt"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/lightningnetwork/lnd/zpay32"
	litecoinCfg "github.com/ltcsuite/ltcd/chaincfg"
)

// migrateInvoices adds invoice htlcs and a separate cltv delta field to the
// invoices.
func migrateInvoices(tx *bbolt.Tx) error {
	log.Infof("Migrating invoices to new invoice format")

	invoiceB := tx.Bucket(invoiceBucket)
	if invoiceB == nil {
		return nil
	}

	// Iterate through the entire key space of the top-level invoice bucket.
	// If key with a non-nil value stores the next invoice ID which maps to
	// the corresponding invoice. Store those keys first, because it isn't
	// safe to modify the bucket inside a ForEach loop.
	var invoiceKeys [][]byte
	err := invoiceB.ForEach(func(k, v []byte) error {
		if v == nil {
			return nil
		}

		invoiceKeys = append(invoiceKeys, k)

		return nil
	})
	if err != nil {
		return err
	}

	nets := []*bitcoinCfg.Params{
		&bitcoinCfg.MainNetParams, &bitcoinCfg.SimNetParams,
		&bitcoinCfg.RegressionNetParams, &bitcoinCfg.TestNet3Params,
	}

	ltcNets := []*litecoinCfg.Params{
		&litecoinCfg.MainNetParams, &litecoinCfg.SimNetParams,
		&litecoinCfg.RegressionNetParams, &litecoinCfg.TestNet4Params,
	}
	for _, net := range ltcNets {
		var convertedNet bitcoinCfg.Params
		applyLitecoinParams(&convertedNet, net)
		nets = append(nets, &convertedNet)
	}

	// Iterate over all stored keys and migrate the invoices.
	for _, k := range invoiceKeys {
		v := invoiceB.Get(k)

		// Deserialize the invoice with the deserializing function that
		// was in use for this version of the database.
		invoiceReader := bytes.NewReader(v)
		invoice, err := deserializeInvoiceLegacy(invoiceReader)
		if err != nil {
			return err
		}

		// Try to decode the payment request for every possible net to
		// avoid passing a the active network to channeldb. This would
		// be a layering violation, while this migration is only running
		// once and will likely be removed in the future.
		var payReq *zpay32.Invoice
		for _, net := range nets {
			payReq, err = zpay32.Decode(
				string(invoice.PaymentRequest), net,
			)
			if err == nil {
				break
			}
		}
		if payReq == nil {
			return fmt.Errorf("cannot decode payreq")
		}
		invoice.FinalCltvDelta = int32(payReq.MinFinalCLTVExpiry())

		// Serialize the invoice in the new format and use it to replace
		// the old invoice in the database.
		var buf bytes.Buffer
		if err := serializeInvoice(&buf, &invoice); err != nil {
			return err
		}

		err = invoiceB.Put(k, buf.Bytes())
		if err != nil {
			return err
		}
	}

	log.Infof("Migration of invoices completed!")
	return nil
}

func deserializeInvoiceLegacy(r io.Reader) (Invoice, error) {
	var err error
	invoice := Invoice{}

	// TODO(roasbeef): use read full everywhere
	invoice.Memo, err = wire.ReadVarBytes(r, 0, MaxMemoSize, "")
	if err != nil {
		return invoice, err
	}
	invoice.Receipt, err = wire.ReadVarBytes(r, 0, MaxReceiptSize, "")
	if err != nil {
		return invoice, err
	}

	invoice.PaymentRequest, err = wire.ReadVarBytes(r, 0, MaxPaymentRequestSize, "")
	if err != nil {
		return invoice, err
	}

	birthBytes, err := wire.ReadVarBytes(r, 0, 300, "birth")
	if err != nil {
		return invoice, err
	}
	if err := invoice.CreationDate.UnmarshalBinary(birthBytes); err != nil {
		return invoice, err
	}

	settledBytes, err := wire.ReadVarBytes(r, 0, 300, "settled")
	if err != nil {
		return invoice, err
	}
	if err := invoice.SettleDate.UnmarshalBinary(settledBytes); err != nil {
		return invoice, err
	}

	if _, err := io.ReadFull(r, invoice.Terms.PaymentPreimage[:]); err != nil {
		return invoice, err
	}
	var scratch [8]byte
	if _, err := io.ReadFull(r, scratch[:]); err != nil {
		return invoice, err
	}
	invoice.Terms.Value = lnwire.MilliSatoshi(byteOrder.Uint64(scratch[:]))

	if err := binary.Read(r, byteOrder, &invoice.Terms.State); err != nil {
		return invoice, err
	}

	if err := binary.Read(r, byteOrder, &invoice.AddIndex); err != nil {
		return invoice, err
	}
	if err := binary.Read(r, byteOrder, &invoice.SettleIndex); err != nil {
		return invoice, err
	}
	if err := binary.Read(r, byteOrder, &invoice.AmtPaid); err != nil {
		return invoice, err
	}

	return invoice, nil
}

// applyLitecoinParams applies the relevant chain configuration parameters that
// differ for litecoin to the chain parameters typed for btcsuite derivation.
// This function is used in place of using something like interface{} to
// abstract over _which_ chain (or fork) the parameters are for.
//
// Note: this is a copy of a function with the same name in the lnd package. It
// couldn't be imported because a circular dependency would occur. Moving the
// function into the lncfg package doesn't work either, because it also creates
// a different circular dependency. As this migration code is likely to be
// removed at some point anyway, the function is duplicated.
func applyLitecoinParams(params *bitcoinCfg.Params, litecoinParams *litecoinCfg.Params) {
	params.Name = litecoinParams.Name
	params.Net = bitcoinWire.BitcoinNet(litecoinParams.Net)
	params.DefaultPort = litecoinParams.DefaultPort
	params.CoinbaseMaturity = litecoinParams.CoinbaseMaturity

	params.GenesisHash = &chainhash.Hash{}
	copy(params.GenesisHash[:], litecoinParams.GenesisHash[:])

	// Address encoding magics
	params.PubKeyHashAddrID = litecoinParams.PubKeyHashAddrID
	params.ScriptHashAddrID = litecoinParams.ScriptHashAddrID
	params.PrivateKeyID = litecoinParams.PrivateKeyID
	params.WitnessPubKeyHashAddrID = litecoinParams.WitnessPubKeyHashAddrID
	params.WitnessScriptHashAddrID = litecoinParams.WitnessScriptHashAddrID
	params.Bech32HRPSegwit = litecoinParams.Bech32HRPSegwit

	copy(params.HDPrivateKeyID[:], litecoinParams.HDPrivateKeyID[:])
	copy(params.HDPublicKeyID[:], litecoinParams.HDPublicKeyID[:])

	params.HDCoinType = litecoinParams.HDCoinType

	checkPoints := make([]chaincfg.Checkpoint, len(litecoinParams.Checkpoints))
	for i := 0; i < len(litecoinParams.Checkpoints); i++ {
		var chainHash chainhash.Hash
		copy(chainHash[:], litecoinParams.Checkpoints[i].Hash[:])

		checkPoints[i] = chaincfg.Checkpoint{
			Height: litecoinParams.Checkpoints[i].Height,
			Hash:   &chainHash,
		}
	}
	params.Checkpoints = checkPoints
}
