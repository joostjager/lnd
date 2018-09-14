// +build !rpctest

package main

import (
	"bytes"
	"fmt"
	"github.com/lightningnetwork/lnd/channeldb"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcec"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btclog"
	"github.com/btcsuite/btcutil"
	"github.com/lightningnetwork/lnd/chainntnfs"
	"github.com/lightningnetwork/lnd/lnwallet"
)

var (
	outPoints = []wire.OutPoint{
		{
			Hash: [chainhash.HashSize]byte{
				0x51, 0xb6, 0x37, 0xd8, 0xfc, 0xd2, 0xc6, 0xda,
				0x48, 0x59, 0xe6, 0x96, 0x31, 0x13, 0xa1, 0x17,
				0x2d, 0xe7, 0x93, 0xe4, 0xb7, 0x25, 0xb8, 0x4d,
				0x1f, 0xb, 0x4c, 0xf9, 0x9e, 0xc5, 0x8c, 0xe9,
			},
			Index: 9,
		},
		{
			Hash: [chainhash.HashSize]byte{
				0xb7, 0x94, 0x38, 0x5f, 0x2d, 0x1e, 0xf7, 0xab,
				0x4d, 0x92, 0x73, 0xd1, 0x90, 0x63, 0x81, 0xb4,
				0x4f, 0x2f, 0x6f, 0x25, 0x88, 0xa3, 0xef, 0xb9,
				0x6a, 0x49, 0x18, 0x83, 0x31, 0x98, 0x47, 0x53,
			},
			Index: 49,
		},
		{
			Hash: [chainhash.HashSize]byte{
				0x81, 0xb6, 0x37, 0xd8, 0xfc, 0xd2, 0xc6, 0xda,
				0x63, 0x59, 0xe6, 0x96, 0x31, 0x13, 0xa1, 0x17,
				0x0d, 0xe7, 0x95, 0xe4, 0xb7, 0x25, 0xb8, 0x4d,
				0x1e, 0xb, 0x4c, 0xfd, 0x9e, 0xc5, 0x8c, 0xe9,
			},
			Index: 23,
		},
		{
			Hash: [chainhash.HashSize]byte{
				0x1e, 0xb, 0x4c, 0xfd, 0x9e, 0xc5, 0x8c, 0xe9,
				0x81, 0xb6, 0x37, 0xd8, 0xfc, 0xd2, 0xc6, 0xda,
				0x0d, 0xe7, 0x95, 0xe4, 0xb7, 0x25, 0xb8, 0x4d,
				0x63, 0x59, 0xe6, 0x96, 0x31, 0x13, 0xa1, 0x17,
			},
			Index: 30,
		},
		{
			Hash: [chainhash.HashSize]byte{
				0x0d, 0xe7, 0x95, 0xe4, 0xfc, 0xd2, 0xc6, 0xda,
				0xb7, 0x25, 0xb8, 0x4d, 0x63, 0x59, 0xe6, 0x96,
				0x31, 0x13, 0xa1, 0x17, 0x81, 0xb6, 0x37, 0xd8,
				0x1e, 0x0b, 0x4c, 0xfd, 0x9e, 0xc5, 0x8c, 0xe9,
			},
			Index: 2,
		},
		{
			Hash: [chainhash.HashSize]byte{
				0x48, 0x59, 0xe6, 0x96, 0x31, 0x13, 0xa1, 0x17,
				0x51, 0xb6, 0x37, 0xd8, 0x1f, 0x0b, 0x4c, 0xf9,
				0x9e, 0xc5, 0x8c, 0xe9, 0xfc, 0xd2, 0xc6, 0xda,
				0x2d, 0xe7, 0x93, 0xe4, 0xb7, 0x25, 0xb8, 0x4d,
			},
			Index: 9,
		},
	}

	keys = [][]byte{
		{0x04, 0x11, 0xdb, 0x93, 0xe1, 0xdc, 0xdb, 0x8a,
			0x01, 0x6b, 0x49, 0x84, 0x0f, 0x8c, 0x53, 0xbc, 0x1e,
			0xb6, 0x8a, 0x38, 0x2e, 0x97, 0xb1, 0x48, 0x2e, 0xca,
			0xd7, 0xb1, 0x48, 0xa6, 0x90, 0x9a, 0x5c, 0xb2, 0xe0,
			0xea, 0xdd, 0xfb, 0x84, 0xcc, 0xf9, 0x74, 0x44, 0x64,
			0xf8, 0x2e, 0x16, 0x0b, 0xfa, 0x9b, 0x8b, 0x64, 0xf9,
			0xd4, 0xc0, 0x3f, 0x99, 0x9b, 0x86, 0x43, 0xf6, 0x56,
			0xb4, 0x12, 0xa3,
		},
		{0x07, 0x11, 0xdb, 0x93, 0xe1, 0xdc, 0xdb, 0x8a,
			0x01, 0x6b, 0x49, 0x84, 0x0f, 0x8c, 0x53, 0xbc, 0x1e,
			0xb6, 0x8a, 0x38, 0x2e, 0x97, 0xb1, 0x48, 0x2e, 0xca,
			0xd7, 0xb1, 0x48, 0xa6, 0x90, 0x9a, 0x5c, 0xb2, 0xe0,
			0xea, 0xdd, 0xfb, 0x84, 0xcc, 0xf9, 0x74, 0x44, 0x64,
			0xf8, 0x2e, 0x16, 0x0b, 0xfa, 0x9b, 0x8b, 0x64, 0xf9,
			0xd4, 0xc0, 0x3f, 0x99, 0x9b, 0x86, 0x43, 0xf6, 0x56,
			0xb4, 0x12, 0xa3,
		},
		{0x02, 0xce, 0x0b, 0x14, 0xfb, 0x84, 0x2b, 0x1b,
			0xa5, 0x49, 0xfd, 0xd6, 0x75, 0xc9, 0x80, 0x75, 0xf1,
			0x2e, 0x9c, 0x51, 0x0f, 0x8e, 0xf5, 0x2b, 0xd0, 0x21,
			0xa9, 0xa1, 0xf4, 0x80, 0x9d, 0x3b, 0x4d,
		},
	}

	signDescriptors = []lnwallet.SignDescriptor{
		{
			SingleTweak: []byte{
				0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02,
				0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02,
				0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02,
				0x02, 0x02, 0x02, 0x02, 0x02,
			},
			WitnessScript: []byte{
				0x00, 0x14, 0xee, 0x91, 0x41, 0x7e, 0x85, 0x6c, 0xde,
				0x10, 0xa2, 0x91, 0x1e, 0xdc, 0xbd, 0xbd, 0x69, 0xe2,
				0xef, 0xb5, 0x71, 0x48,
			},
			Output: &wire.TxOut{
				Value: 5000000000,
				PkScript: []byte{
					0x41, // OP_DATA_65
					0x04, 0xd6, 0x4b, 0xdf, 0xd0, 0x9e, 0xb1, 0xc5,
					0xfe, 0x29, 0x5a, 0xbd, 0xeb, 0x1d, 0xca, 0x42,
					0x81, 0xbe, 0x98, 0x8e, 0x2d, 0xa0, 0xb6, 0xc1,
					0xc6, 0xa5, 0x9d, 0xc2, 0x26, 0xc2, 0x86, 0x24,
					0xe1, 0x81, 0x75, 0xe8, 0x51, 0xc9, 0x6b, 0x97,
					0x3d, 0x81, 0xb0, 0x1c, 0xc3, 0x1f, 0x04, 0x78,
					0x34, 0xbc, 0x06, 0xd6, 0xd6, 0xed, 0xf6, 0x20,
					0xd1, 0x84, 0x24, 0x1a, 0x6a, 0xed, 0x8b, 0x63,
					0xa6, // 65-byte signature
					0xac, // OP_CHECKSIG
				},
			},
			HashType: txscript.SigHashAll,
		},
		{
			SingleTweak: []byte{
				0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02,
				0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02,
				0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02,
				0x02, 0x02, 0x02, 0x02, 0x02,
			},
			WitnessScript: []byte{
				0x00, 0x14, 0xee, 0x91, 0x41, 0x7e, 0x85, 0x6c, 0xde,
				0x10, 0xa2, 0x91, 0x1e, 0xdc, 0xbd, 0xbd, 0x69, 0xe2,
				0xef, 0xb5, 0x71, 0x48,
			},
			Output: &wire.TxOut{
				Value: 5000000000,
				PkScript: []byte{
					0x41, // OP_DATA_65
					0x04, 0xd6, 0x4b, 0xdf, 0xd0, 0x9e, 0xb1, 0xc5,
					0xfe, 0x29, 0x5a, 0xbd, 0xeb, 0x1d, 0xca, 0x42,
					0x81, 0xbe, 0x98, 0x8e, 0x2d, 0xa0, 0xb6, 0xc1,
					0xc6, 0xa5, 0x9d, 0xc2, 0x26, 0xc2, 0x86, 0x24,
					0xe1, 0x81, 0x75, 0xe8, 0x51, 0xc9, 0x6b, 0x97,
					0x3d, 0x81, 0xb0, 0x1c, 0xc3, 0x1f, 0x04, 0x78,
					0x34, 0xbc, 0x06, 0xd6, 0xd6, 0xed, 0xf6, 0x20,
					0xd1, 0x84, 0x24, 0x1a, 0x6a, 0xed, 0x8b, 0x63,
					0xa6, // 65-byte signature
					0xac, // OP_CHECKSIG
				},
			},
			HashType: txscript.SigHashAll,
		},
		{
			SingleTweak: []byte{
				0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02,
				0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02,
				0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02,
				0x02, 0x02, 0x02, 0x02, 0x02,
			},
			WitnessScript: []byte{
				0x00, 0x14, 0xee, 0x91, 0x41, 0x7e, 0x85, 0x6c, 0xde,
				0x10, 0xa2, 0x91, 0x1e, 0xdc, 0xbd, 0xbd, 0x69, 0xe2,
				0xef, 0xb5, 0x71, 0x48,
			},
			Output: &wire.TxOut{
				Value: 5000000000,
				PkScript: []byte{
					0x41, // OP_DATA_65
					0x04, 0xd6, 0x4b, 0xdf, 0xd0, 0x9e, 0xb1, 0xc5,
					0xfe, 0x29, 0x5a, 0xbd, 0xeb, 0x1d, 0xca, 0x42,
					0x81, 0xbe, 0x98, 0x8e, 0x2d, 0xa0, 0xb6, 0xc1,
					0xc6, 0xa5, 0x9d, 0xc2, 0x26, 0xc2, 0x86, 0x24,
					0xe1, 0x81, 0x75, 0xe8, 0x51, 0xc9, 0x6b, 0x97,
					0x3d, 0x81, 0xb0, 0x1c, 0xc3, 0x1f, 0x04, 0x78,
					0x34, 0xbc, 0x06, 0xd6, 0xd6, 0xed, 0xf6, 0x20,
					0xd1, 0x84, 0x24, 0x1a, 0x6a, 0xed, 0x8b, 0x63,
					0xa6, // 65-byte signature
					0xac, // OP_CHECKSIG
				},
			},
			HashType: txscript.SigHashAll,
		},
	}

	kidOutputs = []kidOutput{
		{
			breachedOutput: breachedOutput{
				amt:         btcutil.Amount(13e7),
				outpoint:    outPoints[1],
				witnessType: lnwallet.CommitmentTimeLock,
			},
			originChanPoint:  outPoints[0],
			blocksToMaturity: uint32(42),
			confHeight:       uint32(1000),
		},

		{
			breachedOutput: breachedOutput{
				amt:         btcutil.Amount(24e7),
				outpoint:    outPoints[2],
				witnessType: lnwallet.CommitmentTimeLock,
			},
			originChanPoint:  outPoints[0],
			blocksToMaturity: uint32(42),
			confHeight:       uint32(1000),
		},

		{
			breachedOutput: breachedOutput{
				amt:         btcutil.Amount(2e5),
				outpoint:    outPoints[3],
				witnessType: lnwallet.CommitmentTimeLock,
			},
			originChanPoint:  outPoints[0],
			blocksToMaturity: uint32(28),
			confHeight:       uint32(500),
		},

		{
			breachedOutput: breachedOutput{
				amt:         btcutil.Amount(10e6),
				outpoint:    outPoints[4],
				witnessType: lnwallet.CommitmentTimeLock,
			},
			originChanPoint:  outPoints[0],
			blocksToMaturity: uint32(28),
			confHeight:       uint32(500),
		},
	}

	babyOutputs = []babyOutput{
		{
			kidOutput: kidOutputs[1],
			expiry:    3829,
			timeoutTx: timeoutTx,
		},
		{
			kidOutput: kidOutputs[2],
			expiry:    4,
			timeoutTx: timeoutTx,
		},
		{
			kidOutput: kidOutputs[3],
			expiry:    4,
			timeoutTx: timeoutTx,
		},
	}

	// Dummy timeout tx used to test serialization, borrowed from btcd
	// msgtx_test
	timeoutTx = &wire.MsgTx{
		Version: 1,
		TxIn: []*wire.TxIn{
			{
				PreviousOutPoint: wire.OutPoint{
					Hash: chainhash.Hash{
						0xa5, 0x33, 0x52, 0xd5, 0x13, 0x57, 0x66, 0xf0,
						0x30, 0x76, 0x59, 0x74, 0x18, 0x26, 0x3d, 0xa2,
						0xd9, 0xc9, 0x58, 0x31, 0x59, 0x68, 0xfe, 0xa8,
						0x23, 0x52, 0x94, 0x67, 0x48, 0x1f, 0xf9, 0xcd,
					},
					Index: 19,
				},
				SignatureScript: []byte{},
				Witness: [][]byte{
					{ // 70-byte signature
						0x30, 0x43, 0x02, 0x1f, 0x4d, 0x23, 0x81, 0xdc,
						0x97, 0xf1, 0x82, 0xab, 0xd8, 0x18, 0x5f, 0x51,
						0x75, 0x30, 0x18, 0x52, 0x32, 0x12, 0xf5, 0xdd,
						0xc0, 0x7c, 0xc4, 0xe6, 0x3a, 0x8d, 0xc0, 0x36,
						0x58, 0xda, 0x19, 0x02, 0x20, 0x60, 0x8b, 0x5c,
						0x4d, 0x92, 0xb8, 0x6b, 0x6d, 0xe7, 0xd7, 0x8e,
						0xf2, 0x3a, 0x2f, 0xa7, 0x35, 0xbc, 0xb5, 0x9b,
						0x91, 0x4a, 0x48, 0xb0, 0xe1, 0x87, 0xc5, 0xe7,
						0x56, 0x9a, 0x18, 0x19, 0x70, 0x01,
					},
					{ // 33-byte serialize pub key
						0x03, 0x07, 0xea, 0xd0, 0x84, 0x80, 0x7e, 0xb7,
						0x63, 0x46, 0xdf, 0x69, 0x77, 0x00, 0x0c, 0x89,
						0x39, 0x2f, 0x45, 0xc7, 0x64, 0x25, 0xb2, 0x61,
						0x81, 0xf5, 0x21, 0xd7, 0xf3, 0x70, 0x06, 0x6a,
						0x8f,
					},
				},
				Sequence: 0xffffffff,
			},
		},
		TxOut: []*wire.TxOut{
			{
				Value: 395019,
				PkScript: []byte{ // p2wkh output
					0x00, // Version 0 witness program
					0x14, // OP_DATA_20
					0x9d, 0xda, 0xc6, 0xf3, 0x9d, 0x51, 0xe0, 0x39,
					0x8e, 0x53, 0x2a, 0x22, 0xc4, 0x1b, 0xa1, 0x89,
					0x40, 0x6a, 0x85, 0x23, // 20-byte pub key hash
				},
			},
		},
	}
)

func init() {
	// Finish initializing our test vectors by parsing the desired public keys and
	// properly populating the sign descriptors of all baby and kid outputs.
	for i := range signDescriptors {
		pk, err := btcec.ParsePubKey(keys[i], btcec.S256())
		if err != nil {
			panic(fmt.Sprintf("unable to parse pub key during init: %v", err))
		}
		signDescriptors[i].KeyDesc.PubKey = pk

	}
	for i := range kidOutputs {
		isd := i % len(signDescriptors)
		kidOutputs[i].signDesc = signDescriptors[isd]
	}

	for i := range babyOutputs {
		isd := i % len(signDescriptors)
		babyOutputs[i].kidOutput.signDesc = signDescriptors[isd]
	}

	initIncubateTests()
}

func TestKidOutputSerialization(t *testing.T) {
	for i, kid := range kidOutputs {
		var b bytes.Buffer
		if err := kid.Encode(&b); err != nil {
			t.Fatalf("Encode #%d: unable to serialize "+
				"kid output: %v", i, err)
		}

		var deserializedKid kidOutput
		if err := deserializedKid.Decode(&b); err != nil {
			t.Fatalf("Decode #%d: unable to deserialize "+
				"kid output: %v", i, err)
		}

		if !reflect.DeepEqual(kid, deserializedKid) {
			t.Fatalf("DeepEqual #%d: unexpected kidOutput, "+
				"want %+v, got %+v",
				i, kid, deserializedKid)
		}
	}
}

func TestBabyOutputSerialization(t *testing.T) {
	for i, baby := range babyOutputs {
		var b bytes.Buffer
		if err := baby.Encode(&b); err != nil {
			t.Fatalf("Encode #%d: unable to serialize "+
				"baby output: %v", i, err)
		}

		var deserializedBaby babyOutput
		if err := deserializedBaby.Decode(&b); err != nil {
			t.Fatalf("Decode #%d: unable to deserialize "+
				"baby output: %v", i, err)
		}

		if !reflect.DeepEqual(baby, deserializedBaby) {
			t.Fatalf("DeepEqual #%d: unexpected babyOutput, "+
				"want %+v, got %+v",
				i, baby, deserializedBaby)
		}

	}
}

var testChanPoint = wire.OutPoint{}

type testWriter struct {
}

func (w testWriter) Write(p []byte) (n int, err error) {
	os.Stdout.Write(p)

	return len(p), nil
}

func init() {
	backendLog := btclog.NewBackend(testWriter{})
	utxnLog = backendLog.Logger("TEST")
	utxnLog.SetLevel(btclog.LevelTrace)
}

type nurseryTestContext struct {
	nursery     *utxoNursery
	notifier    *nurseryMockNotifier
	publishChan chan wire.MsgTx
	store       *nurseryStoreInterceptor
	restart     func() bool
	receiveTx   func() wire.MsgTx
	t           *testing.T
}

func createNurseryTestContext(t *testing.T,
	checkStartStop func(func()) bool) *nurseryTestContext {

	// Create a temporary database and connect nurseryStore to it. The
	// alternative, mocking nurseryStore, is not chosen because there is
	// still considerable logic in the store.

	tempDirName, err := ioutil.TempDir("", "channeldb")
	if err != nil {
		t.Fatalf("unable to create temp dir: %v", err)
	}

	cdb, err := channeldb.Open(tempDirName)
	if err != nil {
		t.Fatalf("unable to open channeldb: %v", err)
	}

	store, err := newNurseryStore(&chainhash.Hash{}, cdb)
	if err != nil {
		t.Fatal(err)
	}

	// Wrap the store in an inceptor to be able to wait for events in this
	// test.
	storeIntercepter := newNurseryStoreInterceptor(store)

	notifier := newNurseryMockNotifier()

	cfg := NurseryConfig{
		Notifier: notifier,
		DB:       cdb,
		Store:    storeIntercepter,
		ChainIO:  &mockChainIO{},
		GenSweepScript: func() ([]byte, error) {
			return []byte{}, nil
		},
		Estimator: &mockFeeEstimator{},
		Signer:    &nurseryMockSigner{},
	}

	publishChan := make(chan wire.MsgTx, 1)
	cfg.PublishTransaction = func(tx *wire.MsgTx) error {
		utxnLog.Tracef("Publishing tx %v", tx.TxHash())
		publishChan <- *tx
		return nil
	}

	nursery := newUtxoNursery(&cfg)
	nursery.Start()

	ctx := &nurseryTestContext{
		nursery:     nursery,
		notifier:    notifier,
		store:       storeIntercepter,
		publishChan: publishChan,
		t:           t,
	}

	ctx.restart = func() bool {
		return checkStartStop(func() {
			ctx.nursery.Stop()
			// Simulate lnd restart.
			ctx.nursery = newUtxoNursery(ctx.nursery.cfg)
			ctx.nursery.Start()
		})
	}

	ctx.receiveTx = func() wire.MsgTx {
		var tx wire.MsgTx
		select {
		case tx = <-ctx.publishChan:
			return tx
		case <-time.After(5 * time.Second):
			t.Fatalf("tx not published")
		}
		return tx
	}

	// Start with testing an immediate restart.
	ctx.restart()

	return ctx
}

func (ctx *nurseryTestContext) finish() {
	// Add a final restart point in this state
	ctx.restart()

	// We assume that when finish is called, nursery has finished all its
	// goroutines. This implies that the waitgroup is empty.
	signalChan := make(chan struct{})
	go func() {
		ctx.nursery.wg.Wait()
		close(signalChan)
	}()

	// The only goroutine that is still expected to be running is
	// incubator(). Simulate exit of this goroutine.
	ctx.nursery.wg.Done()

	// We now expect the Wait to succeed.
	select {
	case <-signalChan:
	case <-time.After(time.Second):
		ctx.t.Fatalf("lingering goroutines detected after test is finished")
	}

	// Restore waitgroup state to what it was before.
	ctx.nursery.wg.Add(1)

	ctx.nursery.Stop()

	// We should have consumed and asserted all published transactions in
	// our unit tests.
	select {
	case <-ctx.publishChan:
		ctx.t.Fatalf("unexpected transactions published")
	default:
	}
}

func createOutgoingRes(onLocalCommitment bool) *lnwallet.OutgoingHtlcResolution {
	// Set up an outgoing htlc resolution to hand off to nursery.
	closeTx := &wire.MsgTx{}

	htlcOp := wire.OutPoint{
		Hash:  closeTx.TxHash(),
		Index: 0,
	}

	outgoingRes := lnwallet.OutgoingHtlcResolution{
		Expiry: 125,
		SweepSignDesc: lnwallet.SignDescriptor{
			Output: &wire.TxOut{
				Value: 10000,
			},
		},
		CsvDelay: 2,
	}

	if onLocalCommitment {
		timeoutTx := &wire.MsgTx{
			TxIn: []*wire.TxIn{
				{
					PreviousOutPoint: htlcOp,
					Witness:          [][]byte{{}},
				},
			},
			TxOut: []*wire.TxOut{
				{},
			},
		}

		outgoingRes.SignedTimeoutTx = timeoutTx
	} else {
		outgoingRes.ClaimOutpoint = htlcOp
	}

	return &outgoingRes
}

func incubateTestOutput(t *testing.T, nursery *utxoNursery,
	onLocalCommitment bool) *lnwallet.OutgoingHtlcResolution {

	outgoingRes := createOutgoingRes(onLocalCommitment)

	// Hand off to nursery.
	err := nursery.IncubateOutputs(
		testChanPoint,
		nil,
		[]lnwallet.OutgoingHtlcResolution{*outgoingRes},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	// IncubateOutputs is executing synchronously and we expect the output
	// to immediately show up in the report.
	expectedStage := uint32(2)
	if onLocalCommitment {
		expectedStage = 1
	}
	assertNurseryReport(t, nursery, 1, expectedStage)

	return outgoingRes
}

func assertNurseryReport(t *testing.T, nursery *utxoNursery,
	expectedNofHtlcs int, expectedStage uint32) {
	report, err := nursery.NurseryReport(&testChanPoint)
	if err != nil {
		t.Fatal(err)
	}

	if len(report.htlcs) != expectedNofHtlcs {
		t.Fatalf("expected %v outputs to be reported, but report "+
			"contains %v", expectedNofHtlcs, len(report.htlcs))
	}
	htlcReport := report.htlcs[0]
	if htlcReport.stage != expectedStage {
		t.Fatalf("expected htlc be advanced to stage %v, but it is "+
			"reported in stage %v", expectedStage, htlcReport.stage)
	}
}

func assertNurseryReportUnavailable(t *testing.T, nursery *utxoNursery) {
	_, err := nursery.NurseryReport(&testChanPoint)
	if err != ErrContractNotFound {
		t.Fatal("expected report to be unavailable")
	}
}

// testRestartLoop runs the specified test multiple times and in every run it
// will attempt to execute a restart action in a different location. This is to
// assert that the unit under test is recovering correctly from restarts.
func testRestartLoop(t *testing.T, test func(*testing.T,
	func(func()) bool)) {

	// Start with running the test without any restarts (index zero)
	restartIdx := 0

	for {
		currentStartStopIdx := 0

		// checkStartStop is called at every point in the test where a
		// restart should be exercised. When this function is called as
		// many times as the current value of currentStartStopIdx, it
		// will execute startStopFunc.
		checkStartStop := func(startStopFunc func()) bool {
			currentStartStopIdx++
			if restartIdx == currentStartStopIdx {
				startStopFunc()

				return true
			}
			return false
		}

		t.Run(fmt.Sprintf("restart_%v", restartIdx),
			func(t *testing.T) {
				test(t, checkStartStop)
			})

		// Exit the loop when all restart points have been tested.
		if currentStartStopIdx == restartIdx {
			return
		}
		restartIdx++
	}
}

func TestNurserySuccessLocal(t *testing.T) {
	testRestartLoop(t, testNurserySuccessLocal)
}

func testNurserySuccessLocal(t *testing.T,
	checkStartStop func(func()) bool) {

	ctx := createNurseryTestContext(t, checkStartStop)

	outgoingRes := incubateTestOutput(t, ctx.nursery, true)

	ctx.restart()

	// Notify arrival of block where HTLC CLTV expires.
	ctx.notifier.epochChan <- &chainntnfs.BlockEpoch{
		Height: 125,
	}

	// This should trigger nursery to publish the timeout tx.
	ctx.receiveTx()

	if ctx.restart() {
		// Restart should retrigger broadcast of timeout tx.
		ctx.receiveTx()
	}

	// Spend the previous outpoint of the timeout transaction.
	// Nursery is listening for it.
	timeoutTxHash := outgoingRes.SignedTimeoutTx.TxHash()
	ctx.notifier.spendOutpoint(
		&outgoingRes.SignedTimeoutTx.TxIn[0].PreviousOutPoint,
		outgoingRes.SignedTimeoutTx)

	// Confirm the timeout tx. This should promote the HTLC to KNDR state.
	ctx.notifier.confirmTx(&timeoutTxHash, 126)

	// Wait for output to be promoted in store to KNDR.
	select {
	case <-ctx.store.cribToKinderChan:
	case <-time.After(5 * time.Second):
		t.Fatalf("output not promoted to KNDR")
	}

	ctx.restart()

	// Notify arrival of block where second level HTLC unlocks.
	ctx.notifier.epochChan <- &chainntnfs.BlockEpoch{
		Height: 128,
	}

	// Check final sweep into wallet.
	testSweep(t, ctx)
}

func TestNurserySuccessRemote(t *testing.T) {
	testRestartLoop(t, testNurserySuccessRemote)
}

func testNurserySuccessRemote(t *testing.T,
	checkStartStop func(func()) bool) {

	ctx := createNurseryTestContext(t, checkStartStop)

	outgoingRes := incubateTestOutput(t, ctx.nursery, false)

	// TODO(joostjager): for this restart to work, channel db needs to be
	// mocked. Waiting for merge of #1847 to completely remove reading
	// closed channel summary.

	// ctx.restart()

	// Notify confirmation of the commitment tx. Is only listened to when
	// resolving remote commitment tx.
	//
	// TODO(joostjager): This is probably not correct?
	ctx.notifier.confirmTx(&outgoingRes.ClaimOutpoint.Hash, 124)

	// Wait for output to be promoted from PSCL to KNDR.
	select {
	case <-ctx.store.preschoolToKinderChan:
	case <-time.After(5 * time.Second):
		t.Fatalf("output not promoted to KNDR")
	}

	ctx.restart()

	// Notify arrival of block where HTLC CLTV expires.
	ctx.notifier.epochChan <- &chainntnfs.BlockEpoch{
		Height: 125,
	}

	// Check final sweep into wallet.
	testSweep(t, ctx)
}

func testSweep(t *testing.T, ctx *nurseryTestContext) {
	// Wait for nursery to publish the sweep tx.
	sweepTx := ctx.receiveTx()

	if ctx.restart() {
		// Restart will trigger rebroadcast of sweep tx.
		sweepTx = ctx.receiveTx()
	}

	// Verify stage in nursery report. HTLCs should now both still be in
	// stage two.
	assertNurseryReport(t, ctx.nursery, 1, 2)

	sweepTxHash := sweepTx.TxHash()

	ctx.notifier.spendOutpoint(
		&sweepTx.TxIn[0].PreviousOutPoint,
		&sweepTx)

	// Confirm the sweep tx.
	ctx.notifier.confirmTx(&sweepTxHash, 129)

	// Wait for output to be promoted in store to GRAD.
	select {
	case <-ctx.store.graduateKinderChan:
	case <-time.After(5 * time.Second):
		t.Fatalf("output not graduated")
	}

	ctx.restart()

	// As there only was one output to graduate, we expect the channel to be
	// closed and no report available anymore.
	assertNurseryReportUnavailable(t, ctx.nursery)

	ctx.finish()
}

func TestNurseryRemoteSpendOnLocal(t *testing.T) {
	testRestartLoop(t, testNurseryRemoteSpendOnLocal)
}

func testNurseryRemoteSpendOnLocal(t *testing.T,
	checkStartStop func(func()) bool) {

	ctx := createNurseryTestContext(t, checkStartStop)

	outgoingRes := incubateTestOutput(t, ctx.nursery, true)

	ctx.restart()

	// Have the remote party spend the commit tx outpoint.
	remoteSpendTxPreviousOutpoint :=
		outgoingRes.SignedTimeoutTx.TxIn[0].PreviousOutPoint

	remoteSpendTx := wire.MsgTx{
		TxIn: []*wire.TxIn{
			{
				PreviousOutPoint: remoteSpendTxPreviousOutpoint,
			},
		},
	}
	ctx.notifier.spendOutpoint(
		&remoteSpendTxPreviousOutpoint,
		&remoteSpendTx)

	// Wait for output to be marked as SPND.
	select {
	case <-ctx.store.cribToRemoteSpendChan:
	case <-time.After(5 * time.Second):
		t.Fatalf("not marked as SPND")
	}

	// Notify arrival of block where HTLC expires.
	ctx.notifier.epochChan <- &chainntnfs.BlockEpoch{
		Height: 125,
	}

	// This should not trigger the nursery to publish the timeout tx.
	select {
	case <-ctx.publishChan:
		t.Fatalf("tx should not be published anymore")
	default:
	}

	// Verify stage in nursery report.
	assertNurseryReport(t, ctx.nursery, 1, 0)

	ctx.nursery.Stop()
}

func TestNurseryRemoteSpendOnRemote(t *testing.T) {
	testRestartLoop(t, testNurseryRemoteSpendOnRemote)
}

func testNurseryRemoteSpendOnRemote(t *testing.T,
	checkStartStop func(func()) bool) {

	ctx := createNurseryTestContext(t, checkStartStop)

	outgoingRes := incubateTestOutput(t, ctx.nursery, false)

	ctx.notifier.confirmTx(&outgoingRes.ClaimOutpoint.Hash, 124)

	// Wait for output to be promoted from PSCL to KNDR.
	select {
	case <-ctx.store.preschoolToKinderChan:
	case <-time.After(5 * time.Second):
		t.Fatalf("output not promoted to KNDR")
	}

	ctx.restart()

	// Remote spend the htlc using a remote success tx. Set witness script
	// length to 5 so nursery recognizes it properly.
	remoteSpendTxPreviousOutpoint := outgoingRes.ClaimOutpoint

	remoteSpendTx := wire.MsgTx{
		TxIn: []*wire.TxIn{
			{
				PreviousOutPoint: remoteSpendTxPreviousOutpoint,
				Witness:          [][]byte{{}, {}, {}, {}, {}},
			},
		},
	}
	ctx.notifier.spendOutpoint(
		&remoteSpendTxPreviousOutpoint,
		&remoteSpendTx)

	// Wait for output to be marked as SPND.
	select {
	case <-ctx.store.kidToRemoteSpendChan:
	case <-time.After(5 * time.Second):
		t.Fatalf("not marked as SPND")
	}

	// Notify arrival of block where HTLC expires.
	ctx.notifier.epochChan <- &chainntnfs.BlockEpoch{
		Height: 125,
	}

	// This should not trigger the nursery to publish the timeout tx.
	select {
	case <-ctx.publishChan:
		t.Fatalf("tx should not be published anymore")
	default:
	}

	// Verify stage in nursery report.
	assertNurseryReport(t, ctx.nursery, 1, 0)

	ctx.nursery.Stop()
}

type nurseryStoreInterceptor struct {
	ns NurseryStore

	// TODO(joostjager): put more useful info through these channels.
	cribToKinderChan      chan struct{}
	cribToRemoteSpendChan chan struct{}
	graduateKinderChan    chan struct{}
	preschoolToKinderChan chan struct{}
	kidToRemoteSpendChan  chan struct{}
}

func newNurseryStoreInterceptor(ns NurseryStore) *nurseryStoreInterceptor {
	return &nurseryStoreInterceptor{
		ns:                    ns,
		cribToKinderChan:      make(chan struct{}),
		cribToRemoteSpendChan: make(chan struct{}),
		graduateKinderChan:    make(chan struct{}),
		preschoolToKinderChan: make(chan struct{}),
		kidToRemoteSpendChan:  make(chan struct{}),
	}
}

func (i *nurseryStoreInterceptor) Incubate(kidOutputs []kidOutput,
	babyOutputs []babyOutput) error {

	return i.ns.Incubate(kidOutputs, babyOutputs)
}

func (i *nurseryStoreInterceptor) CribToKinder(babyOutput *babyOutput) error {
	err := i.ns.CribToKinder(babyOutput)

	i.cribToKinderChan <- struct{}{}

	return err
}

func (i *nurseryStoreInterceptor) PreschoolToKinder(kidOutput *kidOutput) error {
	err := i.ns.PreschoolToKinder(kidOutput)

	i.preschoolToKinderChan <- struct{}{}

	return err
}

func (i *nurseryStoreInterceptor) CribToRemoteSpend(baby *babyOutput) error {
	err := i.ns.CribToRemoteSpend(baby)

	i.cribToRemoteSpendChan <- struct{}{}

	return err
}

func (i *nurseryStoreInterceptor) KidToRemoteSpend(kid *kidOutput) error {
	err := i.ns.KidToRemoteSpend(kid)

	i.kidToRemoteSpendChan <- struct{}{}

	return err
}

func (i *nurseryStoreInterceptor) GraduateKinder(height uint32) error {
	err := i.ns.GraduateKinder(height)

	i.graduateKinderChan <- struct{}{}

	return err
}

func (i *nurseryStoreInterceptor) FetchPreschools() ([]kidOutput, error) {
	return i.ns.FetchPreschools()
}

func (i *nurseryStoreInterceptor) FetchKinder() ([]kidOutput, error) {
	return i.ns.FetchKinder()
}

func (i *nurseryStoreInterceptor) FetchCribs() ([]babyOutput, error) {
	return i.ns.FetchCribs()
}

func (i *nurseryStoreInterceptor) FetchClass(height uint32) (*wire.MsgTx, []kidOutput, []babyOutput, error) {
	return i.ns.FetchClass(height)
}

func (i *nurseryStoreInterceptor) FinalizeKinder(height uint32, tx *wire.MsgTx) error {
	return i.ns.FinalizeKinder(height, tx)
}

func (i *nurseryStoreInterceptor) LastFinalizedHeight() (uint32, error) {
	return i.ns.LastFinalizedHeight()
}

func (i *nurseryStoreInterceptor) GraduateHeight(height uint32) error {
	return i.ns.GraduateHeight(height)
}

func (i *nurseryStoreInterceptor) LastGraduatedHeight() (uint32, error) {
	return i.ns.LastGraduatedHeight()
}

func (i *nurseryStoreInterceptor) HeightsBelowOrEqual(height uint32) ([]uint32, error) {
	return i.ns.HeightsBelowOrEqual(height)
}

func (i *nurseryStoreInterceptor) ForChanOutputs(chanPoint *wire.OutPoint,
	callback func([]byte, []byte) error) error {

	return i.ns.ForChanOutputs(chanPoint, callback)
}

func (i *nurseryStoreInterceptor) ListChannels() ([]wire.OutPoint, error) {
	return i.ns.ListChannels()
}

func (i *nurseryStoreInterceptor) IsMatureChannel(chanPoint *wire.OutPoint) (bool, error) {
	return i.ns.IsMatureChannel(chanPoint)
}

func (i *nurseryStoreInterceptor) RemoveChannel(chanPoint *wire.OutPoint) error {
	return i.ns.RemoveChannel(chanPoint)
}

type mockFeeEstimator struct{}

func (m *mockFeeEstimator) EstimateFeePerKW(
	numBlocks uint32) (lnwallet.SatPerKWeight, error) {

	return lnwallet.SatPerKWeight(10000), nil
}

func (m *mockFeeEstimator) Start() error {
	return nil
}
func (m *mockFeeEstimator) Stop() error {
	return nil
}

type nurseryMockSigner struct {
}

func (m *nurseryMockSigner) SignOutputRaw(tx *wire.MsgTx,
	signDesc *lnwallet.SignDescriptor) ([]byte, error) {

	return []byte{}, nil
}

func (m *nurseryMockSigner) ComputeInputScript(tx *wire.MsgTx,
	signDesc *lnwallet.SignDescriptor) (*lnwallet.InputScript, error) {

	return &lnwallet.InputScript{}, nil
}

type nurseryMockNotifier struct {
	confChannel map[chainhash.Hash]chan *chainntnfs.TxConfirmation
	epochChan   chan *chainntnfs.BlockEpoch
	spendChan   map[wire.OutPoint]chan *chainntnfs.SpendDetail
}

func newNurseryMockNotifier() *nurseryMockNotifier {
	return &nurseryMockNotifier{
		confChannel: make(map[chainhash.Hash]chan *chainntnfs.TxConfirmation),
		epochChan:   make(chan *chainntnfs.BlockEpoch),
		spendChan:   make(map[wire.OutPoint]chan *chainntnfs.SpendDetail),
	}
}

func (m *nurseryMockNotifier) confirmTx(txid *chainhash.Hash, height uint32) {
	m.getConfChannel(txid) <- &chainntnfs.TxConfirmation{BlockHeight: height}
}

func (m *nurseryMockNotifier) spendOutpoint(outpoint *wire.OutPoint,
	spendingTx *wire.MsgTx) {

	spenderTxHash := spendingTx.TxHash()
	m.getSpendChannel(outpoint) <- &chainntnfs.SpendDetail{
		SpenderTxHash: &spenderTxHash,
		SpendingTx:    spendingTx,
	}
}

func (m *nurseryMockNotifier) RegisterConfirmationsNtfn(txid *chainhash.Hash,
	_ []byte, numConfs, heightHint uint32) (*chainntnfs.ConfirmationEvent, error) {

	return &chainntnfs.ConfirmationEvent{
		Confirmed: m.getConfChannel(txid),
	}, nil
}

func (m *nurseryMockNotifier) getConfChannel(txid *chainhash.Hash) chan *chainntnfs.TxConfirmation {
	channel, ok := m.confChannel[*txid]
	if ok {
		return channel
	}
	channel = make(chan *chainntnfs.TxConfirmation)
	m.confChannel[*txid] = channel
	return channel
}

func (m *nurseryMockNotifier) RegisterBlockEpochNtfn(
	bestBlock *chainntnfs.BlockEpoch) (*chainntnfs.BlockEpochEvent, error) {
	return &chainntnfs.BlockEpochEvent{
		Epochs: m.epochChan,
		Cancel: func() {},
	}, nil
}

func (m *nurseryMockNotifier) Start() error {
	return nil
}

func (m *nurseryMockNotifier) Stop() error {
	return nil
}

func (m *nurseryMockNotifier) getSpendChannel(outpoint *wire.OutPoint) chan *chainntnfs.SpendDetail {
	channel, ok := m.spendChan[*outpoint]
	if ok {
		return channel
	}
	channel = make(chan *chainntnfs.SpendDetail, 1)
	m.spendChan[*outpoint] = channel
	return channel
}

func (m *nurseryMockNotifier) RegisterSpendNtfn(outpoint *wire.OutPoint, _ []byte,
	heightHint uint32) (*chainntnfs.SpendEvent, error) {
	return &chainntnfs.SpendEvent{
		Spend:  m.getSpendChannel(outpoint),
		Cancel: func() {},
	}, nil
}
