package sweep

import (
	"github.com/btcsuite/btcd/btcec"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/keychain"
	"github.com/lightningnetwork/lnd/lnwallet"
	"os"
	"runtime/pprof"
	"testing"
	"time"
)

type sweeperTestContext struct {
	sweeper      *UtxoSweeper
	notifier     *MockNotifier
	estimator    *lnwallet.StaticFeeEstimator
	publishChan  chan wire.MsgTx
	t            *testing.T
	walletInputs map[wire.OutPoint]*wire.MsgTx
	timeoutChan  chan chan time.Time
}

var (
	spendableInputs []*BaseInput
	testInputCount  int

	testPubKey, _ = btcec.ParsePubKey([]byte{
		0x04, 0x11, 0xdb, 0x93, 0xe1, 0xdc, 0xdb, 0x8a,
		0x01, 0x6b, 0x49, 0x84, 0x0f, 0x8c, 0x53, 0xbc, 0x1e,
		0xb6, 0x8a, 0x38, 0x2e, 0x97, 0xb1, 0x48, 0x2e, 0xca,
		0xd7, 0xb1, 0x48, 0xa6, 0x90, 0x9a, 0x5c, 0xb2, 0xe0,
		0xea, 0xdd, 0xfb, 0x84, 0xcc, 0xf9, 0x74, 0x44, 0x64,
		0xf8, 0x2e, 0x16, 0x0b, 0xfa, 0x9b, 0x8b, 0x64, 0xf9,
		0xd4, 0xc0, 0x3f, 0x99, 0x9b, 0x86, 0x43, 0xf6, 0x56,
		0xb4, 0x12, 0xa3,
	}, btcec.S256())
)

func createTestInput(value int64, witnessType lnwallet.WitnessType) BaseInput {
	hash := chainhash.Hash{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		byte(testInputCount)}

	input := MakeBaseInput(
		&wire.OutPoint{
			Hash: hash,
		},
		witnessType,
		&lnwallet.SignDescriptor{
			Output: &wire.TxOut{
				Value: value,
			},
			KeyDesc: keychain.KeyDescriptor{
				PubKey: testPubKey,
			},
		},
		0,
	)

	testInputCount++

	return input
}

func init() {
	// Create a set of test spendable inputs.
	for i := 0; i < 5; i++ {
		input := createTestInput(int64(10000+i*500),
			lnwallet.CommitmentTimeLock)

		spendableInputs = append(spendableInputs, &input)
	}
}

func createSweeperTestContext(t *testing.T) *sweeperTestContext {

	notifier := NewMockNotifier(t)

	estimator := &lnwallet.StaticFeeEstimator{
		FeePerKW: lnwallet.SatPerKWeight(10000),
		RelayFee: lnwallet.SatPerKWeight(1000),
	}

	publishChan := make(chan wire.MsgTx, 2)

	ctx := &sweeperTestContext{
		notifier:     notifier,
		publishChan:  publishChan,
		walletInputs: make(map[wire.OutPoint]*wire.MsgTx),
		t:            t,
		estimator:    estimator,
		timeoutChan:  make(chan chan time.Time, 1),
	}

	var outputScriptCount byte
	ctx.sweeper = New(&UtxoSweeperConfig{
		GenSweepScript: func() ([]byte, error) {
			script := []byte{outputScriptCount}
			outputScriptCount++
			return script, nil
		},
		Estimator: estimator,
		Signer:    &mockSigner{},
		Notifier:  notifier,
		PublishTransaction: func(tx *wire.MsgTx) error {
			for _, input := range tx.TxIn {
				ctx.walletInputs[input.PreviousOutPoint] = tx
			}
			log.Tracef("Publishing tx %v", tx.TxHash())
			select {
			case publishChan <- *tx:
			case <-time.After(defaultTestTimeout):
				t.Fatalf("unexpected tx published")
			}
			return nil
		},
		NewBatchTimer: func() <-chan time.Time {
			c := make(chan time.Time, 1)
			ctx.timeoutChan <- c
			return c
		},
		ChainIO: &mockChainIO{},
		HasSpendingTx: func(op wire.OutPoint) (*wire.MsgTx, error) {
			spendingTx, ok := ctx.walletInputs[op]
			if !ok {
				return nil, nil
			}

			return spendingTx, nil
		},
		MaxInputsPerTx: 3,
	})

	ctx.sweeper.Start()

	return ctx
}

func (ctx *sweeperTestContext) tick() {
	log.Trace("Waiting for tick to be consumed")
	select {
	case c := <-ctx.timeoutChan:
		select {
		case c <- time.Time{}:
			log.Trace("Tick")
		case <-time.After(defaultTestTimeout):
			ctx.t.Fatal("tick timeout - tick not consumed")
		}
	case <-time.After(defaultTestTimeout):
		ctx.t.Fatal("tick timeout - no new timer created")
	}
}

func (ctx *sweeperTestContext) assertNoNewTimer() {
	select {
	case <-ctx.timeoutChan:
		ctx.t.Fatal("no new timer expected")
	default:
	}
}

func (ctx *sweeperTestContext) finish() {
	// We assume that when finish is called, sweeper has finished all its
	// goroutines. This implies that the waitgroup is empty.
	signalChan := make(chan struct{})
	go func() {
		ctx.sweeper.wg.Wait()
		close(signalChan)
	}()

	// The only goroutine that is still expected to be running is
	// collector(). Simulate exit of this goroutine.
	ctx.sweeper.wg.Done()

	// We now expect the Wait to succeed.
	select {
	case <-signalChan:
	case <-time.After(time.Second):
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)

		ctx.t.Fatalf("lingering goroutines detected after test " +
			"is finished")
	}

	// Restore waitgroup state to what it was before.
	ctx.sweeper.wg.Add(1)

	ctx.sweeper.Stop()

	// We should have consumed and asserted all published transactions in
	// our unit tests.
	ctx.assertNoTx()
	ctx.assertNoNewTimer()
}

func (ctx *sweeperTestContext) assertNoTx() {
	select {
	case <-ctx.publishChan:
		ctx.t.Fatalf("unexpected transactions published")
	default:
	}
}

func (ctx *sweeperTestContext) receiveTx() wire.MsgTx {
	var tx wire.MsgTx
	select {
	case tx = <-ctx.publishChan:
		return tx
	case <-time.After(5 * time.Second):
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)

		ctx.t.Fatalf("tx not published")
	}
	return tx
}

// TestSuccess tests the sweeper happy flow.
func TestSuccess(t *testing.T) {
	ctx := createSweeperTestContext(t)

	resultChan, err := ctx.sweeper.SweepInput(spendableInputs[0])
	if err != nil {
		t.Fatal(err)
	}

	ctx.tick()

	sweepTx := ctx.receiveTx()

	// Spend the input of the sweep tx.
	ctx.notifier.SpendOutpoint(spendableInputs[0].OutPoint(), &sweepTx)

	select {
	case result := <-resultChan:
		if result.Err != nil {
			t.Fatalf("expected successful spend, but received "+
				"error %v instead", result.Err)
		}
		if result.Tx.TxHash() != sweepTx.TxHash() {
			t.Fatalf("expected sweep tx ")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("no result received")
	}

	ctx.finish()
}

// TestDust asserts that inputs that are not big enough to raise above the dust
// limit, are held back until the total set does surpass the limit.
func TestDust(t *testing.T) {
	ctx := createSweeperTestContext(t)

	// Sweeping a single output produces a tx of 486 weight units. With the
	// test fee rate, the sweep tx will pay 4860 sat in fees.
	//
	// Create an input so that the output after paying fees is still
	// positive (400 sat), but less than the dust limit (537 sat) for the
	// sweep tx output script (P2WPKH).
	dustInput := createTestInput(5260, lnwallet.CommitmentTimeLock)

	_, err := ctx.sweeper.SweepInput(&dustInput)
	if err != nil {
		t.Fatal(err)
	}

	// No sweep transaction is expected now. The sweeper should recognize
	// that the sweep output will not be relayed and not generate the tx.

	// Sweep another input that brings the tx output above the dust limit.
	largeInput := createTestInput(100000, lnwallet.CommitmentTimeLock)

	_, err = ctx.sweeper.SweepInput(&largeInput)
	if err != nil {
		t.Fatal(err)
	}

	ctx.tick()

	// The second input brings the sweep output above the dust limit. We
	// expect a sweep tx now.

	sweepTx := ctx.receiveTx()
	if len(sweepTx.TxIn) != 2 {
		t.Fatalf("Expected tx to sweep 2 inputs, but contains %v "+
			"inputs instead", len(sweepTx.TxIn))
	}

	ctx.notifier.SpendOutpoint(dustInput.OutPoint(), &sweepTx)
	ctx.notifier.SpendOutpoint(largeInput.OutPoint(), &sweepTx)

	ctx.finish()
}

// TestNegativeInput asserts that no inputs with a negative yield are swept.
// Negative yield means that the value minus the added fee is negative.
func TestNegativeInput(t *testing.T) {
	ctx := createSweeperTestContext(t)

	// Sweep an input large enough to cover fees, so in any case the tx
	// output will be above the dust limit.
	largeInput := createTestInput(100000, lnwallet.CommitmentNoDelay)

	_, err := ctx.sweeper.SweepInput(&largeInput)
	if err != nil {
		t.Fatal(err)
	}

	// Sweep an additional input with a negative net yield. The weight of
	// the HtlcAcceptedRemoteSuccess input type adds more in fees than its
	// value at the current fee level.
	negativeInput := createTestInput(2900, lnwallet.HtlcOfferedRemoteTimeout)
	_, err = ctx.sweeper.SweepInput(&negativeInput)
	if err != nil {
		t.Fatal(err)
	}

	// Sweep a third input that has a smaller output than the previous one,
	// but yields positively because of its lower weight.
	positiveInput := createTestInput(2800, lnwallet.CommitmentNoDelay)
	_, err = ctx.sweeper.SweepInput(&positiveInput)
	if err != nil {
		t.Fatal(err)
	}

	ctx.tick()

	// We expect that a sweep tx is published now, but it should only
	// contain the large input. The negative input should stay out of sweeps
	// until fees come down to get a positive net yield.
	sweepTx1 := ctx.receiveTx()

	if !testTxIns(&sweepTx1, []*wire.OutPoint{
		largeInput.OutPoint(), positiveInput.OutPoint(),
	}) {
		t.Fatal("Tx does not contain expected inputs")
	}

	ctx.notifier.SpendOutpoint(largeInput.OutPoint(), &sweepTx1)
	ctx.notifier.SpendOutpoint(positiveInput.OutPoint(), &sweepTx1)

	// Lower fee rate so that the negative input is no longer negative.
	ctx.estimator.FeePerKW = 1000

	// Create another large input
	secondLargeInput := createTestInput(100000, lnwallet.CommitmentNoDelay)
	_, err = ctx.sweeper.SweepInput(&secondLargeInput)
	if err != nil {
		t.Fatal(err)
	}

	ctx.tick()

	sweepTx2 := ctx.receiveTx()
	if !testTxIns(&sweepTx2, []*wire.OutPoint{
		secondLargeInput.OutPoint(), negativeInput.OutPoint(),
	}) {
		t.Fatal("Tx does not contain expected inputs")
	}

	ctx.notifier.SpendOutpoint(secondLargeInput.OutPoint(), &sweepTx2)
	ctx.notifier.SpendOutpoint(negativeInput.OutPoint(), &sweepTx2)

	ctx.finish()
}

func testTxIns(tx *wire.MsgTx, inputs []*wire.OutPoint) bool {
	if len(tx.TxIn) != len(inputs) {
		return false
	}

	ins := make(map[wire.OutPoint]struct{})
	for _, in := range tx.TxIn {
		ins[in.PreviousOutPoint] = struct{}{}
	}

	for _, expectedIn := range inputs {
		if _, ok := ins[*expectedIn]; !ok {
			return false
		}
	}

	return true
}

// TestChunks asserts that large sets of inputs are split into multiple txes.
func TestChunks(t *testing.T) {
	ctx := createSweeperTestContext(t)

	// Sweep three inputs.
	for _, input := range spendableInputs[:3] {
		_, err := ctx.sweeper.SweepInput(input)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Because the max number of inputs is reached, we expect a sweep to be
	// published immediately.
	sweepTx1 := ctx.receiveTx()
	if len(sweepTx1.TxIn) != 3 {
		t.Fatalf("Expected first tx to sweep 3 inputs, but contains %v "+
			"inputs instead", len(sweepTx1.TxIn))
	}

	for _, input := range spendableInputs[:3] {
		ctx.notifier.SpendOutpoint(input.OutPoint(), &sweepTx1)
	}

	// Started time still expires, even though sweeper isn't listening
	// anymore.
	ctx.tick()

	// Sweep two more inputs.
	for _, input := range spendableInputs[3:5] {
		_, err := ctx.sweeper.SweepInput(input)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Max number of inputs not reached yet, so it needs a tick to sweep.
	ctx.tick()

	sweepTx2 := ctx.receiveTx()
	if len(sweepTx2.TxIn) != 2 {
		t.Fatalf("Expected first tx to sweep 2 inputs, but contains %v "+
			"inputs instead", len(sweepTx1.TxIn))
	}

	for _, input := range spendableInputs[3:5] {
		ctx.notifier.SpendOutpoint(input.OutPoint(), &sweepTx1)
	}

	ctx.finish()
}

// TestRemoteSpend asserts that remote spends are properly detected and handled
// both before the sweep is published as well as after.
func TestRemoteSpend(t *testing.T) {
	t.Run("pre-sweep", func(t *testing.T) {
		testRemoteSpend(t, false)
	})
	t.Run("post-sweep", func(t *testing.T) {
		testRemoteSpend(t, true)
	})
}

func testRemoteSpend(t *testing.T, postSweep bool) {
	ctx := createSweeperTestContext(t)

	resultChan1, err := ctx.sweeper.SweepInput(spendableInputs[0])
	if err != nil {
		t.Fatal(err)
	}

	resultChan2, err := ctx.sweeper.SweepInput(spendableInputs[1])
	if err != nil {
		t.Fatal(err)
	}

	if postSweep {
		ctx.tick()

		ctx.receiveTx()
	}

	// Spend the input with an unknown tx.
	remoteTx := &wire.MsgTx{
		TxIn: []*wire.TxIn{
			{
				PreviousOutPoint: *(spendableInputs[0].OutPoint()),
			},
		},
	}
	ctx.notifier.SpendOutpoint(spendableInputs[0].OutPoint(), remoteTx)

	select {
	case result := <-resultChan1:
		if result.Err != ErrRemoteSpend {
			t.Fatalf("expected remote spend")
		}
		if result.Tx.TxHash() != remoteTx.TxHash() {
			t.Fatalf("expected remote spend tx")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("no result received")
	}

	// Assert that the sweeper sweeps the remaining input.
	ctx.tick()
	sweepTx := ctx.receiveTx()

	if len(sweepTx.TxIn) != 1 {
		t.Fatal("expected sweep to only sweep the one remaining output")
	}

	ctx.notifier.SpendOutpoint(spendableInputs[1].OutPoint(), &sweepTx)

	select {
	case result := <-resultChan2:
		if result.Err != nil {
			t.Fatalf("expected sweep success")
		}
		if result.Tx.TxHash() != sweepTx.TxHash() {
			t.Fatalf("expected sweep tx")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("no result received")
	}

	ctx.finish()
}

// TestIdempotency asserts that offering the same input multiple times is
// handled correctly.
func TestIdempotency(t *testing.T) {
	ctx := createSweeperTestContext(t)

	resultChan1, err := ctx.sweeper.SweepInput(spendableInputs[0])
	if err != nil {
		t.Fatal(err)
	}
	resultChan2, err := ctx.sweeper.SweepInput(spendableInputs[0])
	if err != nil {
		t.Fatal(err)
	}

	ctx.assertNoTx()

	ctx.tick()

	spendingTx := ctx.receiveTx()

	// Spend the input of the sweep tx.
	ctx.notifier.SpendOutpoint(spendableInputs[0].OutPoint(), &spendingTx)

	resultsReceived := 0
	for resultsReceived < 2 {
		select {
		case <-resultChan1:
			resultsReceived++
		case <-resultChan2:
			resultsReceived++
		case <-time.After(5 * time.Second):
			t.Fatalf("no result received")
		}
		log.Tracef("Result received")
	}

	ctx.finish()
}

// TestNoInputs asserts that nothing happens if nothing happens.
func TestNoInputs(t *testing.T) {
	ctx := createSweeperTestContext(t)

	// No tx should appear. This is asserted in finish().
	ctx.finish()
}

// TestRestart asserts that the sweeper picks up sweeping properly after
// a restart.
func TestRestart(t *testing.T) {

	ctx := createSweeperTestContext(t)

	// Sweep input.
	_, err := ctx.sweeper.SweepInput(spendableInputs[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx.tick()

	// Should result in sweep tx.
	spendingTx1 := ctx.receiveTx()

	// Sweep another input.
	_, err = ctx.sweeper.SweepInput(spendableInputs[1])
	if err != nil {
		t.Fatal(err)
	}

	ctx.tick()

	spendingTx2 := ctx.receiveTx()

	// Simulate that tx 2 didn't reach the mempool.

	outPoint := spendableInputs[1].OutPoint()
	delete(ctx.walletInputs, *outPoint)

	// Restart sweeper.
	ctx.sweeper.Stop()

	ctx.sweeper = New(ctx.sweeper.cfg)
	ctx.sweeper.Start()

	// Simulate other subsystem (eg contract resolver) re-offering inputs.
	spendChan1, err := ctx.sweeper.SweepInput(spendableInputs[0])
	if err != nil {
		t.Fatal(err)
	}

	spendChan2, err := ctx.sweeper.SweepInput(spendableInputs[1])
	if err != nil {
		t.Fatal(err)
	}

	ctx.tick()

	// We don't expect tx1 to be republished, as the wallet reported the
	// input as part of an unconfirmed tx. Tx2 did not reach the mempool, so
	// for that tx we do expect a new transaction.

	spendingTx2 = ctx.receiveTx()

	// Spend inputs of sweep txes and verify that spend channels signal
	// spends.
	ctx.notifier.SpendOutpoint(
		&spendingTx1.TxIn[0].PreviousOutPoint, &spendingTx1)

	select {
	case <-spendChan1:
	case <-time.After(defaultTestTimeout):
		t.Fatalf("tx not removed from store")
	}

	ctx.notifier.SpendOutpoint(
		&spendingTx2.TxIn[0].PreviousOutPoint, &spendingTx2)

	select {
	case <-spendChan2:
	case <-time.After(defaultTestTimeout):
		t.Fatalf("tx not removed from store")
	}

	// Restart sweeper again.
	ctx.sweeper.Stop()
	ctx.sweeper = New(ctx.sweeper.cfg)
	ctx.sweeper.Start()

	// Output should have been marked spend and not trigger any publish of
	// tx on restart.
	ctx.assertNoTx()
}
