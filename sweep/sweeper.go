package sweep

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
	"github.com/btcsuite/btcwallet/wallet/txrules"
	"github.com/lightningnetwork/lnd/chainntnfs"
	"github.com/lightningnetwork/lnd/lnwallet"
)

var (
	// DefaultMaxInputsPerTx specifies the default maximum number of inputs
	// allowed in a single sweep tx. If more need to be swept, multiple txes
	// are created and published.
	DefaultMaxInputsPerTx = 100

	// ErrRemoteSpend is returned in case an output that we try to sweep is
	// confirmed in a tx of the remote party.
	ErrRemoteSpend = errors.New("remote party swept utxo")
)

// UtxoSweeper is responsible for sweeping outputs back into the wallet
type UtxoSweeper struct {
	started uint32 // To be used atomically.
	stopped uint32 // To be used atomically.

	cfg *UtxoSweeperConfig

	newInputs chan *sweepInputMessage
	spendChan chan *chainntnfs.SpendDetail

	pendingInputs map[wire.OutPoint]*pendingInput

	timer <-chan time.Time

	dustLimit btcutil.Amount

	quit chan struct{}
	wg   sync.WaitGroup
}

// UtxoSweeperConfig contains dependencies of UtxoSweeper.
type UtxoSweeperConfig struct {
	// GenSweepScript generates a P2WKH script belonging to the wallet where
	// funds can be swept.
	GenSweepScript func() ([]byte, error)

	// Estimator is used when crafting sweep transactions to estimate the
	// necessary fee relative to the expected size of the sweep
	// transaction.
	Estimator lnwallet.FeeEstimator

	// Signer is used by the sweeper to generate valid witnesses at the
	// time the incubated outputs need to be spent.
	Signer lnwallet.Signer

	// PublishTransaction facilitates the process of broadcasting a signed
	// transaction to the appropriate network.
	PublishTransaction func(*wire.MsgTx) error

	// NewBatchTimer creates a channel that will be sent on when a certain
	// time window has passed. During this time window, new inputs can still
	// be added to the sweep tx that is about to be generated.
	NewBatchTimer func() <-chan time.Time

	// SweepTxConfTarget assigns a confirmation target for sweep txes on
	// which the fee calculation will be based.
	SweepTxConfTarget uint32

	// Notifier is an instance of a chain notifier we'll use to watch for
	// certain on-chain events.
	Notifier chainntnfs.ChainNotifier

	// ChainIO allows us to query the state of the current main chain.
	ChainIO lnwallet.BlockChainIO

	// HasSpendingTx allows the sweeper to check whether there is already a
	// unconfirmed or confirmed wallet transaction for this output. It
	// prevents unnecessary republication and address inflation.
	HasSpendingTx func(op wire.OutPoint) (*wire.MsgTx, error)

	// MaxInputsPerTx specifies the  maximum number of inputs allowed in a
	// single sweep tx. If more need to be swept, multiple txes are created
	// and published.
	MaxInputsPerTx int
}

// Result is the struct that is pushed through the result channel. Callers
// can use this to be informed of the final sweep result. In case of a remote
// spend, Err will be ErrRemoteSpend.
type Result struct {
	// Err is the final result of the sweep. It is nil when the input is
	// swept successfully by us. ErrRemoteSpend is returned when another
	// party took the input.
	Err error

	// Tx is the transaction that spent the input.
	Tx *wire.MsgTx
}

// sweepInputMessage structs are used in the internal channel between the
// SweepInput call and the sweeper main loop.
type sweepInputMessage struct {
	input      Input
	resultChan chan Result
}

type inputState uint8

const (
	// stateNew is the initial state of inputs. No tx of ours has been
	// published yet. Not in this run and not in previous runs (as reported
	// via HasWalletTx).
	stateNew inputState = iota

	// statePublished indicates that we published a sweep tx for this input.
	statePublished

	// stateError indicates that we didn't succeed in publishing the sweep
	// tx.
	stateError
)

// pendingInput is created when an input reaches the main loop for the first
// time. It tracks all relevant state that is needed for sweeping.
type pendingInput struct {
	// txHash is the hash of our sweep tx. It is set when the sweep is
	// published and the input moves to statePublished.
	txHash chainhash.Hash

	// listeners is a list of channels over which the final outcome of the
	// sweep needs to be broadcasted.
	listeners []chan Result

	// input is the original struct that contains the input and sign
	// descriptor.
	input Input

	// ntfnRegCancel is populated with a function that cancels the chain
	// notifier spend registration.
	ntfnRegCancel func()

	// state tracks the state this input is in.
	state inputState

	// witnessSizeUpperBound is the upper bound of the number of witness
	// bytes that this input is going to add to the tx weight.
	witnessSizeUpperBound int
}

// New returns a new Sweeper instance.
func New(cfg *UtxoSweeperConfig) *UtxoSweeper {
	// Calculate dust limit based on the P2WPKH output script of the sweep
	// txes.
	dustLimit := txrules.GetDustThreshold(
		lnwallet.P2WPKHSize,
		btcutil.Amount(cfg.Estimator.RelayFeePerKW().FeePerKVByte()))

	return &UtxoSweeper{
		cfg:           cfg,
		newInputs:     make(chan *sweepInputMessage),
		spendChan:     make(chan *chainntnfs.SpendDetail),
		quit:          make(chan struct{}),
		pendingInputs: make(map[wire.OutPoint]*pendingInput),
		dustLimit:     dustLimit,
	}
}

// Start starts the process of constructing and publish sweep txes.
func (s *UtxoSweeper) Start() error {
	if !atomic.CompareAndSwapUint32(&s.started, 0, 1) {
		return nil
	}

	log.Tracef("Sweeper starting")

	s.wg.Add(1)
	go s.collector()

	return nil
}

// Stop stops sweeper from listening to block epochs and constructing sweep
// txes.
func (s *UtxoSweeper) Stop() error {
	if !atomic.CompareAndSwapUint32(&s.stopped, 0, 1) {
		return nil
	}

	log.Tracef("Sweeper shutting down")

	close(s.quit)
	s.wg.Wait()

	log.Tracef("Sweeper shut down")

	return nil
}

// SweepInput sweeps inputs back into the wallet. The inputs will be batched and
// swept after the batch time window ends.
//
// NOTE: Extreme care needs to be taken that input isn't changed externally.
// Because it is an interface and we don't know what is exactly behind it, we
// cannot make a local copy in sweeper.
func (s *UtxoSweeper) SweepInput(input Input) (chan Result, error) {
	if input == nil || input.OutPoint() == nil || input.SignDesc() == nil {
		return nil, errors.New("nil input received")
	}

	log.Debugf("Sweep request received: %v", input.OutPoint())

	sweeperInput := &sweepInputMessage{
		input:      input,
		resultChan: make(chan Result, 1),
	}

	// Deliver input to main event loop.
	select {
	case s.newInputs <- sweeperInput:
	case <-s.quit:
		return nil, fmt.Errorf("sweeper shutting down")
	}

	return sweeperInput.resultChan, nil
}

// collector is the sweeper main loop. It processes new inputs, spend
// notifications and counts down to publication of the sweep tx.
func (s *UtxoSweeper) collector() {
	defer s.wg.Done()

	for {
		select {
		case input := <-s.newInputs:
			outpoint := *input.input.OutPoint()
			pendInput, pending := s.pendingInputs[outpoint]
			if pending {
				log.Debugf("Already pending input %v received",
					outpoint)

				// Add additional result channel to signal spend
				// of this input.
				pendInput.listeners = append(
					pendInput.listeners, input.resultChan)

				continue
			}

			// Create a new pendingInput and initialize the
			// listeners slice with the passed in result channel. If
			// this input is offered for sweep again, the result
			// channel will be appended to this slice.
			pendInput = &pendingInput{
				listeners: []chan Result{input.resultChan},
				input:     input.input,
				state:     stateNew,
			}
			s.pendingInputs[outpoint] = pendInput

			size, err := getInputWitnessSizeUpperBound(input.input)
			if err != nil {
				err = fmt.Errorf("cannot determine size: %v",
					err)

				s.signalAndRemove(&outpoint, Result{Err: err})

				continue
			}
			pendInput.witnessSizeUpperBound = size

			// Start watching for spend of this input, either by us
			// or the remote party. The possibility of a remote
			// spend is the reason why just registering for conf. of
			// our sweep tx isn't enough.
			cancel, err := s.registerSpendNtfn(input.input)
			if err != nil {
				err = fmt.Errorf("cannot watch for spend: %v",
					err)

				s.signalAndRemove(&outpoint, Result{Err: err})

				continue
			}
			pendInput.ntfnRegCancel = cancel

			// Lookup if there is already a confirmed or unconfirmed
			// tx of ours that spends this input.
			spendingTx, err := s.cfg.HasSpendingTx(outpoint)
			if err != nil {
				log.Errorf("HasSpendingTx %v: %v", outpoint,
					err)

				// TODO: Re-enable code below once HasSpendingTx
				// is reliable. For now, just continue as if
				// there is no spending tx.

				// s.signalAndRemove(&outpoint, Result{Err: err})
				// continue
			}

			if spendingTx != nil {
				spendingTxHash := spendingTx.TxHash()
				log.Debugf("Already swept input %v received "+
					"(tx: %v)", outpoint, spendingTxHash)

				// Save spending tx to find sibling inputs in
				// case of remote spend.
				pendInput.txHash = spendingTxHash
				pendInput.state = statePublished

				continue
			}

			// If there is no spending tx yet, we include
			// this input in the list of inputs to sweep in
			// the next round.

			log.Debugf("Scheduling sweep for input %v",
				pendInput.input.OutPoint())

			s.scheduleSweep()

		case spend := <-s.spendChan:
			invalidatedTxes := make(map[chainhash.Hash]struct{})
			for _, txIn := range spend.SpendingTx.TxIn {
				outpoint := txIn.PreviousOutPoint

				// Check if this input is known to us. It could
				// probably be unknown if we canceled the
				// registration, deleted from pendingInputs but
				// the ntfn was in-flight already. Or this could
				// be not one of our inputs.
				pendingInput, ok := s.pendingInputs[outpoint]
				if !ok {
					continue
				}

				var err error

				switch pendingInput.state {

				// We haven't even published a sweep and
				// won't need to anymore.
				case stateNew, stateError:
					err = ErrRemoteSpend

				// If we published, check if spender tx hash
				// matches our tx. If not, this is a remote
				// spend.
				case statePublished:
					txHash := pendingInput.txHash
					if txHash != *spend.SpenderTxHash {
						// This implies that the stored
						// tx must be invalid now.
						invalidatedTxes[txHash] =
							struct{}{}

						err = ErrRemoteSpend
					}
				}

				// Signal result channels sweep result.
				s.signalAndRemove(&outpoint, Result{
					Tx:  spend.SpendingTx,
					Err: err,
				})
			}

			// Reschedule remaining inputs of all invalidated txes.
			rescheduledInputs := false
			for _, input := range s.pendingInputs {
				if input.state != statePublished {
					continue
				}

				_, ok := invalidatedTxes[input.txHash]
				if !ok {
					// Input isn't invalidated.
					continue
				}

				// Move input back to the new state so it can be
				// re-swept.
				input.state = stateNew

				log.Debugf("Re-schedule sweep for input %v",
					input.input.OutPoint())

				rescheduledInputs = true
			}

			// If any inputs were rescheduled, check to see if
			// already a tx consisting the maximum number of inputs
			// can be constructed.
			if rescheduledInputs {
				s.scheduleSweep()
			}

		case <-s.timer:
			log.Debugf("Timer expired")

			// Set timer to nil so we know that a new timer needs to
			// be started when new inputs arrive.
			s.timer = nil

			// Examine pending inputs and try to construct one or
			// more sweep txes. Publish a tx even if it is not full
			// yet (input count less than maximum allowed inputs per
			// tx).
			_, err := s.sweepAll(false)
			if err != nil {
				log.Errorf("Cannot sweep on timer event: %v",
					err)
			}

		case <-s.quit:
			return
		}

		// TODO: Handle the case where a tx is not confirmed within
		// specified time and fee needs to be bumped.

		// TODO: Periodically refresh fee rate and see if it unlocks any
		// inputs that then have a positive yield.
	}
}

// scheduleSweep tries to sweep inputs, but only goes through if the generated
// tx has the maximum number of inputs. If inputs remain, the batch timer is
// started to create an opportunity for more inputs to be added.
func (s *UtxoSweeper) scheduleSweep() {
	// Try to sweep inputs, but only if full txes can be generated.
	inputsRemaining, err := s.sweepAll(true)
	if err != nil {
		log.Errorf("Cannot sweep during scheduling: %v", err)
		return
	}

	if !inputsRemaining {
		// Nothing sweepable anymore, disable timer. This prevents two
		// sweeps from happening in fast succession. The first sweep
		// triggered by the maximum allowed number of inputs and the
		// second because of the timer.
		log.Debugf("No inputs remaining, disabling timer")
		s.timer = nil
		return
	}

	if s.timer == nil {
		// Start sweep timer to create opportunity for more inputs to be
		// added before a tx is constructed.
		log.Debugf("Starting timer")
		s.timer = s.cfg.NewBatchTimer()
	} else {
		log.Debugf("Timer still ticking")
	}
}

// signalAndRemove notifies the listeners of the final result of the input
// sweep. It cancels any pending spend notification and removes the input from
// the list of pending inputs. When this function returns, the sweeper has
// completely forgotten about the input.
func (s *UtxoSweeper) signalAndRemove(outpoint *wire.OutPoint, result Result) {
	pendInput := s.pendingInputs[*outpoint]
	listeners := pendInput.listeners

	if result.Err == nil {
		log.Debugf("Dispatching sweep success for %v to %v listeners",
			outpoint, len(listeners))
	} else {
		log.Debugf("Dispatching sweep error for %v to %v listeners: %v",
			outpoint, len(listeners), result.Err)
	}

	for _, resultChan := range listeners {
		resultChan <- result
	}

	// Cancel spend notification with chain notifier. This is not necessary
	// in case of a success, except for that a reorg could still happen.
	if pendInput.ntfnRegCancel != nil {
		pendInput.ntfnRegCancel()
	}

	// Inputs are no longer pending after result has been sent.
	delete(s.pendingInputs, *outpoint)
}

// sweepAll goes through all pending inputs and constructs sweep transactions,
// each up to the configured maximum number of inputs. Negative yield inputs are
// skipped. Transactions with an output below the dust limit are not published.
// Those inputs remain pending and will be bundled with future inputs if
// possible.
//
// Returns a boolean indicating whether there are sweepable inputs remaining.
// This can happen when fullOnly is true and there are less sweepable inputs
// then the configured maximum number per tx.
func (s *UtxoSweeper) sweepAll(fullOnly bool) (bool, error) {
	// Retrieve fee estimate for input filtering and final tx fee
	// calculation.
	satPerKW, err := s.cfg.Estimator.EstimateFeePerKW(
		s.cfg.SweepTxConfTarget)
	if err != nil {
		return false, err
	}

	// Retrieve current height to properly set tx locktime.
	_, currentHeight, err := s.cfg.ChainIO.GetBestBlock()
	if err != nil {
		return false, err
	}

	// Filter for inputs that need to be swept.
	var sweepableInputs []*pendingInput
	for _, input := range s.pendingInputs {
		// Skip inputs that are already part of a sweep tx.
		if input.state != stateNew {
			continue
		}

		sweepableInputs = append(sweepableInputs, input)
	}

	// Sort input by yield. We will start constructing sweep transactions
	// starting with the highest yield inputs. This is to prevent the
	// construction of a sweep tx with an output below the dust limit,
	// causing the sweep process to stop, while there are still higher value
	// inputs sweepable. It also allows us to stop evaluating more inputs
	// when the first input in this ordering is encountered with a negative
	// yield.
	//
	// Yield is calculated as the difference between value and added fee for
	// this input. The fee calculation excludes fee components that are
	// common to all inputs, as those wouldn't influence the order. The
	// single component that is differentiating is witness size.
	//
	// For witness size, a worst case upper limit is taken. The actual size
	// depends on the signature length, which is not known yet at this
	// point.
	yield := func(i *pendingInput) int64 {
		return i.input.SignDesc().Output.Value -
			int64(satPerKW.FeeForWeight(
				int64(i.witnessSizeUpperBound)))
	}

	sort.Slice(sweepableInputs, func(i, j int) bool {
		return yield(sweepableInputs[i]) > yield(sweepableInputs[j])
	})

	// Select blocks of inputs up to the configured maximum number.
	stopSweep := false
	for len(sweepableInputs) > 0 && !stopSweep {
		var weightEstimate lnwallet.TxWeightEstimator

		// Add the sweep tx output to the weight estimate.
		weightEstimate.AddP2WKHOutput()

		var inputList []Input
		var total, outputValue int64
		for len(sweepableInputs) > 0 {
			input := sweepableInputs[0]
			sweepableInputs = sweepableInputs[1:]
			size, err := getInputWitnessSizeUpperBound(input.input)
			if err != nil {
				log.Warnf("Failed adding input weight: %v", err)
				continue
			}
			weightEstimate.AddWitnessInput(size)

			newTotal := total +
				input.input.SignDesc().Output.Value

			weight := weightEstimate.Weight()
			fee := satPerKW.FeeForWeight(int64(weight))
			newOutputValue := newTotal - int64(fee)

			// If adding this input makes the total output value of
			// the tx decrease, this is a negative yield input. It
			// shouldn't be added to the tx. We also don't need to
			// consider further inputs. Because of the sorting by
			// value, subsequent inputs almost certainly have an
			// even higher negative yield.
			if newOutputValue <= outputValue {
				stopSweep = true
				break
			}

			inputList = append(inputList, input.input)
			total = newTotal
			outputValue = newOutputValue
			if len(inputList) >= s.cfg.MaxInputsPerTx {
				break
			}
		}

		// We can get an empty input list if all of the inputs had a
		// negative yield. In that case, we can stop here.
		if len(inputList) == 0 {
			return false, nil
		}

		// If the output value of this block of inputs does not reach
		// the dust limit, stop sweeping. Because of the sorting,
		// continuing with remaining inputs will only lead to
		// transactions with a even lower output value.
		if outputValue < int64(s.dustLimit) {
			log.Debugf("Tx output value %v below dust limit of %v",
				outputValue, s.dustLimit)
			return false, nil
		}

		// If fullOnly, don't sweep if less inputs than the maximum. Do
		// this check after the dust limit check, to properly report
		// whether there are sweepable inputs remaining.
		if fullOnly && len(inputList) < s.cfg.MaxInputsPerTx {
			return true, nil
		}

		s.sweep(inputList, uint32(currentHeight), satPerKW)
	}

	return false, nil
}

// sweep creates and publishes a sweep tx for the provided list of inputs. It
// uses the outcome to advance the pending input states and notifies listeners
// if necessary.
func (s *UtxoSweeper) sweep(inputList []Input, currentHeight uint32,
	feePerKw lnwallet.SatPerKWeight) {

	sweepTx, err := s.createAndPublishTx(inputList, currentHeight, feePerKw)
	if err != nil {
		log.Error(err)
	}

	// TODO: Rebroadcaster/retry mechanisms need to be added.

	// Advance input states or report outcomes.
	for _, input := range inputList {
		pi := s.pendingInputs[*(input.OutPoint())]

		switch {

		// Published successfully, record tx hash.
		case err == nil:
			pi.txHash = sweepTx.TxHash()
			pi.state = statePublished

		// In case of a double spend, don't notify listeners yet
		// but wait for the spending tx to trigger the spend
		// notification. Because of the inaccurate error
		// reporting, ErrDoubleSpend may actually mean various
		// things other than a real double spend.
		case err == lnwallet.ErrDoubleSpend:
			pi.state = stateError

		// In case of an other error, signal the listener and
		// remove the input.
		default:
			s.signalAndRemove(input.OutPoint(), Result{Err: err})
		}
	}
}

func (s *UtxoSweeper) createAndPublishTx(inputs []Input,
	currentHeight uint32, feePerKw lnwallet.SatPerKWeight) (
	*wire.MsgTx, error) {

	tx, err := s.createSweepTx(inputs, s.cfg.SweepTxConfTarget,
		currentHeight, feePerKw)
	if err != nil {
		return nil, fmt.Errorf("cannot create sweep tx: %v", err)
	}

	// Publish sweep tx.
	log.Debugf("Publishing sweep tx %v", tx.TxHash())
	err = s.cfg.PublishTransaction(tx)
	if err != nil {
		return nil, err
	}

	return tx, nil
}

// registerSpendNtfn registers a spend notification with the chain notifier. It
// returns a cancel function that can be used to cancel the registration.
func (s *UtxoSweeper) registerSpendNtfn(input Input) (func(), error) {
	outpoint := input.OutPoint()

	log.Debugf("Wait for spend of %v", outpoint)

	spendEvent, err := s.cfg.Notifier.RegisterSpendNtfn(
		outpoint, input.SignDesc().Output.PkScript, input.HeightHint(),
	)
	if err != nil {
		return nil, err
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		select {
		case spend, ok := <-spendEvent.Spend:
			if !ok {
				return
			}

			select {
			case s.spendChan <- spend:
				log.Debugf("Delivering spend ntfn for %v",
					outpoint)

			case <-s.quit:
			}
		case <-s.quit:
		}
	}()

	return spendEvent.Cancel, nil
}

// CreateSweepTx accepts a list of inputs and signs and generates a txn that
// spends from them. This method also makes an accurate fee estimate before
// generating the required witnesses.
//
// The created transaction has a single output sending all the funds back to
// the source wallet, after accounting for the fee estimate.
//
// The value of currentBlockHeight argument will be set as the tx locktime.
// This function assumes that all CLTV inputs will be unlocked after
// currentBlockHeight. Reasons not to use the maximum of all actual CLTV expiry
// values of the inputs:
//
// - Make handling re-orgs easier.
// - Thwart future possible fee sniping attempts.
// - Make us blend in with the bitcoind wallet.
func (s *UtxoSweeper) CreateSweepTx(inputs []Input, confTarget uint32,
	currentBlockHeight uint32) (*wire.MsgTx, error) {

	feePerKw, err := s.cfg.Estimator.EstimateFeePerKW(confTarget)
	if err != nil {
		return nil, err
	}

	return s.createSweepTx(inputs, confTarget, currentBlockHeight, feePerKw)
}

func (s *UtxoSweeper) createSweepTx(inputs []Input, confTarget uint32,
	currentBlockHeight uint32, feePerKw lnwallet.SatPerKWeight) (
	*wire.MsgTx, error) {

	inputs, txWeight, csvCount, cltvCount := getWeightEstimate(inputs)
	log.Infof("Creating sweep transaction for %v inputs (%v CSV, %v CLTV) "+
		"using %v sat/kw", len(inputs), csvCount, cltvCount,
		int64(feePerKw))

	// Using the txn weight estimate, compute the required txn fee.
	txFee := feePerKw.FeeForWeight(txWeight)

	// Generate the receiving script to which the funds will be swept.
	pkScript, err := s.cfg.GenSweepScript()
	if err != nil {
		return nil, err
	}

	// Sum up the total value contained in the inputs.
	var totalSum int64
	for _, o := range inputs {
		totalSum += o.SignDesc().Output.Value
	}

	// Sweep as much possible, after subtracting txn fees.
	sweepAmt := totalSum - int64(txFee)

	// Create the sweep transaction that we will be building. We use
	// version 2 as it is required for CSV. The txn will sweep the amount
	// after fees to the pkscript generated above.
	sweepTx := wire.NewMsgTx(2)
	sweepTx.AddTxOut(&wire.TxOut{
		PkScript: pkScript,
		Value:    sweepAmt,
	})

	sweepTx.LockTime = currentBlockHeight

	// Add all inputs to the sweep transaction. Ensure that for each
	// csvInput, we set the sequence number properly.
	for _, input := range inputs {
		sweepTx.AddTxIn(&wire.TxIn{
			PreviousOutPoint: *input.OutPoint(),
			Sequence:         input.BlocksToMaturity(),
		})
	}

	// Before signing the transaction, check to ensure that it meets some
	// basic validity requirements.
	//
	// TODO(conner): add more control to sanity checks, allowing us to
	// delay spending "problem" outputs, e.g. possibly batching with other
	// classes if fees are too low.
	btx := btcutil.NewTx(sweepTx)
	if err := blockchain.CheckTransactionSanity(btx); err != nil {
		return nil, err
	}

	hashCache := txscript.NewTxSigHashes(sweepTx)

	// With all the inputs in place, use each output's unique witness
	// function to generate the final witness required for spending.
	addWitness := func(idx int, tso Input) error {
		witness, err := tso.BuildWitness(
			s.cfg.Signer, sweepTx, hashCache, idx,
		)
		if err != nil {
			return err
		}

		sweepTx.TxIn[idx].Witness = witness

		return nil
	}

	// Finally we'll attach a valid witness to each csv and cltv input
	// within the sweeping transaction.
	for i, input := range inputs {
		if err := addWitness(i, input); err != nil {
			return nil, err
		}
	}

	return sweepTx, nil
}

func getInputWitnessSizeUpperBound(input Input) (int, error) {
	switch input.WitnessType() {

	// Outputs on a remote commitment transaction that pay directly
	// to us.
	case lnwallet.CommitmentNoDelay:
		return lnwallet.P2WKHWitnessSize, nil

	// Outputs on a past commitment transaction that pay directly
	// to us.
	case lnwallet.CommitmentTimeLock:
		return lnwallet.ToLocalTimeoutWitnessSize, nil

	// Outgoing second layer HTLC's that have confirmed within the
	// chain, and the output they produced is now mature enough to
	// sweep.
	case lnwallet.HtlcOfferedTimeoutSecondLevel:
		return lnwallet.ToLocalTimeoutWitnessSize, nil

	// Incoming second layer HTLC's that have confirmed within the
	// chain, and the output they produced is now mature enough to
	// sweep.
	case lnwallet.HtlcAcceptedSuccessSecondLevel:
		return lnwallet.ToLocalTimeoutWitnessSize, nil

	// An HTLC on the commitment transaction of the remote party,
	// that has had its absolute timelock expire.
	case lnwallet.HtlcOfferedRemoteTimeout:
		return lnwallet.AcceptedHtlcTimeoutWitnessSize, nil

	// An HTLC on the commitment transaction of the remote party,
	// that can be swept with the preimage.
	case lnwallet.HtlcAcceptedRemoteSuccess:
		return lnwallet.OfferedHtlcSuccessWitnessSize, nil
	}

	return 0, fmt.Errorf("unexpected witness type: %v", input.WitnessType())
}

// getWeightEstimate returns a weight estimate for the given inputs.
// Additionally, it returns counts for the number of csv and cltv inputs.
func getWeightEstimate(inputs []Input) ([]Input, int64, int, int) {
	// We initialize a weight estimator so we can accurately asses the
	// amount of fees we need to pay for this sweep transaction.
	//
	// TODO(roasbeef): can be more intelligent about buffering outputs to
	// be more efficient on-chain.
	var weightEstimate lnwallet.TxWeightEstimator

	// Our sweep transaction will pay to a single segwit p2wkh address,
	// ensure it contributes to our weight estimate.
	weightEstimate.AddP2WKHOutput()

	// For each output, use its witness type to determine the estimate
	// weight of its witness, and add it to the proper set of spendable
	// outputs.
	var (
		sweepInputs         []Input
		csvCount, cltvCount int
	)
	for i := range inputs {
		input := inputs[i]

		size, err := getInputWitnessSizeUpperBound(input)
		if err != nil {
			log.Warn(err)

			// Skip inputs for which no weight estimate can be
			// given.
			continue
		}
		weightEstimate.AddWitnessInput(size)

		switch input.WitnessType() {
		case lnwallet.CommitmentTimeLock,
			lnwallet.HtlcOfferedTimeoutSecondLevel,
			lnwallet.HtlcAcceptedSuccessSecondLevel:
			csvCount++
		case lnwallet.HtlcOfferedRemoteTimeout:
			cltvCount++
		}
		sweepInputs = append(sweepInputs, input)
	}

	txWeight := int64(weightEstimate.Weight())

	return sweepInputs, txWeight, csvCount, cltvCount
}
