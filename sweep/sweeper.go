package sweep

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/btcsuite/btcd/wire"
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

	testSpendChan chan wire.OutPoint

	quit chan struct{}
	wg   sync.WaitGroup
}

// UtxoSweeperConfig contains dependencies of UtxoSweeper.
type UtxoSweeperConfig struct {
	// PublishTransaction facilitates the process of broadcasting a signed
	// transaction to the appropriate network.
	PublishTransaction func(*wire.MsgTx) error

	// NewBatchTimer creates a channel that will be sent on when a certain
	// time window has passed. During this time window, new inputs can still
	// be added to the sweep tx that is about to be generated.
	NewBatchTimer func() <-chan time.Time

	// Notifier is an instance of a chain notifier we'll use to watch for
	// certain on-chain events.
	Notifier chainntnfs.ChainNotifier

	// ChainIO is used  to determine the current block height.
	ChainIO lnwallet.BlockChainIO

	// Store stores the published sweeper txes.
	Store SweeperStore

	// TxGenerator generates a set of sweep txes given a list of inputs.
	TxGenerator *TxGenerator

	// SweepTxConfTarget assigns a confirmation target for sweep txes on
	// which the fee calculation will be based.
	SweepTxConfTarget uint32
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

// pendingInput is created when an input reaches the main loop for the first
// time. It tracks all relevant state that is needed for sweeping.
type pendingInput struct {
	// listeners is a list of channels over which the final outcome of the
	// sweep needs to be broadcasted.
	listeners []chan Result

	// input is the original struct that contains the input and sign
	// descriptor.
	input Input

	// ntfnRegCancel is populated with a function that cancels the chain
	// notifier spend registration.
	ntfnRegCancel func()

	// minPublishHeight indicates the minimum block height at which this
	// input may be (re)published.
	minPublishHeight int32

	// publishAttempts records the number of attempts that have already been
	// made to sweep this tx.
	publishAttempts int
}

// New returns a new Sweeper instance.
func New(cfg *UtxoSweeperConfig) *UtxoSweeper {

	return &UtxoSweeper{
		cfg:           cfg,
		newInputs:     make(chan *sweepInputMessage),
		spendChan:     make(chan *chainntnfs.SpendDetail),
		quit:          make(chan struct{}),
		pendingInputs: make(map[wire.OutPoint]*pendingInput),
	}
}

// Start starts the process of constructing and publish sweep txes.
func (s *UtxoSweeper) Start() error {
	if !atomic.CompareAndSwapUint32(&s.started, 0, 1) {
		return nil

	}

	log.Tracef("Sweeper starting")

	// TODO: Republish last tx? No matter what the result is, the output address should
	// be considered used.

	/*
		txes, err := s.cfg.Store.GetUnconfirmedTxes()
		if err != nil {
			return err
		}

		// Republish all txes on startup, in case lnd crashed during the
		// previous publication attempt.
		for _, tx := range txes {
			err := s.cfg.PublishTransaction(tx)

			// Double spends are expected, because wea already published
			// these txes on a previous run.
			if err != nil && err != lnwallet.ErrDoubleSpend {
				return err
			}

			for _, in := range tx.TxIn {
				// Start watching for spend of this input, either by us
				// or the remote party.
				//
				// TODO: Pass in script and heighthint!
				cancel, err := s.registerSpendNtfn(
					in.PreviousOutPoint, nil, 0)
				if err != nil {
					return fmt.Errorf("cannot watch for spend: %v",
						err)
				}

				pendInput := &pendingInput{
					ntfnRegCancel: cancel,
					errored:       err != nil,
				}
				s.pendingInputs[in.PreviousOutPoint] = pendInput
			}
		}*/

	s.wg.Add(1)
	go func() {
		err := s.collector()
		if err != nil {
			log.Errorf("Sweeper stopped: %v", err)
		}
	}()

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
func (s *UtxoSweeper) collector() error {
	defer s.wg.Done()

	bestHash, bestHeight, err := s.cfg.ChainIO.GetBestBlock()
	if err != nil {
		return err
	}

	blockEpochs, err := s.cfg.Notifier.RegisterBlockEpochNtfn(&chainntnfs.BlockEpoch{
		Height: bestHeight,
		Hash:   bestHash,
	})
	if err != nil {
		return err
	}
	defer blockEpochs.Cancel()

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
				listeners:        []chan Result{input.resultChan},
				input:            input.input,
				minPublishHeight: bestHeight,
			}
			s.pendingInputs[outpoint] = pendInput

			// Start watching for spend of this input, either by us
			// or the remote party. The possibility of a remote
			// spend is the reason why just registering for conf. of
			// our sweep tx isn't enough.
			cancel, err := s.registerSpendNtfn(
				outpoint,
				input.input.SignDesc().Output.PkScript,
				input.input.HeightHint(),
			)
			if err != nil {
				err = fmt.Errorf(
					"cannot watch for spend: %v",
					err)

				s.signalAndRemove(&outpoint,
					Result{Err: err})

				continue
			}
			pendInput.ntfnRegCancel = cancel

			// Check to see if with this new input a sweep tx can be
			// formed.
			s.scheduleSweep(bestHeight)

		case spend := <-s.spendChan:
			// For testing
			if s.testSpendChan != nil {
				s.testSpendChan <- *spend.SpentOutPoint
			}

			isOurTx := s.cfg.Store.IsOurTx(*spend.SpenderTxHash)

			// Signal sweep results for inputs in this confirmed tx.
			for _, txIn := range spend.SpendingTx.TxIn {
				outpoint := txIn.PreviousOutPoint

				// Check if this input is known to us. It could
				// probably be unknown if we canceled the
				// registration, deleted from pendingInputs but
				// the ntfn was in-flight already. Or this could
				// be not one of our inputs.
				_, ok := s.pendingInputs[outpoint]
				if !ok {
					continue
				}

				var err error
				if !isOurTx {
					err = ErrRemoteSpend
				}

				// Signal result channels sweep result.
				s.signalAndRemove(&outpoint, Result{
					Tx:  spend.SpendingTx,
					Err: err,
				})
			}

			// Don't need to schedule a sweep, because spend ntfns
			// coincide with block epochs.

		case <-s.timer:
			log.Debugf("Timer expired")

			// Set timer to nil so we know that a new timer needs to
			// be started when new inputs arrive.
			s.timer = nil

			// Examine pending inputs and try to construct one or
			// more sweep txes.
			err := s.sweepAll(bestHeight)
			if err != nil {
				log.Errorf("Cannot sweep on timer event: %v",
					err)
			}
		case epoch := <-blockEpochs.Epochs:
			bestHeight = epoch.Height
			s.scheduleSweep(bestHeight)
		case <-s.quit:
			return nil
		}

		// TODO: Handle the case where a tx is not confirmed within
		// specified time and fee needs to be bumped.

		// TODO: Periodically refresh fee rate and see if it unlocks any
		// inputs that then have a positive yield.
	}
}

// scheduleSweep starts the sweep timer to create an opportunity for more inputs
// to be added.
func (s *UtxoSweeper) scheduleSweep(currentHeight int32) {
	sweepableInputs := func() bool {
		for _, input := range s.pendingInputs {
			if input.minPublishHeight <= currentHeight {
				return true
			}
		}
		return false
	}

	if !sweepableInputs() {
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
func (s *UtxoSweeper) sweepAll(currentHeight int32) error {
	// TODO: Separate zero-attempt inputs from retries.

	// Filter for inputs that need to be swept.
	var sweepableInputs []Input
	for _, input := range s.pendingInputs {
		// Skip inputs that have a minimum publish height that is not
		// yet reached.
		if input.minPublishHeight > currentHeight {
			continue
		}

		sweepableInputs = append(sweepableInputs, input.input)
	}

	for len(sweepableInputs) > 0 {
		var (
			err error
			tx  *wire.MsgTx
		)
		tx, sweepableInputs, err = s.cfg.TxGenerator.Generate(sweepableInputs,
			s.cfg.SweepTxConfTarget, currentHeight)
		if err != nil {
			return err
		}

		if tx == nil {
			break
		}

		// Add tx before publication, so that we will always know that a
		// spend by this tx is ours.
		s.cfg.Store.NotifyPublishTx(tx)

		// Publish sweep tx.
		log.Debugf("Publishing sweep tx %v", tx.TxHash())
		err = s.cfg.PublishTransaction(tx)
		if err != nil {
			log.Error(err)
		}

		// Reschedule sweep.
		for _, input := range tx.TxIn {
			pi := s.pendingInputs[input.PreviousOutPoint]

			var nextAttemptDelta int32
			switch err {
			// Successful publish, but it may still not confirm. Retry later.
			case nil:
				nextAttemptDelta = 6

			// In case of a double spend, retry the next block.
			case lnwallet.ErrDoubleSpend:
				nextAttemptDelta = 1

			// For other errors, back off exponentially.
			default:
				nextAttemptDelta = 1 << uint(pi.publishAttempts)
			}

			pi.minPublishHeight = currentHeight + nextAttemptDelta
			pi.publishAttempts++
		}
	}

	return nil
}

// registerSpendNtfn registers a spend notification with the chain notifier. It
// returns a cancel function that can be used to cancel the registration.
func (s *UtxoSweeper) registerSpendNtfn(outpoint wire.OutPoint,
	script []byte, heightHint uint32) (func(), error) {

	log.Debugf("Wait for spend of %v", outpoint)

	spendEvent, err := s.cfg.Notifier.RegisterSpendNtfn(
		&outpoint, script, heightHint,
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
