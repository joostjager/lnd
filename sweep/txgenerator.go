package sweep

import (
	"fmt"
	"github.com/btcsuite/btcwallet/wallet/txrules"
	"sort"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
	"github.com/lightningnetwork/lnd/lnwallet"
)

// TxGenerator generates sets of sweep txes.
type TxGenerator struct {
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

	// MaxInputsPerTx specifies the  maximum number of inputs allowed in a
	// single sweep tx. If more need to be swept, multiple txes are created
	// and published.
	MaxInputsPerTx int
}

type inputWithYield struct {
}

// Generate goes through all given inputs and constructs sweep transactions,
// each up to the configured maximum number of inputs. Negative yield inputs are
// skipped. Transactions with an output below the dust limit are not published.
// Those inputs remain pending and will be bundled with future inputs if
// possible.
//
// Returns a boolean indicating whether there are sweepable inputs remaining.
// This can happen when fullOnly is true and there are less sweepable inputs
// then the configured maximum number per tx.
func (s *TxGenerator) Generate(sweepableInputs []Input, confTarget uint32,
	currentHeight int32) (*wire.MsgTx, []Input, error) {

	// Calculate dust limit based on the P2WPKH output script of the sweep
	// txes.

	dustLimit := txrules.GetDustThreshold(
		lnwallet.P2WPKHSize,
		btcutil.Amount(s.Estimator.RelayFeePerKW().FeePerKVByte()))

	// Retrieve fee estimate for input filtering and final tx fee
	// calculation.
	satPerKW, err := s.Estimator.EstimateFeePerKW(
		confTarget)
	if err != nil {
		return nil, nil, err
	}

	log.Tracef("Sweepable inputs count: %v", len(sweepableInputs))

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

	yields := make(map[wire.OutPoint]int64)
	for _, input := range sweepableInputs {
		size, err := getInputWitnessSizeUpperBound(input)
		if err != nil {
			return nil, nil, fmt.Errorf(
				"failed adding input weight: %v", err)
		}

		yields[*input.OutPoint()] = input.SignDesc().Output.Value -
			int64(satPerKW.FeeForWeight(int64(size)))
	}

	sort.Slice(sweepableInputs, func(i, j int) bool {
		return yields[*sweepableInputs[i].OutPoint()] >
			yields[*sweepableInputs[j].OutPoint()]
	})

	var weightEstimate lnwallet.TxWeightEstimator

	// Add the sweep tx output to the weight estimate.
	weightEstimate.AddP2WKHOutput()

	// Select inputs up to the configured maximum number.
	var inputList []Input
	var total, outputValue int64
	for len(sweepableInputs) > 0 {
		input := sweepableInputs[0]
		sweepableInputs = sweepableInputs[1:]

		// Can ignore error, because
		size, _ := getInputWitnessSizeUpperBound(input)

		weightEstimate.AddWitnessInput(size)

		newTotal := total +
			input.SignDesc().Output.Value

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
			sweepableInputs = nil
			break
		}

		inputList = append(inputList, input)
		total = newTotal
		outputValue = newOutputValue
		if len(inputList) >= s.MaxInputsPerTx {
			break
		}
	}

	// We can get an empty input list if all of the inputs had a
	// negative yield. In that case, we can stop here.
	if len(inputList) == 0 {
		return nil, nil, nil
	}

	// If the output value of this block of inputs does not reach
	// the dust limit, stop sweeping. Because of the sorting,
	// continuing with remaining inputs will only lead to
	// transactions with a even lower output value.
	if outputValue < int64(dustLimit) {
		log.Debugf("Tx output value %v below dust limit of %v",
			outputValue, dustLimit)
		return nil, nil, nil
	}

	tx, err := s.createSweepTx(inputList, confTarget,
		uint32(currentHeight), satPerKW)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot create sweep tx: %v", err)
	}

	return tx, sweepableInputs, nil
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
func (s *TxGenerator) CreateSweepTx(inputs []Input, confTarget uint32,
	currentBlockHeight uint32) (*wire.MsgTx, error) {

	feePerKw, err := s.Estimator.EstimateFeePerKW(confTarget)
	if err != nil {
		return nil, err
	}

	return s.createSweepTx(inputs, confTarget, currentBlockHeight, feePerKw)
}

func (s *TxGenerator) createSweepTx(inputs []Input, confTarget uint32,
	currentBlockHeight uint32, feePerKw lnwallet.SatPerKWeight) (
	*wire.MsgTx, error) {

	inputs, txWeight, csvCount, cltvCount := getWeightEstimate(inputs)
	log.Infof("Creating sweep transaction for %v inputs (%v CSV, %v CLTV) "+
		"using %v sat/kw", len(inputs), csvCount, cltvCount,
		int64(feePerKw))

	// Using the txn weight estimate, compute the required txn fee.
	txFee := feePerKw.FeeForWeight(txWeight)

	// Generate the receiving script to which the funds will be swept.
	pkScript, err := s.GenSweepScript()
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
			s.Signer, sweepTx, hashCache, idx,
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
