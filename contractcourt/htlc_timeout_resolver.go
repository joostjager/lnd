package contractcourt

import (
	"encoding/binary"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/lightningnetwork/lnd/chainntnfs"
	"github.com/lightningnetwork/lnd/input"
	"io"

	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/lnwallet"
	"github.com/lightningnetwork/lnd/lnwire"
)

// htlcTimeoutResolver is a ContractResolver that's capable of resolving an
// outgoing HTLC. The HTLC may be on our commitment transaction, or on the
// commitment transaction of the remote party. An output on our commitment
// transaction is considered fully resolved once the second-level transaction
// has been confirmed (and reached a sufficient depth). An output on the
// commitment transaction of the remote party is resolved once we detect a
// spend of the direct HTLC output using the timeout clause.
type htlcTimeoutResolver struct {
	// htlcResolution contains all the information required to properly
	// resolve this outgoing HTLC.
	htlcResolution lnwallet.OutgoingHtlcResolution

	// outputIncubating returns true if we've sent the output to the output
	// incubator (utxo nursery).
	outputIncubating bool

	// resolved reflects if the contract has been fully resolved or not.
	resolved bool

	// broadcastHeight is the height that the original contract was
	// broadcast to the main-chain at. We'll use this value to bound any
	// historical queries to the chain for spends/confirmations.
	//
	// TODO(roasbeef): wrap above into definite resolution embedding?
	broadcastHeight uint32

	// htlcIndex is the index of this HTLC within the trace of the
	// additional commitment state machine.
	htlcIndex uint64

	// htlcAmt is the original amount of the htlc, not taking into
	// account any fees that may have to be paid if it goes on chain.
	htlcAmt lnwire.MilliSatoshi

	ResolverKit
}

// ResolverKey returns an identifier which should be globally unique for this
// particular resolver within the chain the original contract resides within.
//
// NOTE: Part of the ContractResolver interface.
func (h *htlcTimeoutResolver) ResolverKey() []byte {
	// The primary key for this resolver will be the outpoint of the HTLC
	// on the commitment transaction itself. If this is our commitment,
	// then the output can be found within the signed timeout tx,
	// otherwise, it's just the ClaimOutpoint.
	var op wire.OutPoint
	if h.htlcResolution.SignedTimeoutTx != nil {
		op = h.htlcResolution.SignedTimeoutTx.TxIn[0].PreviousOutPoint
	} else {
		op = h.htlcResolution.ClaimOutpoint
	}

	key := newResolverID(op)
	return key[:]
}

type outgoingState uint8

const (
	cltvWait uint8 = iota
	secondLevelConf
	csvWait
	sweepWait
)

// Resolve commences the resolution of this contract. As this contract hasn't
// yet timed out, we'll wait for one of two things to happen
//
//   1. The HTLC expires. In this case, we'll sweep the funds and send a clean
//      up cancel message to outside sub-systems.
//
//   2. The remote party sweeps this HTLC on-chain, in which case we'll add the
//      pre-image to our global cache, then send a clean up settle message
//      backwards.
//
// When either of these two things happens, we'll create a new resolver which
// is able to handle the final resolution of the contract. We're only the pivot
// point.
func (h *htlcTimeoutResolver) Resolve() (ContractResolver, error) {
	// If we're already full resolved, then we don't have anything further
	// to do.
	if h.resolved {
		return nil, nil
	}

	// claimCleanUp is a helper function that's called once the HTLC output
	// is spent by the remote party. It'll extract the preimage, add it to
	// the global cache, and finally send the appropriate clean up message.
	claimCleanUp := func(commitSpend *chainntnfs.SpendDetail) (ContractResolver, error) {
		// Depending on if this is our commitment or not, then we'll be
		// looking for a different witness pattern.
		spenderIndex := commitSpend.SpenderInputIndex
		spendingInput := commitSpend.SpendingTx.TxIn[spenderIndex]

		log.Infof("%T(%v): extracting preimage! remote party spent "+
			"HTLC with tx=%v", h, h.htlcResolution.ClaimOutpoint,
			spew.Sdump(commitSpend.SpendingTx))

		// If this is the remote party's commitment, then we'll be
		// looking for them to spend using the second-level success
		// transaction.
		var preimage [32]byte
		if h.htlcResolution.SignedTimeoutTx == nil {
			// The witness stack when the remote party sweeps the
			// output to them looks like:
			//
			//  * <sender sig> <recvr sig> <preimage> <witness script>
			copy(preimage[:], spendingInput.Witness[3])
		} else {
			// Otherwise, they'll be spending directly from our
			// commitment output. In which case the witness stack
			// looks like:
			//
			//  * <sig> <preimage> <witness script>
			copy(preimage[:], spendingInput.Witness[1])
		}

		log.Infof("%T(%v): extracting preimage=%x from on-chain "+
			"spend!", h, h.htlcResolution.ClaimOutpoint, preimage[:])

		// With the preimage obtained, we can now add it to the global
		// cache.
		if err := h.PreimageDB.AddPreimages(preimage); err != nil {
			log.Errorf("%T(%v): unable to add witness to cache",
				h, h.htlcResolution.ClaimOutpoint)
		}

		// Finally, we'll send the clean up message, mark ourselves as
		// resolved, then exit.
		if err := h.DeliverResolutionMsg(ResolutionMsg{
			SourceChan: h.ShortChanID,
			HtlcIndex:  h.htlcIndex,
			PreImage:   &preimage,
		}); err != nil {
			return nil, err
		}
		h.resolved = true
		return nil, h.Checkpoint(h)
	}

	// Otherwise, we'll watch for two external signals to decide if we'll
	// morph into another resolver, or fully resolve the contract.

	// The output we'll be watching for is the *direct* spend from the HTLC
	// output. If this isn't our commitment transaction, it'll be right on
	// the resolution. Otherwise, we fetch this pointer from the input of
	// the time out transaction.
	var (
		outPointToWatch      wire.OutPoint
		scriptToWatch        []byte
		err                  error
		secondLevelSpendChan <-chan *chainntnfs.SpendDetail
	)
	if h.htlcResolution.SignedTimeoutTx == nil {
		outPointToWatch = h.htlcResolution.ClaimOutpoint
		scriptToWatch = h.htlcResolution.SweepSignDesc.Output.PkScript

		// Create dummy channel that will never receive a notification.
		secondLevelSpendChan = make(chan *chainntnfs.SpendDetail)
	} else {
		// If this is the remote party's commitment, then we'll need to
		// grab watch the output that our timeout transaction points
		// to. We can directly grab the outpoint, then also extract the
		// witness script (the last element of the witness stack) to
		// re-construct the pkScipt we need to watch.
		outPointToWatch = h.htlcResolution.SignedTimeoutTx.TxIn[0].PreviousOutPoint
		witness := h.htlcResolution.SignedTimeoutTx.TxIn[0].Witness
		scriptToWatch, err = input.WitnessScriptHash(
			witness[len(witness)-1],
		)
		if err != nil {
			return nil, err
		}

		secondLevelOutPointToWatch := h.htlcResolution.ClaimOutpoint
		secondLevelScriptToWatch := h.htlcResolution.SweepSignDesc.Output.PkScript
		secondLevelSpendNtfn, err := h.Notifier.RegisterSpendNtfn(
			&secondLevelOutPointToWatch, secondLevelScriptToWatch, h.broadcastHeight,
		)
		if err != nil {
			return nil, err
		}
		secondLevelSpendChan = secondLevelSpendNtfn.Spend
	}

	// First, we'll register for a spend notification for this output. If
	// the remote party sweeps with the pre-image, we'll be notified.
	spendNtfn, err := h.Notifier.RegisterSpendNtfn(
		&outPointToWatch, scriptToWatch, h.broadcastHeight,
	)
	if err != nil {
		return nil, err
	}

	// TODO: Cancel spend notifications on quit?

	_, currentHeight, err := h.ChainIO.GetBestBlock()
	if err != nil {
		return nil, err
	}

	state := cltvWait
	var secondLevelExpiry int32

	evaluateHeight := func() {
		switch {
		case state == cltvWait &&
			uint32(currentHeight) >= h.htlcResolution.Expiry-1:

			if h.htlcResolution.SignedTimeoutTx == nil {
				// Start sweep

				state = sweepWait
			} else {
				// Publish timeout tx

				state = secondLevelConf
			}

		case state == csvWait && currentHeight >= secondLevelExpiry-1:
			// Start sweep

			state = sweepWait
		}

	}

	// Evaluate current height in case cltv has already expired.
	evaluateHeight()

	// If we reach this point, then we can't fully act yet, so we'll await
	// either of our signals triggering: the HTLC expires, or we learn of
	// the preimage.
	blockEpochs, err := h.Notifier.RegisterBlockEpochNtfn(nil)
	if err != nil {
		return nil, err
	}
	defer blockEpochs.Cancel()

loop:
	for {
		select {

		// A new block has arrived, we'll check to see if this leads to
		// HTLC expiration.
		case newBlock, ok := <-blockEpochs.Epochs:
			if !ok {
				return nil, fmt.Errorf("quitting")
			}

			// If this new height expires the HTLC, then we can
			// exit early and create a resolver that's capable of
			// handling the time locked output.
			currentHeight = newBlock.Height
			evaluateHeight()

		// The output has been spent! This means the preimage has been
		// revealed on-chain.
		case commitSpend, ok := <-spendNtfn.Spend:
			if !ok {
				return nil, fmt.Errorf("quitting")
			}

			// The only way this output can be spent by the remote
			// party is by revealing the preimage. So we'll perform
			// our duties to clean up the contract once it has been
			// claimed.
			var isRemoteSpend bool
			if h.htlcResolution.SignedTimeoutTx != nil {
				timeoutTxHash := h.htlcResolution.SignedTimeoutTx.TxHash()

				// If spend not by our own timeout tx, it must
				// be a remote spend.
				isRemoteSpend = *commitSpend.SpenderTxHash !=
					timeoutTxHash
			} else {
				// Detect remote success tx by witness length.
				isRemoteSpend = len(commitSpend.SpendingTx.TxIn[0].Witness) == 5
			}

			if isRemoteSpend {
				return claimCleanUp(commitSpend)
			}

			// At this point, the second-level transaction is sufficiently
			// confirmed, or a transaction directly spending the output is.
			// Therefore, we can now send back our clean up message.
			failureMsg := &lnwire.FailPermanentChannelFailure{}
			if err := h.DeliverResolutionMsg(ResolutionMsg{
				SourceChan: h.ShortChanID,
				HtlcIndex:  h.htlcIndex,
				Failure:    failureMsg,
			}); err != nil {
				return nil, err
			}

			if state == secondLevelConf {
				state = csvWait
				secondLevelExpiry = currentHeight +
					int32(h.htlcResolution.CsvDelay)

				evaluateHeight()
			} else {
				break loop
			}

		case _, ok := <-secondLevelSpendChan:
			if !ok {
				return nil, fmt.Errorf("quitting")
			}
			break loop

		case <-h.Quit:
			return nil, fmt.Errorf("resolver cancelled")
		}

	}
	h.resolved = true
	return nil, h.Checkpoint(h)
}

// Stop signals the resolver to cancel any current resolution processes, and
// suspend.
//
// NOTE: Part of the ContractResolver interface.
func (h *htlcTimeoutResolver) Stop() {
	close(h.Quit)
}

// IsResolved returns true if the stored state in the resolve is fully
// resolved. In this case the target output can be forgotten.
//
// NOTE: Part of the ContractResolver interface.
func (h *htlcTimeoutResolver) IsResolved() bool {
	return h.resolved
}

// Encode writes an encoded version of the ContractResolver into the passed
// Writer.
//
// NOTE: Part of the ContractResolver interface.
func (h *htlcTimeoutResolver) Encode(w io.Writer) error {
	// First, we'll write out the relevant fields of the
	// OutgoingHtlcResolution to the writer.
	if err := encodeOutgoingResolution(w, &h.htlcResolution); err != nil {
		return err
	}

	// With that portion written, we can now write out the fields specific
	// to the resolver itself.
	if err := binary.Write(w, endian, h.outputIncubating); err != nil {
		return err
	}
	if err := binary.Write(w, endian, h.resolved); err != nil {
		return err
	}
	if err := binary.Write(w, endian, h.broadcastHeight); err != nil {
		return err
	}

	if err := binary.Write(w, endian, h.htlcIndex); err != nil {
		return err
	}

	return nil
}

// Decode attempts to decode an encoded ContractResolver from the passed Reader
// instance, returning an active ContractResolver instance.
//
// NOTE: Part of the ContractResolver interface.
func (h *htlcTimeoutResolver) Decode(r io.Reader) error {
	// First, we'll read out all the mandatory fields of the
	// OutgoingHtlcResolution that we store.
	if err := decodeOutgoingResolution(r, &h.htlcResolution); err != nil {
		return err
	}

	// With those fields read, we can now read back the fields that are
	// specific to the resolver itself.
	if err := binary.Read(r, endian, &h.outputIncubating); err != nil {
		return err
	}
	if err := binary.Read(r, endian, &h.resolved); err != nil {
		return err
	}
	if err := binary.Read(r, endian, &h.broadcastHeight); err != nil {
		return err
	}

	if err := binary.Read(r, endian, &h.htlcIndex); err != nil {
		return err
	}

	return nil
}

// AttachResolverKit should be called once a resolved is successfully decoded
// from its stored format. This struct delivers a generic tool kit that
// resolvers need to complete their duty.
//
// NOTE: Part of the ContractResolver interface.
func (h *htlcTimeoutResolver) AttachResolverKit(r ResolverKit) {
	h.ResolverKit = r
}

// A compile time assertion to ensure htlcTimeoutResolver meets the
// ContractResolver interface.
var _ ContractResolver = (*htlcTimeoutResolver)(nil)
