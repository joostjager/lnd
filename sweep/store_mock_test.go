package sweep

import (
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
)

type mockSweeperStore struct {
	lastTx            *wire.MsgTx
	ourTxes           map[chainhash.Hash]struct{}
	unconfirmedInputs map[wire.OutPoint]*wire.MsgTx
	unconfirmedTxes   map[chainhash.Hash]*wire.MsgTx
}

func newMockSweeperStore() *mockSweeperStore {
	return &mockSweeperStore{
		unconfirmedInputs: make(map[wire.OutPoint]*wire.MsgTx),
		ourTxes:           make(map[chainhash.Hash]struct{}),
		unconfirmedTxes:   make(map[chainhash.Hash]*wire.MsgTx),
	}
}

func (s *mockSweeperStore) GetUnconfirmedTxes() ([]*wire.MsgTx, error) {
	var txes []*wire.MsgTx
	for _, tx := range s.unconfirmedTxes {
		txes = append(txes, tx)
	}
	return txes, nil
}

func (s *mockSweeperStore) IsUnconfirmedOutput(outpoint wire.OutPoint) bool {
	_, ok := s.unconfirmedInputs[outpoint]
	return ok
}

func (s *mockSweeperStore) IsOurTx(hash chainhash.Hash) bool {
	_, ok := s.ourTxes[hash]
	return ok
}

func (s *mockSweeperStore) NotifyTxPublished(tx *wire.MsgTx) error {
	txHash := tx.TxHash()
	s.unconfirmedTxes[txHash] = tx
	s.ourTxes[txHash] = struct{}{}

	return nil
}

func (s *mockSweeperStore) NotifyTxAccepted(tx *wire.MsgTx) error {
	txHash := tx.TxHash()

	testLog.Infof("mockStore notify tx accepted: %v", txHash)
	s.removeDoubleSpends(tx)

	for _, acceptedIn := range tx.TxIn {
		s.unconfirmedInputs[acceptedIn.PreviousOutPoint] = tx
	}
	s.unconfirmedTxes[txHash] = tx

	return nil
}

func (s *mockSweeperStore) NotifyTxConfirmed(tx *wire.MsgTx) error {
	return s.removeDoubleSpends(tx)
}

func (s *mockSweeperStore) removeDoubleSpends(tx *wire.MsgTx) error {
	for _, confirmedIn := range tx.TxIn {
		confirmedOutpoint := confirmedIn.PreviousOutPoint

		// Check for an unconfirmed tx spending this outpoint.
		unconfirmedTx, ok := s.unconfirmedInputs[confirmedOutpoint]
		if !ok {
			continue
		}

		// Inputs of this tx are either spent or no longer part of an
		// unconfirmed tx.
		for _, in := range unconfirmedTx.TxIn {
			delete(s.unconfirmedInputs, in.PreviousOutPoint)
		}

		delete(s.unconfirmedTxes, unconfirmedTx.TxHash())
	}

	return nil
}

// Compile-time constraint to ensure mockSweeperStore implements SweeperStore.
var _ SweeperStore = (*mockSweeperStore)(nil)
