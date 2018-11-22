package sweep

import (
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
)

type mockSweeperStore struct {
	txes     map[chainhash.Hash]*wire.MsgTx
	inputMap map[wire.OutPoint]chainhash.Hash
}

func newMockSweeperStore() *mockSweeperStore {
	return &mockSweeperStore{
		txes:     make(map[chainhash.Hash]*wire.MsgTx),
		inputMap: make(map[wire.OutPoint]chainhash.Hash),
	}
}

func (s *mockSweeperStore) GetTxes() ([]*wire.MsgTx, error) {
	testLog.Infof("mockStore get txes")
	txes := make([]*wire.MsgTx, 0, len(s.txes))
	for _, tx := range s.txes {
		txes = append(txes, tx)
	}
	return txes, nil
}

func (s *mockSweeperStore) GetSpendingTx(outpoint wire.OutPoint) *chainhash.Hash {
	tx, ok := s.inputMap[outpoint]
	if !ok {
		return nil
	}
	return &tx
}

func (s *mockSweeperStore) RemoveTxByInput(outpoint wire.OutPoint) error {
	txHash, ok := s.inputMap[outpoint]
	if !ok {
		testLog.Warnf("mockStore input to remove not found: %v", outpoint)
		return nil
	}

	testLog.Infof("mockStore remove tx %v by input %v", txHash, outpoint)
	for _, in := range s.txes[txHash].TxIn {
		delete(s.inputMap, in.PreviousOutPoint)
	}
	delete(s.txes, txHash)

	return nil
}

func (s *mockSweeperStore) AddTx(tx *wire.MsgTx) error {
	txHash := tx.TxHash()

	testLog.Infof("mockStore add tx %v", txHash)

	for _, in := range tx.TxIn {
		s.RemoveTxByInput(in.PreviousOutPoint)
	}

	for _, in := range tx.TxIn {
		s.inputMap[in.PreviousOutPoint] = txHash
	}
	s.txes[txHash] = tx

	return nil
}

// Compile-time constraint to ensure mockSweeperStore implements SweeperStore.
var _ SweeperStore = (*mockSweeperStore)(nil)
