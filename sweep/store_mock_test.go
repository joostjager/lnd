package sweep

import (
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
)

type mockSweeperStore struct {
	lastTx  *wire.MsgTx
	ourTxes map[chainhash.Hash]struct{}
}

func newMockSweeperStore() *mockSweeperStore {
	return &mockSweeperStore{
		ourTxes: make(map[chainhash.Hash]struct{}),
	}
}

func (s *mockSweeperStore) IsOurTx(hash chainhash.Hash) bool {
	_, ok := s.ourTxes[hash]
	return ok
}

func (s *mockSweeperStore) NotifyPublishTx(tx *wire.MsgTx) error {
	txHash := tx.TxHash()
	s.ourTxes[txHash] = struct{}{}

	return nil
}

func (s *mockSweeperStore) GetLastPublishedTx() (*wire.MsgTx, error) {
	return nil, nil
}

// Compile-time constraint to ensure mockSweeperStore implements SweeperStore.
var _ SweeperStore = (*mockSweeperStore)(nil)
