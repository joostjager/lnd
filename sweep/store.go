package sweep

import (
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/channeldb"
)

// SweeperStore stores published txes.
type SweeperStore interface {
	GetTxes() ([]*wire.MsgTx, error)

	GetSpendingTx(wire.OutPoint) *chainhash.Hash

	RemoveTxByInput(wire.OutPoint) error

	AddTx(*wire.MsgTx) error
}

type sweeperStore struct {
	db *channeldb.DB
}

func newSweeperStore(db *channeldb.DB) (*sweeperStore, error) {

	return &sweeperStore{
		db: db,
	}, nil
}

func (s *sweeperStore) GetTxes() ([]*wire.MsgTx, error) {
	return nil, nil
}

func (s *sweeperStore) RemoveTxByInput(wire.OutPoint) error {
	return nil
}

func (s *sweeperStore) GetSpendingTx(wire.OutPoint) *chainhash.Hash {
	return nil
}

func (s *sweeperStore) AddTx(*wire.MsgTx) error {
	return nil
}

// Compile-time constraint to ensure nurseryStore implements NurseryStore.
var _ SweeperStore = (*sweeperStore)(nil)
