package lncfg

import (
	"context"
	"fmt"
	"time"

	"github.com/lightningnetwork/lnd/kvdb"
	"github.com/lightningnetwork/lnd/kvdb/etcd"
	"github.com/lightningnetwork/lnd/kvdb/postgres"
	"github.com/lightningnetwork/lnd/lnwallet/btcwallet"
)

const (
	dbName                     = "channel.db"
	BoltBackend                = "bolt"
	EtcdBackend                = "etcd"
	PostgresBackend            = "postgres"
	DefaultBatchCommitInterval = 500 * time.Millisecond
)

// DB holds database configuration for LND.
type DB struct {
	Backend string `long:"backend" description:"The selected database backend."`

	BatchCommitInterval time.Duration `long:"batch-commit-interval" description:"The maximum duration the channel graph batch schedulers will wait before attempting to commit a batch of pending updates. This can be tradeoff database contenion for commit latency."`

	Etcd *etcd.Config `group:"etcd" namespace:"etcd" description:"Etcd settings."`

	Bolt *kvdb.BoltConfig `group:"bolt" namespace:"bolt" description:"Bolt settings."`

	Postgres *postgres.Config `group:"postgres" namespace:"postgres" description:"Postgres settings."`
}

// DefaultDB creates and returns a new default DB config.
func DefaultDB() *DB {
	return &DB{
		Backend:             BoltBackend,
		BatchCommitInterval: DefaultBatchCommitInterval,
		Bolt: &kvdb.BoltConfig{
			AutoCompactMinAge: kvdb.DefaultBoltAutoCompactMinAge,
			DBTimeout:         kvdb.DefaultDBTimeout,
		},
	}
}

// Validate validates the DB config.
func (db *DB) Validate() error {
	switch db.Backend {
	case BoltBackend:
	case PostgresBackend:
	case EtcdBackend:
		if !db.Etcd.Embedded && db.Etcd.Host == "" {
			return fmt.Errorf("etcd host must be set")
		}

	default:
		return fmt.Errorf("unknown backend, must be either \"%v\" or \"%v\"",
			BoltBackend, EtcdBackend)
	}

	return nil
}

// Init should be called upon start to pre-initialize database access dependent
// on configuration.
func (db *DB) Init(ctx context.Context, dbPath string) error {
	// Start embedded etcd server if requested.
	if db.Backend == EtcdBackend && db.Etcd.Embedded {
		cfg, _, err := kvdb.StartEtcdTestBackend(
			dbPath, db.Etcd.EmbeddedClientPort,
			db.Etcd.EmbeddedPeerPort,
		)
		if err != nil {
			return err
		}

		// Override the original config with the config for
		// the embedded instance.
		db.Etcd = cfg
	}

	return nil
}

// boltBackendCreator is a type alias for a function that returns a bolt DB
// backend with the given bolt specific configuration.
type boltBackendCreator func(boltCfg *kvdb.BoltConfig) (kvdb.Backend, error)

// DatabaseBackends is a two-tuple that holds the set of active database
// backends for the daemon. The two backends we expose are the graph database
// backend, and the channel state backend.
type DatabaseBackends struct {
	// GraphDB points to the database backend that contains the less
	// critical data that is accessed often, such as the channel graph and
	// chain height hints.
	GraphDB kvdb.Backend

	// ChanStateDB points to a possibly networked replicated backend that
	// contains the critical channel state related data.
	ChanStateDB kvdb.Backend

	// MacaroonDB points to a database backend that stores the macaroon root
	// keys.
	MacaroonDB kvdb.Backend

	// DecayedLogDB points to a database backend that stores the decayed log
	// data.
	DecayedLogDB kvdb.Backend

	// TowerClientDB points to a database backend that stores the watchtower
	// client data. This might be nil if the watchtower client is disabled.
	TowerClientDB kvdb.Backend

	// TowerServerDB points to a database backend that stores the watchtower
	// server data. This might be nil if the watchtower server is disabled.
	TowerServerDB kvdb.Backend

	// WalletDB is an option that instructs the wallet loader where to load
	// the underlying wallet database from.
	WalletDB btcwallet.LoaderOption

	// Replicated indicates whether the database backends are remote, data
	// replicated instances or local bbolt backed databases.
	Replicated bool
}

// GetBackends returns a set of kvdb.Backends as set in the DB config.
func (db *DB) GetBackends(ctx context.Context, chanDBPath, walletDBPath string,
	makeMacaroonBoltDB boltBackendCreator,
	makeDecayedLogBoltDB boltBackendCreator,
	makeTowerClientBoltDB boltBackendCreator,
	makeTowerServerBoltDB boltBackendCreator) (
	*DatabaseBackends, error) {

	switch db.Backend {
	case EtcdBackend:
		etcdGraphBackend, err := kvdb.Open(
			kvdb.EtcdBackendName, ctx,
			db.Etcd.CloneWithSubNamespace("graphdb"),
		)
		if err != nil {
			return nil, fmt.Errorf("error opening etcd graph DB: "+
				"%v", err)
		}

		etcdChanStateBackend, err := kvdb.Open(
			kvdb.EtcdBackendName, ctx,
			db.Etcd.CloneWithSubNamespace("chanstatedb"),
		)
		if err != nil {
			return nil, fmt.Errorf("error opening etcd chan state "+
				"DB: %v", err)
		}

		etcdMacaroonBackend, err := kvdb.Open(
			kvdb.EtcdBackendName, ctx,
			db.Etcd.CloneWithSubNamespace("macaroondb"),
		)
		if err != nil {
			return nil, fmt.Errorf("error opening etcd macaroon "+
				"DB: %v", err)
		}

		etcdDecayedLogBackend, err := kvdb.Open(
			kvdb.EtcdBackendName, ctx,
			db.Etcd.CloneWithSubNamespace("decayedlogdb"),
		)
		if err != nil {
			return nil, fmt.Errorf("error opening etcd decayed "+
				"log DB: %v", err)
		}

		etcdTowerClientBackend, err := kvdb.Open(
			kvdb.EtcdBackendName, ctx,
			db.Etcd.CloneWithSubNamespace("towerclientdb"),
		)
		if err != nil {
			return nil, fmt.Errorf("error opening etcd tower "+
				"client DB: %v", err)
		}

		etcdTowerServerBackend, err := kvdb.Open(
			kvdb.EtcdBackendName, ctx,
			db.Etcd.CloneWithSubNamespace("towerserverdb"),
		)
		if err != nil {
			return nil, fmt.Errorf("error opening etcd tower "+
				"server DB: %v", err)
		}

		etcdWalletBackend, err := kvdb.Open(
			kvdb.EtcdBackendName, ctx,
			db.Etcd.CloneWithSubNamespace("walletdb"),
		)
		if err != nil {
			return nil, fmt.Errorf("error opening etcd macaroon "+
				"DB: %v", err)
		}

		return &DatabaseBackends{
			GraphDB:       etcdGraphBackend,
			ChanStateDB:   etcdChanStateBackend,
			MacaroonDB:    etcdMacaroonBackend,
			DecayedLogDB:  etcdDecayedLogBackend,
			TowerClientDB: etcdTowerClientBackend,
			TowerServerDB: etcdTowerServerBackend,
			// The wallet loader will attempt to use/create the
			// wallet in the replicated remote DB if we're running
			// in a clustered environment. This will ensure that all
			// members of the cluster have access to the same wallet
			// state.
			WalletDB: btcwallet.LoaderWithExternalWalletDB(
				etcdWalletBackend,
			),
			Replicated: true,
		}, nil

	case PostgresBackend:
		postgresGraphBackend, err := kvdb.Open(
			kvdb.PostgresBackendName, ctx,
			db.Postgres, "graphdb",
		)
		if err != nil {
			return nil, fmt.Errorf("error opening etcd graph DB: "+
				"%v", err)
		}

		postgresChanStateBackend, err := kvdb.Open(
			kvdb.PostgresBackendName, ctx,
			db.Postgres, "chanstatedb",
		)
		if err != nil {
			return nil, fmt.Errorf("error opening etcd chan state "+
				"DB: %v", err)
		}

		postgresMacaroonBackend, err := kvdb.Open(
			kvdb.PostgresBackendName, ctx,
			db.Postgres, "macaroondb",
		)
		if err != nil {
			return nil, fmt.Errorf("error opening etcd macaroon "+
				"DB: %v", err)
		}

		postgresDecayedLogBackend, err := kvdb.Open(
			kvdb.PostgresBackendName, ctx,
			db.Postgres, "decayedlogdb",
		)
		if err != nil {
			return nil, fmt.Errorf("error opening etcd decayed "+
				"log DB: %v", err)
		}

		postgresTowerClientBackend, err := kvdb.Open(
			kvdb.PostgresBackendName, ctx,
			db.Postgres, "towerclientdb",
		)
		if err != nil {
			return nil, fmt.Errorf("error opening etcd tower "+
				"client DB: %v", err)
		}

		postgresTowerServerBackend, err := kvdb.Open(
			kvdb.PostgresBackendName, ctx,
			db.Postgres, "towerserverdb",
		)
		if err != nil {
			return nil, fmt.Errorf("error opening etcd tower "+
				"server DB: %v", err)
		}

		postgresWalletBackend, err := kvdb.Open(
			kvdb.PostgresBackendName, ctx,
			db.Postgres, "walletdb",
		)
		if err != nil {
			return nil, fmt.Errorf("error opening etcd macaroon "+
				"DB: %v", err)
		}

		return &DatabaseBackends{
			GraphDB:       postgresGraphBackend,
			ChanStateDB:   postgresChanStateBackend,
			MacaroonDB:    postgresMacaroonBackend,
			DecayedLogDB:  postgresDecayedLogBackend,
			TowerClientDB: postgresTowerClientBackend,
			TowerServerDB: postgresTowerServerBackend,
			// The wallet loader will attempt to use/create the
			// wallet in the replicated remote DB if we're running
			// in a clustered environment. This will ensure that all
			// members of the cluster have access to the same wallet
			// state.
			WalletDB: btcwallet.LoaderWithExternalWalletDB(
				postgresWalletBackend,
			),
			Replicated: true,
		}, nil
	}

	// We're using all bbolt based databases by default.
	boltBackend, err := kvdb.GetBoltBackend(&kvdb.BoltBackendConfig{
		DBPath:            chanDBPath,
		DBFileName:        dbName,
		DBTimeout:         db.Bolt.DBTimeout,
		NoFreelistSync:    !db.Bolt.SyncFreelist,
		AutoCompact:       db.Bolt.AutoCompact,
		AutoCompactMinAge: db.Bolt.AutoCompactMinAge,
	})
	if err != nil {
		return nil, fmt.Errorf("error opening bolt DB: %v", err)
	}

	macaroonBackend, err := makeMacaroonBoltDB(db.Bolt)
	if err != nil {
		return nil, fmt.Errorf("error opening macaroon DB: %v", err)
	}

	decayedLogBackend, err := makeDecayedLogBoltDB(db.Bolt)
	if err != nil {
		return nil, fmt.Errorf("error opening decayed log DB: %v", err)
	}

	// This DB could actually be nil if the watchtower client isn't enabled.
	towerClientBackend, err := makeTowerClientBoltDB(db.Bolt)
	if err != nil {
		return nil, fmt.Errorf("error opening tower client DB: %v", err)
	}

	// This DB could actually be nil if the watchtower server isn't enabled.
	towerServerBackend, err := makeTowerServerBoltDB(db.Bolt)
	if err != nil {
		return nil, fmt.Errorf("error opening tower server DB: %v", err)
	}

	return &DatabaseBackends{
		GraphDB:       boltBackend,
		ChanStateDB:   boltBackend,
		MacaroonDB:    macaroonBackend,
		DecayedLogDB:  decayedLogBackend,
		TowerClientDB: towerClientBackend,
		TowerServerDB: towerServerBackend,
		// When "running locally", LND will use the bbolt wallet.db to
		// store the wallet located in the chain data dir, parametrized
		// by the active network.
		WalletDB: btcwallet.LoaderWithLocalWalletDB(
			walletDBPath, !db.Bolt.SyncFreelist, db.Bolt.DBTimeout,
		),
	}, nil

}

// Compile-time constraint to ensure Workers implements the Validator interface.
var _ Validator = (*DB)(nil)
