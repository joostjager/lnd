package lncfg

import (
	"context"
	"fmt"
	"time"

	"github.com/lightningnetwork/lnd/kvdb"
	"github.com/lightningnetwork/lnd/kvdb/etcd"
	"github.com/lightningnetwork/lnd/kvdb/postgres"
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

	FullyRemote bool `long:"fully-remote" description:"Don't store anything in local bbolt databases when using an etcd or postgres backend'"`

	BatchCommitInterval time.Duration `long:"batch-commit-interval" description:"The maximum duration the channel graph batch schedulers will wait before attempting to commit a batch of pending updates. This can be tradeoff database contention for commit latency."`

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

type boltBackendCreator func(boltCfg *kvdb.BoltConfig) (kvdb.Backend, error)

// DatabaseBackends is a two-tuple that holds the set of active database
// backends for the daemon. The two backends we expose are the local database
// backend, and the remote backend. The LocalDB attribute will always be
// populated. However, the remote DB will only be set if a replicated database
// is active.
type DatabaseBackends struct {
	// LocalDB points to the local non-replicated backend.
	LocalDB kvdb.Backend

	// RemoteDB points to a possibly networked replicated backend. If no
	// replicated backend is active, then this pointer will be nil.
	RemoteDB kvdb.Backend

	MacaroonDB kvdb.Backend

	TowerClientDB kvdb.Backend

	DecayedLogDB kvdb.Backend
}

// GetBackends returns a set of kvdb.Backends as set in the DB config.  The
// local database will ALWAYS be non-nil, while the remote database will only
// be populated if etcd is specified.
func (db *DB) GetBackends(ctx context.Context, graphPath string,
	fullyRemote bool, macaroonDBCreator,
	decayedLogDBCreator boltBackendCreator) (*DatabaseBackends, error) {

	var (
		backends = &DatabaseBackends{}
		err      error
	)

	switch db.Backend {
	case EtcdBackend:
		backends.RemoteDB, err = kvdb.Open(
			kvdb.EtcdBackendName, ctx, db.Etcd,
		)
		if err != nil {
			return nil, fmt.Errorf("error opening remote DB: %v",
				err)
		}

		if fullyRemote {
			backends.LocalDB, err = kvdb.Open(
				kvdb.EtcdBackendName, ctx, db.Etcd,
			)
			if err != nil {
				return nil, fmt.Errorf("error opening local "+
					"DB: %v", err)
			}

			backends.MacaroonDB, err = kvdb.Open(
				kvdb.EtcdBackendName, ctx, db.Etcd,
			)
			if err != nil {
				return nil, fmt.Errorf("error opening "+
					"macaroon DB: %v", err)
			}

			backends.DecayedLogDB, err = kvdb.Open(
				kvdb.EtcdBackendName, ctx, db.Etcd,
			)
			if err != nil {
				return nil, fmt.Errorf("error opening "+
					"decayed log DB: %v", err)
			}
		}

	case PostgresBackend:
		backends.RemoteDB, err = kvdb.Open(
			kvdb.PostgresBackendName, ctx, db.Postgres,
		)
		if err != nil {
			return nil, fmt.Errorf("error opening remote DB: %v",
				err)
		}

		if fullyRemote {
			backends.LocalDB, err = kvdb.Open(
				kvdb.PostgresBackendName, ctx, db.Postgres,
			)
			if err != nil {
				return nil, fmt.Errorf("error opening local "+
					"DB: %v", err)
			}

			backends.MacaroonDB, err = kvdb.Open(
				kvdb.PostgresBackendName, ctx, db.Postgres,
			)
			if err != nil {
				return nil, fmt.Errorf("error opening "+
					"macaroon DB: %v", err)
			}

			backends.DecayedLogDB, err = kvdb.Open(
				kvdb.PostgresBackendName, ctx, db.Postgres,
			)
			if err != nil {
				return nil, fmt.Errorf("error opening "+
					"decayed log DB: %v", err)
			}
		}
	}

	if !fullyRemote {
		backends.LocalDB, err = kvdb.GetBoltBackend(&kvdb.BoltBackendConfig{
			DBPath:            graphPath,
			DBFileName:        dbName,
			DBTimeout:         db.Bolt.DBTimeout,
			NoFreelistSync:    !db.Bolt.SyncFreelist,
			AutoCompact:       db.Bolt.AutoCompact,
			AutoCompactMinAge: db.Bolt.AutoCompactMinAge,
		})
		if err != nil {
			return nil, fmt.Errorf("error opening local bolt DB: "+
				"%v", err)
		}

		backends.MacaroonDB, err = macaroonDBCreator(db.Bolt)
		if err != nil {
			return nil, fmt.Errorf("error opening macaroon bolt "+
				"DB: %v", err)
		}

		backends.DecayedLogDB, err = decayedLogDBCreator(db.Bolt)
		if err != nil {
			return nil, fmt.Errorf("error opening decayed log "+
				"bolt DB: %v", err)
		}
	}

	return backends, nil
}

// Compile-time constraint to ensure Workers implements the Validator interface.
var _ Validator = (*DB)(nil)
