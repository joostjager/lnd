package postgres

import (
	"context"

	"github.com/btcsuite/btcwallet/walletdb"
	"github.com/jackc/pgx/v4"
)

// readWriteTx holds a reference to an open postgres transaction.
type readWriteTx struct {
	readOnly bool
	db       *db
	tx       pgx.Tx

	// onCommit gets called upon commit.
	onCommit func()

	// active is true if the transaction hasn't been committed yet.
	active bool
}

// newReadWriteTx creates an rw transaction using a connection from the
// specified pool.
func newReadWriteTx(db *db, readOnly bool) (*readWriteTx, error) {
	// Obtain the global lock instance. An alternative here is to obtain a
	// database lock from Postgres. Unfortunately there is no database-level
	// lock in Postgres, meaning that each table would need to be locked
	// individually. Perhaps an advisory lock could perform this function
	// too.
	if readOnly {
		db.lock.RLock()
	} else {
		db.lock.Lock()
	}

	ctx := context.TODO()
	tx, err := db.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}

	return &readWriteTx{
		db:       db,
		tx:       tx,
		active:   true,
		readOnly: readOnly,
	}, nil
}

// rooBucket is a helper function to return the always present
// pseudo root bucket.
func rootBucket(tx *readWriteTx) *readWriteBucket {
	return newReadWriteBucket(tx, nil)
}

// ReadBucket opens the root bucket for read only access.  If the bucket
// described by the key does not exist, nil is returned.
func (tx *readWriteTx) ReadBucket(key []byte) walletdb.ReadBucket {
	return rootBucket(tx).NestedReadWriteBucket(key)
}

// ForEachBucket iterates through all top level buckets.
func (tx *readWriteTx) ForEachBucket(fn func(key []byte) error) error {
	root := rootBucket(tx)
	// We can safely use ForEach here since on the top level there are
	// no values, only buckets.
	return root.ForEach(func(key []byte, val []byte) error {
		if val != nil {
			// A non-nil value would mean that we have a non
			// walletdb/kvdb compatibel database containing
			// arbitrary key/values.
			return walletdb.ErrInvalid
		}

		return fn(key)
	})
}

// Rollback closes the transaction, discarding changes (if any) if the
// database was modified by a write transaction.
func (tx *readWriteTx) Rollback() error {
	// If the transaction has been closed roolback will fail.
	if !tx.active {
		return walletdb.ErrTxClosed
	}

	err := tx.tx.Rollback(context.TODO())
	if err != nil {
		return err
	}
	tx.active = false

	if tx.readOnly {
		tx.db.lock.RUnlock()
	} else {
		tx.db.lock.Unlock()
	}

	return nil
}

// ReadWriteBucket opens the root bucket for read/write access.  If the
// bucket described by the key does not exist, nil is returned.
func (tx *readWriteTx) ReadWriteBucket(key []byte) walletdb.ReadWriteBucket {
	return rootBucket(tx).NestedReadWriteBucket(key)
}

// CreateTopLevelBucket creates the top level bucket for a key if it
// does not exist.  The newly-created bucket it returned.
func (tx *readWriteTx) CreateTopLevelBucket(key []byte) (walletdb.ReadWriteBucket, error) {
	return rootBucket(tx).CreateBucketIfNotExists(key)
}

// DeleteTopLevelBucket deletes the top level bucket for a key.  This
// errors if the bucket can not be found or the key keys a single value
// instead of a bucket.
func (tx *readWriteTx) DeleteTopLevelBucket(key []byte) error {
	return rootBucket(tx).DeleteNestedBucket(key)
}

// Commit commits the transaction if not already committed.
func (tx *readWriteTx) Commit() error {
	// Commit will fail if the transaction is already committed.
	if !tx.active {
		return walletdb.ErrTxClosed
	}

	// Try committing the transaction.
	if err := tx.tx.Commit(context.TODO()); err != nil {
		return err
	}

	if tx.onCommit != nil {
		tx.onCommit()
	}

	if tx.readOnly {
		tx.db.lock.RUnlock()
	} else {
		tx.db.lock.Unlock()
	}

	// Mark the transaction as not active after commit.
	tx.active = false

	return nil
}

// OnCommit sets the commit callback (overriding if already set).
func (tx *readWriteTx) OnCommit(cb func()) {
	tx.onCommit = cb
}
