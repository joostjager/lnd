package postgres

import (
	"context"
	"encoding/hex"
	"strings"

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

func toTableName(key []byte) string {
	for _, b := range string(key) {
		if !(b >= 'a' && b <= 'z') &&
			!(b >= '0' && b <= '9') && b != '-' {

			return "kvhex_" + hex.EncodeToString(key)
		}
	}

	table := strings.Replace(string(key), "-", "_", -1)
	return "kv_" + table
}

func fromTableName(table string) []byte {
	if strings.HasPrefix(table, "kv_") {
		table = strings.Replace(table, "_", "-", -1)
		return []byte(table[3:])
	}

	if strings.HasPrefix(table, "kvhex_") {
		key, err := hex.DecodeString(table[6:])
		if err != nil {
			panic(err)
		}
		return key
	}

	panic("invalid table name")
}

// ReadBucket opens the root bucket for read only access.  If the bucket
// described by the key does not exist, nil is returned.
func (tx *readWriteTx) ReadBucket(key []byte) walletdb.ReadBucket {
	bucket := tx.ReadWriteBucket(key)
	if bucket == nil {
		return nil
	}
	return bucket.(walletdb.ReadBucket)
}

// ForEachBucket iterates through all top level buckets.
func (tx *readWriteTx) ForEachBucket(fn func(key []byte) error) error {
	rows, err := tx.tx.Query(
		context.TODO(),
		"SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname='public' ORDER BY tablename",
	)
	if err != nil {
		return err
	}

	for rows.Next() {
		var table string
		err := rows.Scan(&table)
		if err != nil {
			return err
		}

		if !strings.HasPrefix(table, "kv_") && !strings.HasPrefix(table, "kvhex_") {
			continue
		}

		err = fn(fromTableName(table))
		if err != nil {
			return err
		}
	}

	return nil
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
	table := toTableName(key)

	var value int
	err := tx.tx.QueryRow(
		context.TODO(),
		"SELECT 1 FROM pg_catalog.pg_tables WHERE schemaname='public' and tablename=$1",
		table,
	).Scan(&value)

	if err == pgx.ErrNoRows {
		return nil
	}

	return newReadWriteBucket(tx, table, nil)
}

// CreateTopLevelBucket creates the top level bucket for a key if it
// does not exist.  The newly-created bucket it returned.
func (tx *readWriteTx) CreateTopLevelBucket(key []byte) (walletdb.ReadWriteBucket, error) {
	table := toTableName(key)

	_, err := tx.tx.Exec(context.TODO(), `
	
CREATE TABLE IF NOT EXISTS public.`+table+`
(
    key bytea NOT NULL,
    value bytea,
    parent_id bigint,
    id bigserial PRIMARY KEY,
    sequence bigint,
    CONSTRAINT `+table+`_parent FOREIGN KEY (parent_id)
        REFERENCES public.`+table+` (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE
        NOT VALID
);

CREATE INDEX IF NOT EXISTS `+table+`_parent
    ON public.`+table+` USING btree
    (parent_id ASC NULLS LAST);

CREATE UNIQUE INDEX IF NOT EXISTS `+table+`_unique_parent
    ON public.`+table+` USING btree
    (parent_id ASC NULLS LAST, key ASC NULLS LAST) WHERE parent_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS `+table+`_unique_null_parent ON public.`+table+` (key) WHERE parent_id IS NULL;

`)
	if err != nil {
		return nil, err
	}

	return newReadWriteBucket(tx, table, nil), nil
}

// DeleteTopLevelBucket deletes the top level bucket for a key.  This
// errors if the bucket can not be found or the key keys a single value
// instead of a bucket.
func (tx *readWriteTx) DeleteTopLevelBucket(key []byte) error {
	bucket := tx.ReadWriteBucket(key)
	if bucket == nil {
		return walletdb.ErrBucketNotFound
	}

	table := bucket.(*readWriteBucket).table

	_, err := tx.tx.Exec(context.TODO(), "DROP TABLE public."+table+";")
	if err != nil {
		return err
	}

	_, err = tx.tx.Exec(context.TODO(), "DELETE FROM top_sequences WHERE table_name=$1", table)
	if err != nil {
		return err
	}

	return nil
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
