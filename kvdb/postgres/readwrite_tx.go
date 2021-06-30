package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/btcsuite/btcwallet/walletdb"
)

// readWriteTx holds a reference to an open postgres transaction.
type readWriteTx struct {
	readOnly bool
	db       *db
	tx       *sql.Tx

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

	tx, err := db.db.Begin()
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

func hasSpecialChars(s string) bool {
	for _, b := range s {
		if !(b >= 'a' && b <= 'z') &&
			!(b >= '0' && b <= '9') && b != '-' {

			return true
		}
	}

	return false
}

func (tx *readWriteTx) toTableName(key []byte) string {
	// Max table name length in postgres is 63, but keep some slack for
	// index postfixes.
	const maxTableNameLen = 50

	var table string
	if hasSpecialChars(string(key)) {
		return ""
	}

	table = strings.Replace(string(key), "-", "_", -1)
	table = tx.db.getPrefixedTableName(table)

	if len(table) > maxTableNameLen {
		return ""
	}

	return table
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
	return errors.New("not implemented")
}

// Rollback closes the transaction, discarding changes (if any) if the
// database was modified by a write transaction.
func (tx *readWriteTx) Rollback() error {
	// If the transaction has been closed roolback will fail.
	if !tx.active {
		return walletdb.ErrTxClosed
	}

	err := tx.tx.Rollback()
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
	table := tx.toTableName(key)

	// If the key can't be mapped to a table name, open the bucket from the
	// hex table.
	if table == "" {
		bucket := newReadWriteBucket(tx, tx.db.hexTableName, nil)
		return bucket.NestedReadWriteBucket(key)
	}

	var value int
	err := tx.tx.QueryRowContext(
		context.TODO(),
		"SELECT 1 FROM pg_catalog.pg_tables WHERE schemaname='public' and tablename=$1",
		table,
	).Scan(&value)

	if err == sql.ErrNoRows {
		return nil
	}

	return newReadWriteBucket(tx, table, nil)
}

func getCreateTableSql(table string) string {
	return `
CREATE TABLE IF NOT EXISTS public.` + table + `
(
    key bytea NOT NULL,
    value bytea,
    parent_id bigint,
    id bigserial PRIMARY KEY,
    sequence bigint,
    CONSTRAINT ` + table + `_parent FOREIGN KEY (parent_id)
        REFERENCES public.` + table + ` (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE
        NOT VALID
);

CREATE INDEX IF NOT EXISTS ` + table + `_p
    ON public.` + table + ` USING btree
    (parent_id ASC NULLS LAST);

CREATE UNIQUE INDEX IF NOT EXISTS ` + table + `_up
    ON public.` + table + ` USING btree
    (parent_id ASC NULLS LAST, key ASC NULLS LAST) WHERE parent_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ` + table + `_unp ON public.` + table + ` (key) WHERE parent_id IS NULL;
`
}

// CreateTopLevelBucket creates the top level bucket for a key if it
// does not exist.  The newly-created bucket it returned.
func (tx *readWriteTx) CreateTopLevelBucket(key []byte) (walletdb.ReadWriteBucket, error) {
	table := tx.toTableName(key)

	if table == "" {
		bucket := newReadWriteBucket(tx, tx.db.hexTableName, nil)
		return bucket.CreateBucketIfNotExists(key)
	}

	createTableSql := getCreateTableSql(table)
	_, err := tx.tx.ExecContext(context.TODO(), createTableSql)
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

	_, err := tx.tx.ExecContext(context.TODO(), "DROP TABLE public."+table+";")
	if err != nil {
		return err
	}

	_, err = tx.tx.ExecContext(context.TODO(), "DELETE FROM "+tx.db.sequenceTableName+" WHERE table_name=$1", table)
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
	if err := tx.tx.Commit(); err != nil {
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
