package postgres

import (
	"bytes"
	"database/sql"
	"sort"
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
	// Fetch human-readable top level buckets.
	rows, err := tx.tx.QueryContext(
		tx.db.ctx,
		"SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname='public'",
	)
	if err != nil {
		return err
	}

	var keys [][]byte
	for rows.Next() {
		var table string
		err := rows.Scan(&table)
		if err != nil {
			return err
		}

		if !strings.HasPrefix(table, tx.db.prefix+"_") {
			continue
		}

		keys = append(keys, []byte(table[len(tx.db.prefix)+1:]))
	}

	// Fetch binary top level buckets..
	bucket := newReadWriteBucket(tx, tx.db.hexTableName, nil)
	err = bucket.ForEach(func(k, v []byte) error {
		keys = append(keys, k)
		return nil
	})
	if err != nil {
		return err
	}

	// Sort keys.
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })

	// Call callback.
	for _, k := range keys {
		err = fn(k)
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

	err := tx.tx.Rollback()

	// Unlock the transaction regardless of the error result.
	tx.active = false
	if tx.readOnly {
		tx.db.lock.RUnlock()
	} else {
		tx.db.lock.Unlock()
	}

	return err
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
		tx.db.ctx,
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
	_, err := tx.tx.ExecContext(tx.db.ctx, createTableSql)
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

	_, err := tx.tx.ExecContext(tx.db.ctx, "DROP TABLE public."+table+";")
	if err != nil {
		return err
	}

	_, err = tx.tx.ExecContext(tx.db.ctx, "DELETE FROM "+tx.db.sequenceTableName+" WHERE table_name=$1", table)
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
	err := tx.tx.Commit()
	if err == nil && tx.onCommit != nil {
		tx.onCommit()
	}

	// Unlock the transaction regardless of the error result.
	tx.active = false
	if tx.readOnly {
		tx.db.lock.RUnlock()
	} else {
		tx.db.lock.Unlock()
	}

	return nil
}

// OnCommit sets the commit callback (overriding if already set).
func (tx *readWriteTx) OnCommit(cb func()) {
	tx.onCommit = cb
}
