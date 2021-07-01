package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/btcsuite/btcwallet/walletdb"
	_ "github.com/jackc/pgx/v4/stdlib"
)

const (
	hexTableId      = "hex"
	sequenceTableId = "sequence"

	systemTablePrefixExtension = "sys"
)

// KV stores a key/value pair.
type KV struct {
	key string
	val string
}

// db holds a reference to the postgres connection connection.
type db struct {
	dsn string

	// Table name prefix to simulate namespaces. We don't use schemas
	// because at least sqlite does not support that.
	prefix string

	// TODO: Copied from etcd, but is an anti-pattern.
	ctx context.Context

	db *sql.DB

	lock sync.RWMutex

	hexTableName      string
	sequenceTableName string
}

// Enforce db implements the walletdb.DB interface.
var _ walletdb.DB = (*db)(nil)

// newPostgresBackend returns a db object initialized with the passed backend
// config. If etcd connection cannot be estabished, then returns error.
func newPostgresBackend(ctx context.Context, dsn, prefix string) (*db, error) {
	dbConn, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}

	if prefix == "" {
		return nil, errors.New("empty postgres prefix")
	}

	// Use extended prefix to prevent clashes between system tables and app
	// buckets.
	hexTableName := fmt.Sprintf("%s%s_%s", prefix, systemTablePrefixExtension, hexTableId)
	sequenceTableName := fmt.Sprintf("%s%s_%s", prefix, systemTablePrefixExtension, sequenceTableId)

	hexCreateTableSql := getCreateTableSql(hexTableName)

	_, err = dbConn.ExecContext(context.TODO(), `
	-- DROP SCHEMA public CASCADE;
	CREATE SCHEMA IF NOT EXISTS public;

	CREATE TABLE IF NOT EXISTS public.`+sequenceTableName+`
	(
		table_name TEXT NOT NULL PRIMARY KEY,
		sequence bigint
	);
	`+hexCreateTableSql)
	if err != nil {
		return nil, err
	}

	backend := &db{
		dsn:               dsn,
		prefix:            prefix,
		ctx:               ctx,
		db:                dbConn,
		hexTableName:      hexTableName,
		sequenceTableName: sequenceTableName,
	}

	return backend, nil
}

func (db *db) getPrefixedTableName(table string) string {
	return fmt.Sprintf("%s_%s", db.prefix, table)
}

// View opens a database read transaction and executes the function f with the
// transaction passed as a parameter. After f exits, the transaction is rolled
// back. If f errors, its error is returned, not a rollback error (if any
// occur). The passed reset function is called before the start of the
// transaction and can be used to reset intermediate state. As callers may
// expect retries of the f closure (depending on the database backend used), the
// reset function will be called before each retry respectively.
func (db *db) View(f func(tx walletdb.ReadTx) error, reset func()) error {
	reset()

	tx, err := newReadWriteTx(db, true)
	if err != nil {
		return err
	}

	err = f(tx)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

// Update opens a database read/write transaction and executes the function f
// with the transaction passed as a parameter. After f exits, if f did not
// error, the transaction is committed. Otherwise, if f did error, the
// transaction is rolled back. If the rollback fails, the original error
// returned by f is still returned. If the commit fails, the commit error is
// returned. As callers may expect retries of the f closure, the reset function
// will be called before each retry respectively.
func (db *db) Update(f func(tx walletdb.ReadWriteTx) error, reset func()) error {
	reset()

	tx, err := newReadWriteTx(db, false)
	if err != nil {
		return err
	}

	err = f(tx)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

// PrintStats returns all collected stats pretty printed into a string.
func (db *db) PrintStats() string {
	return ""
}

// BeginReadWriteTx opens a database read+write transaction.
func (db *db) BeginReadWriteTx() (walletdb.ReadWriteTx, error) {
	return newReadWriteTx(db, false)
}

// BeginReadTx opens a database read transaction.
func (db *db) BeginReadTx() (walletdb.ReadTx, error) {
	return newReadWriteTx(db, true)
}

// Copy writes a copy of the database to the provided writer.  This call will
// start a read-only transaction to perform all operations.
// This function is part of the walletdb.Db interface implementation.
func (db *db) Copy(w io.Writer) error {
	return errors.New("not implemented")
}

// Close cleanly shuts down the database and syncs all data.
// This function is part of the walletdb.Db interface implementation.
func (db *db) Close() error {
	db.db.Close()

	return nil
}
