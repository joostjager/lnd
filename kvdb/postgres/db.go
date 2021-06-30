package postgres

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log"
	"sync"

	"github.com/btcsuite/btcwallet/walletdb"
	_ "github.com/jackc/pgx/v4/stdlib"
)

const hexTableName = "hex"

// KV stores a key/value pair.
type KV struct {
	key string
	val string
}

// db holds a reference to the postgres connection connection.
type db struct {
	cfg Config
	ctx context.Context
	db  *sql.DB

	lock sync.RWMutex
}

// Enforce db implements the walletdb.DB interface.
var _ walletdb.DB = (*db)(nil)

// newPostgresBackend returns a db object initialized with the passed backend
// config. If etcd connection cannot be estabished, then returns error.
func newPostgresBackend(ctx context.Context, cfg Config) (*db, error) {
	dbConn, err := sql.Open("pgx", cfg.Dsn)
	if err != nil {
		log.Fatal(err)
	}

	hexCreateTableSql := getCreateTableSql(hexTableName)

	_, err = dbConn.ExecContext(context.TODO(), `
	DROP SCHEMA public CASCADE;
	CREATE SCHEMA public;

	CREATE TABLE IF NOT EXISTS public.top_sequences
	(
		table_name TEXT NOT NULL PRIMARY KEY,
		sequence bigint
	);
	`+hexCreateTableSql)
	if err != nil {
		return nil, err
	}

	backend := &db{
		cfg: cfg,
		ctx: ctx,
		db:  dbConn,
	}

	return backend, nil
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
