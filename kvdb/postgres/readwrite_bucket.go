package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v4"

	"github.com/btcsuite/btcwallet/walletdb"
)

// readWriteBucket stores the bucket id and the buckets transaction.
type readWriteBucket struct {
	// id is used to identify the bucket. If id is null, it refers to the
	// root bucket.
	id *int64

	// tx holds the parent transaction.
	tx *readWriteTx
}

// newReadWriteBucket creates a new rw bucket with the passed transaction
// and bucket id.
func newReadWriteBucket(tx *readWriteTx, id *int64) *readWriteBucket {
	return &readWriteBucket{
		id: id,
		tx: tx,
	}
}

// NestedReadBucket retrieves a nested read bucket with the given key.
// Returns nil if the bucket does not exist.
func (b *readWriteBucket) NestedReadBucket(key []byte) walletdb.ReadBucket {
	return b.NestedReadWriteBucket(key)
}

func parentSelector(id *int64) string {
	if id == nil {
		return "parent_id IS NULL"
	}
	return fmt.Sprintf("parent_id=%v", *id)
}

// ForEach invokes the passed function with every key/value pair in
// the bucket. This includes nested buckets, in which case the value
// is nil, but it does not include the key/value pairs within those
// nested buckets.
func (b *readWriteBucket) ForEach(cb func(k, v []byte) error) error {
	rows, err := b.tx.tx.Query(
		context.TODO(),
		"SELECT key,value FROM kv WHERE "+parentSelector(b.id)+" ORDER BY key",
	)
	if err != nil {
		return err
	}

	type kv struct {
		k, v []byte
	}

	// Read all data in memory, to unblock the connection (nested queries).
	//
	// TODO: Convert to paginated reads to prevent OOM on large tables.
	var kvs []kv
	for rows.Next() {
		var (
			key   []byte
			value []byte
		)
		err := rows.Scan(&key, &value)
		if err != nil {
			rows.Close()
			return err
		}

		kvs = append(kvs, kv{k: key, v: value})
	}
	rows.Close()

	for _, kv := range kvs {
		err = cb(kv.k, kv.v)
		if err != nil {
			return err
		}
	}

	return nil
}

// Get returns the value for the given key. Returns nil if the key does
// not exist in this bucket.
func (b *readWriteBucket) Get(key []byte) []byte {
	// Return nil if the key is empty.
	if len(key) == 0 {
		return nil
	}

	var value *[]byte
	err := b.tx.tx.QueryRow(
		context.TODO(),
		"SELECT value FROM kv WHERE "+parentSelector(b.id)+" AND key=$1",
		key,
	).Scan(&value)

	if err == pgx.ErrNoRows {
		return nil
	}

	return *value
}

func (b *readWriteBucket) ReadCursor() walletdb.ReadCursor {
	return newReadWriteCursor(b)
}

// NestedReadWriteBucket retrieves a nested bucket with the given key.
// Returns nil if the bucket does not exist.
func (b *readWriteBucket) NestedReadWriteBucket(key []byte) walletdb.ReadWriteBucket {
	if len(key) == 0 {
		return nil
	}

	var id int64
	err := b.tx.tx.QueryRow(
		context.TODO(),
		"SELECT id FROM kv WHERE "+parentSelector(b.id)+" AND key=$1 AND value IS NULL",
		key,
	).Scan(&id)

	if err != nil {
		return nil
	}

	return newReadWriteBucket(b.tx, &id)
}

// CreateBucket creates and returns a new nested bucket with the given
// key. Returns ErrBucketExists if the bucket already exists,
// ErrBucketNameRequired if the key is empty, or ErrIncompatibleValue
// if the key value is otherwise invalid for the particular database
// implementation.  Other errors are possible depending on the
// implementation.
func (b *readWriteBucket) CreateBucket(key []byte) (
	walletdb.ReadWriteBucket, error) {

	if len(key) == 0 {
		return nil, walletdb.ErrBucketNameRequired
	}

	var (
		value *[]byte
		id    int64
	)
	err := b.tx.tx.QueryRow(
		context.TODO(),
		"select id,value from kv where "+parentSelector(b.id)+" and key=$1",
		key).Scan(&id, &value)

	switch {
	case err == pgx.ErrNoRows:

	case err == nil && value == nil:
		return nil, walletdb.ErrBucketExists

	case err == nil && value != nil:
		return nil, walletdb.ErrIncompatibleValue

	case err != nil:
		return nil, err
	}

	err = b.tx.tx.QueryRow(
		context.TODO(),
		"insert into kv (parent_id, key) values($1, $2) RETURNING id",
		b.id, key,
	).Scan(&id)
	if err != nil {
		return nil, err
	}

	return newReadWriteBucket(b.tx, &id), nil
}

// CreateBucketIfNotExists creates and returns a new nested bucket with
// the given key if it does not already exist.  Returns
// ErrBucketNameRequired if the key is empty or ErrIncompatibleValue
// if the key value is otherwise invalid for the particular database
// backend.  Other errors are possible depending on the implementation.
func (b *readWriteBucket) CreateBucketIfNotExists(key []byte) (
	walletdb.ReadWriteBucket, error) {

	if len(key) == 0 {
		return nil, walletdb.ErrBucketNameRequired
	}

	var (
		value *[]byte
		id    int64
	)
	err := b.tx.tx.QueryRow(
		context.TODO(),
		"select id,value from kv where "+parentSelector(b.id)+" and key=$1",
		key).Scan(&id, &value)

	switch {
	case err == pgx.ErrNoRows:
		err = b.tx.tx.QueryRow(
			context.TODO(),
			"insert into kv (parent_id, key) values($1, $2) RETURNING id",
			b.id, key,
		).Scan(&id)
		if err != nil {
			return nil, err
		}

	case err == nil && value != nil:
		return nil, walletdb.ErrIncompatibleValue

	case err != nil:
		return nil, err
	}

	return newReadWriteBucket(b.tx, &id), nil
}

// DeleteNestedBucket deletes the nested bucket and its sub-buckets
// pointed to by the passed key. All values in the bucket and sub-buckets
// will be deleted as well.
func (b *readWriteBucket) DeleteNestedBucket(key []byte) error {
	// TODO shouldn't empty key return ErrBucketNameRequired ?
	if len(key) == 0 {
		return walletdb.ErrIncompatibleValue
	}

	result, err := b.tx.tx.Exec(
		context.TODO(),
		"DELETE FROM kv WHERE "+parentSelector(b.id)+" AND key=$1 AND value IS NULL",
		key,
	)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return walletdb.ErrBucketNotFound
	}

	return nil
}

// Put updates the value for the passed key.
// Returns ErrKeyRequred if te passed key is empty.
func (b *readWriteBucket) Put(key, value []byte) error {
	if len(key) == 0 {
		return walletdb.ErrKeyRequired
	}

	// Prevent NULL being written for an empty value slice.
	if value == nil {
		value = []byte{}
	}

	result, err := b.tx.tx.Exec(
		context.TODO(),
		"INSERT INTO kv (key, value, parent_id) VALUES($1, $2, $3) ON CONFLICT (parent_id, key) DO UPDATE SET value=$2 WHERE kv.value IS NOT NULL",
		key, value, b.id,
	)
	if err != nil {
		return err
	}

	if result.RowsAffected() != 1 {
		return walletdb.ErrIncompatibleValue
	}

	return nil
}

// Delete deletes the key/value pointed to by the passed key.
// Returns ErrKeyRequred if the passed key is empty.
func (b *readWriteBucket) Delete(key []byte) error {
	if key == nil {
		return nil
	}
	if len(key) == 0 {
		return walletdb.ErrKeyRequired
	}

	_, err := b.tx.tx.Exec(
		context.TODO(),
		"DELETE FROM kv WHERE key=$1 AND "+parentSelector(b.id)+" AND value IS NOT NULL",
		key,
	)
	if err != nil {
		return err
	}

	// TODO: Check is key points to bucket if no rows affected?
	return nil
}

// ReadWriteCursor returns a new read-write cursor for this bucket.
func (b *readWriteBucket) ReadWriteCursor() walletdb.ReadWriteCursor {
	return newReadWriteCursor(b)
}

// Tx returns the buckets transaction.
func (b *readWriteBucket) Tx() walletdb.ReadWriteTx {
	return b.tx
}

// NextSequence returns an autoincrementing sequence number for this bucket.
// Note that this is not a thread safe function and as such it must not be used
// for synchronization.
func (b *readWriteBucket) NextSequence() (uint64, error) {
	seq := b.Sequence() + 1

	return seq, b.SetSequence(seq)
}

// SetSequence updates the sequence number for the bucket.
func (b *readWriteBucket) SetSequence(v uint64) error {
	result, err := b.tx.tx.Exec(
		context.TODO(),
		"UPDATE kv SET sequence=$2 WHERE id=$1",
		b.id, int64(v),
	)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return errors.New("cannot set sequence")
	}

	return nil
}

// Sequence returns the current sequence number for this bucket without
// incrementing it.
func (b *readWriteBucket) Sequence() uint64 {
	var seq int64

	err := b.tx.tx.QueryRow(
		context.TODO(),
		"SELECT sequence FROM kv WHERE id=$1",
		b.id,
	).Scan(&seq)

	if err == pgx.ErrNoRows {
		return 0
	}

	return uint64(seq)
}
