package postgres

import (
	"context"

	"github.com/btcsuite/btcwallet/walletdb"

	"github.com/jackc/pgx/v4"
)

// readWriteCursor holds a reference to the cursors bucket, the value
// prefix and the current key used while iterating.
type readWriteCursor struct {
	bucket *readWriteBucket

	// currKey holds the current key of the cursor.
	currKey []byte
}

func newReadWriteCursor(b *readWriteBucket) *readWriteCursor {
	return &readWriteCursor{
		bucket: b,
	}
}

// First positions the cursor at the first key/value pair and returns
// the pair.
func (c *readWriteCursor) First() ([]byte, []byte) {
	var (
		key   []byte
		value []byte
	)
	err := c.bucket.tx.tx.QueryRow(
		context.TODO(),
		"SELECT key, value FROM kv WHERE "+parentSelector(c.bucket.id)+" ORDER BY key LIMIT 1",
	).Scan(&key, &value)

	if err == pgx.ErrNoRows {
		return nil, nil
	}

	// Copy current key to prevent modification by the caller.
	c.currKey = make([]byte, len(key))
	copy(c.currKey, key)

	return key, value
}

// Last positions the cursor at the last key/value pair and returns the
// pair.
func (c *readWriteCursor) Last() ([]byte, []byte) {
	var (
		key   []byte
		value []byte
	)
	err := c.bucket.tx.tx.QueryRow(
		context.TODO(),
		"SELECT key, value FROM kv WHERE "+parentSelector(c.bucket.id)+" ORDER BY key DESC LIMIT 1",
	).Scan(&key, &value)

	if err == pgx.ErrNoRows {
		return nil, nil
	}

	// Copy current key to prevent modification by the caller.
	c.currKey = make([]byte, len(key))
	copy(c.currKey, key)

	return key, value
}

// Next moves the cursor one key/value pair forward and returns the new
// pair.
//
// TODO: Optimize this by reading ahead in pages.
func (c *readWriteCursor) Next() ([]byte, []byte) {
	var (
		key   []byte
		value []byte
	)
	err := c.bucket.tx.tx.QueryRow(
		context.TODO(),
		"SELECT key, value FROM kv WHERE "+parentSelector(c.bucket.id)+" AND key>$1 ORDER BY key LIMIT 1",
		c.currKey,
	).Scan(&key, &value)

	if err == pgx.ErrNoRows {
		return nil, nil
	}

	// Copy current key to prevent modification by the caller.
	c.currKey = make([]byte, len(key))
	copy(c.currKey, key)

	return key, value
}

// Prev moves the cursor one key/value pair backward and returns the new
// pair.
func (c *readWriteCursor) Prev() ([]byte, []byte) {
	var (
		key   []byte
		value []byte
	)
	err := c.bucket.tx.tx.QueryRow(
		context.TODO(),
		"SELECT key, value FROM kv WHERE "+parentSelector(c.bucket.id)+" AND key<$1 ORDER BY key DESC LIMIT 1",
		c.currKey,
	).Scan(&key, &value)

	if err == pgx.ErrNoRows {
		return nil, nil
	}

	// Copy current key to prevent modification by the caller.
	c.currKey = make([]byte, len(key))
	copy(c.currKey, key)

	return key, value
}

// Seek positions the cursor at the passed seek key.  If the key does
// not exist, the cursor is moved to the next key after seek.  Returns
// the new pair.
func (c *readWriteCursor) Seek(seek []byte) ([]byte, []byte) {
	// Return nil if trying to seek to an empty key.
	if seek == nil {
		return nil, nil
	}

	var (
		key   []byte
		value []byte
	)
	err := c.bucket.tx.tx.QueryRow(
		context.TODO(),
		"SELECT key, value FROM kv WHERE "+parentSelector(c.bucket.id)+" AND key>=$1 ORDER BY key LIMIT 1",
		seek,
	).Scan(&key, &value)

	if err == pgx.ErrNoRows {
		return nil, nil
	}

	// Copy current key to prevent modification by the caller.
	c.currKey = make([]byte, len(key))
	copy(c.currKey, key)

	return key, value
}

// Delete removes the current key/value pair the cursor is at without
// invalidating the cursor.  Returns ErrIncompatibleValue if attempted
// when the cursor points to a nested bucket.
func (c *readWriteCursor) Delete() error {
	deleteKey := c.currKey

	var key []byte
	err := c.bucket.tx.tx.QueryRow(
		context.TODO(),
		"SELECT key FROM kv WHERE "+parentSelector(c.bucket.id)+" AND key>$1 ORDER BY key LIMIT 1",
		c.currKey,
	).Scan(&key)

	if err == pgx.ErrNoRows {
		c.currKey = nil
	} else {
		c.currKey = key
	}

	result, err := c.bucket.tx.tx.Exec(
		context.TODO(),
		"DELETE FROM kv WHERE "+parentSelector(c.bucket.id)+" AND key=$1 AND value IS NOT NULL",
		deleteKey,
	)
	if result.RowsAffected() != 1 {
		return walletdb.ErrIncompatibleValue
	}
	return err
}
