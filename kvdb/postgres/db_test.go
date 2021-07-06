package postgres

import (
	"database/sql"
	"testing"

	"github.com/btcsuite/btcwallet/walletdb"
	"github.com/btcsuite/btcwallet/walletdb/walletdbtest"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

const (
	dsn    = "postgres://bottle:bottle@localhost:45432/lnd?sslmode=disable"
	prefix = "test"
)

func clearTestDb(t *testing.T) {
	dbConn, err := sql.Open("pgx", dsn)
	require.NoError(t, err)

	_, err = dbConn.ExecContext(context.Background(), "DROP SCHEMA public CASCADE;")
	require.NoError(t, err)
}

func openTestDb(t *testing.T) *db {
	clearTestDb(t)

	db, err := newPostgresBackend(context.Background(), dsn, prefix)
	require.NoError(t, err)

	return db
}

func TestDb(t *testing.T) {
	db := openTestDb(t)

	err := db.Update(func(tx walletdb.ReadWriteTx) error {
		// Try to delete all data (there is none).
		err := tx.DeleteTopLevelBucket([]byte("top"))
		require.ErrorIs(t, walletdb.ErrBucketNotFound, err)

		// Create top level bucket.
		top, err := tx.CreateTopLevelBucket([]byte("top"))
		require.NoError(t, err)
		require.NotNil(t, top)

		// Create second top level bucket with special characters.
		top2, err := tx.CreateTopLevelBucket([]byte{1, 2, 3})
		require.NoError(t, err)
		require.NotNil(t, top2)

		top2 = tx.ReadWriteBucket([]byte{1, 2, 3})
		require.NotNil(t, top2)

		// List top level buckets.
		var tlKeys [][]byte
		require.NoError(t, tx.ForEachBucket(func(k []byte) error {
			tlKeys = append(tlKeys, k)
			return nil
		}))
		require.Equal(t, [][]byte{[]byte{1, 2, 3}, []byte("top")}, tlKeys)

		// Create third top level bucket with special uppercase.
		top3, err := tx.CreateTopLevelBucket([]byte("UpperBucket"))
		require.NoError(t, err)
		require.NotNil(t, top3)

		top3 = tx.ReadWriteBucket([]byte("UpperBucket"))
		require.NotNil(t, top3)

		// Assert that key doesn't exist.
		require.Nil(t, top.Get([]byte("key")))

		require.NoError(t, top.ForEach(func(k, v []byte) error {
			require.Fail(t, "unexpected data")
			return nil
		}))

		// Put key.
		require.NoError(t, top.Put([]byte("key"), []byte("val")))
		require.Equal(t, []byte("val"), top.Get([]byte("key")))

		// Overwrite key.
		require.NoError(t, top.Put([]byte("key"), []byte("val2")))
		require.Equal(t, []byte("val2"), top.Get([]byte("key")))

		// Put nil value.
		require.NoError(t, top.Put([]byte("nilkey"), nil))
		require.Equal(t, []byte(""), top.Get([]byte("nilkey")))

		// Put empty value.
		require.NoError(t, top.Put([]byte("nilkey"), []byte{}))
		require.Equal(t, []byte(""), top.Get([]byte("nilkey")))

		// Try to create bucket with same name as previous key.
		_, err = top.CreateBucket([]byte("key"))
		require.ErrorIs(t, err, walletdb.ErrIncompatibleValue)

		_, err = top.CreateBucketIfNotExists([]byte("key"))
		require.ErrorIs(t, err, walletdb.ErrIncompatibleValue)

		// Create sub-bucket.
		sub2, err := top.CreateBucket([]byte("sub2"))
		require.NoError(t, err)
		require.NotNil(t, sub2)

		// Assert that re-creating the bucket fails.
		_, err = top.CreateBucket([]byte("sub2"))
		require.ErrorIs(t, err, walletdb.ErrBucketExists)

		// Assert that create-if-not-exists succeeds.
		_, err = top.CreateBucketIfNotExists([]byte("sub2"))
		require.NoError(t, err)

		// Assert that fetching the bucket succeeds.
		sub2 = top.NestedReadWriteBucket([]byte("sub2"))
		require.NotNil(t, sub2)

		// Try to put key with same name as bucket.
		require.ErrorIs(t, top.Put([]byte("sub2"), []byte("val")), walletdb.ErrIncompatibleValue)

		// Put key into sub bucket.
		require.NoError(t, sub2.Put([]byte("subkey"), []byte("subval")))
		require.Equal(t, []byte("subval"), sub2.Get([]byte("subkey")))

		// Overwrite key in sub bucket.
		require.NoError(t, sub2.Put([]byte("subkey"), []byte("subval2")))
		require.Equal(t, []byte("subval2"), sub2.Get([]byte("subkey")))

		// Check for each result.
		kvs := make(map[string][]byte)
		require.NoError(t, top.ForEach(func(k, v []byte) error {
			kvs[string(k)] = v
			return nil
		}))
		require.Equal(t, map[string][]byte{
			"key":    []byte("val2"),
			"nilkey": []byte(""),
			"sub2":   nil,
		}, kvs)

		// Delete key.
		require.NoError(t, top.Delete([]byte("key")))

		// Delete non-existent key.
		require.NoError(t, top.Delete([]byte("keynonexistent")))

		// Test sequence.
		require.Equal(t, uint64(0), top.Sequence())

		require.NoError(t, top.SetSequence(100))
		require.Equal(t, uint64(100), top.Sequence())

		require.NoError(t, top.SetSequence(101))
		require.Equal(t, uint64(101), top.Sequence())

		next, err := top.NextSequence()
		require.NoError(t, err)
		require.Equal(t, uint64(102), next)

		next, err = sub2.NextSequence()
		require.NoError(t, err)
		require.Equal(t, uint64(1), next)

		// Test cursor.
		cursor := top.ReadWriteCursor()
		k, v := cursor.First()
		require.Equal(t, []byte("nilkey"), k)
		require.Equal(t, []byte(""), v)

		k, v = cursor.Last()
		require.Equal(t, []byte("sub2"), k)
		require.Nil(t, v)

		k, v = cursor.Prev()
		require.Equal(t, []byte("nilkey"), k)
		require.Equal(t, []byte(""), v)

		k, v = cursor.Prev()
		require.Nil(t, k)
		require.Nil(t, v)

		k, v = cursor.Next()
		require.Equal(t, []byte("sub2"), k)
		require.Nil(t, v)

		k, v = cursor.Next()
		require.Nil(t, k)
		require.Nil(t, v)

		k, v = cursor.Seek([]byte("nilkey"))
		require.Equal(t, []byte("nilkey"), k)
		require.Equal(t, []byte(""), v)

		require.NoError(t, sub2.Put([]byte("k1"), []byte("v1")))
		require.NoError(t, sub2.Put([]byte("k2"), []byte("v2")))
		require.NoError(t, sub2.Put([]byte("k3"), []byte("v3")))

		cursor = sub2.ReadWriteCursor()
		cursor.First()
		for i := 0; i < 4; i++ {
			require.NoError(t, cursor.Delete())
		}
		require.NoError(t, sub2.ForEach(func(k, v []byte) error {
			require.Fail(t, "unexpected data")
			return nil
		}))

		//Try to delete all data.
		require.NoError(t, tx.DeleteTopLevelBucket([]byte("top")))
		require.Nil(t, tx.ReadBucket([]byte("top")))

		return nil
	}, func() {})
	require.NoError(t, err)
}

// TestInterface performs all interfaces tests for this database driver.
func TestInterface(t *testing.T) {
	clearTestDb(t)

	const (
		// dbType is the database type name for this driver.
		dbType = "postgres"
	)

	ctx := context.Background()
	cfg := &Config{
		Dsn: dsn,
	}

	walletdbtest.TestInterface(t, dbType, ctx, cfg, prefix)
}
