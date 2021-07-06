// +build kvdb_etcd

package kvdb

import (
	"testing"

	"github.com/lightningnetwork/lnd/kvdb/etcd"
	"github.com/stretchr/testify/require"
)

var (
	bkey = etcd.Bkey
	bval = etcd.Bval
	vkey = etcd.Vkey
)

func TestBucketCreation(t *testing.T) {
	t.Parallel()

	f := etcd.NewEtcdTestFixture(t)
	defer f.Cleanup()

	testBucketCreation(t, f.NewBackend())

	expected := map[string]string{
		bkey("apple"):                   bval("apple"),
		bkey("apple", "banana"):         bval("apple", "banana"),
		bkey("apple", "mango"):          bval("apple", "mango"),
		bkey("apple", "banana", "pear"): bval("apple", "banana", "pear"),
	}
	require.Equal(t, expected, f.Dump())
}

func TestBucketDeletion(t *testing.T) {
	t.Parallel()

	f := etcd.NewEtcdTestFixture(t)
	defer f.Cleanup()

	testBucketDeletion(t, f.NewBackend())

	expected := map[string]string{
		bkey("apple"):                   bval("apple"),
		bkey("apple", "banana"):         bval("apple", "banana"),
		vkey("key1", "apple", "banana"): "val1",
		vkey("key3", "apple", "banana"): "val3",
	}
	require.Equal(t, expected, f.Dump())
}

func TestBucketForEach(t *testing.T) {
	t.Parallel()

	f := etcd.NewEtcdTestFixture(t)
	defer f.Cleanup()

	testBucketForEach(t, f.NewBackend())

	expected := map[string]string{
		bkey("apple"):                   bval("apple"),
		bkey("apple", "banana"):         bval("apple", "banana"),
		vkey("key1", "apple"):           "val1",
		vkey("key2", "apple"):           "val2",
		vkey("key3", "apple"):           "val3",
		vkey("key1", "apple", "banana"): "val1",
		vkey("key2", "apple", "banana"): "val2",
		vkey("key3", "apple", "banana"): "val3",
	}
	require.Equal(t, expected, f.Dump())
}

func TestBucketForEachWithError(t *testing.T) {
	t.Parallel()

	f := etcd.NewEtcdTestFixture(t)
	defer f.Cleanup()

	testBucketForEachWithError(t, f.NewBackend())

	expected := map[string]string{
		bkey("apple"):           bval("apple"),
		bkey("apple", "banana"): bval("apple", "banana"),
		bkey("apple", "pear"):   bval("apple", "pear"),
		vkey("key1", "apple"):   "val1",
		vkey("key2", "apple"):   "val2",
	}
	require.Equal(t, expected, f.Dump())
}

func TestBucketSequence(t *testing.T) {
	t.Parallel()

	f := etcd.NewEtcdTestFixture(t)
	defer f.Cleanup()

	testBucketSequence(t, f.NewBackend())
}

func TestKeyClash(t *testing.T) {
	t.Parallel()

	f := etcd.NewEtcdTestFixture(t)
	defer f.Cleanup()

	testKeyClash(t, f.NewBackend())

	// Except that the only existing items in the db are:
	// bucket: /apple
	// bucket: /apple/banana
	// value: /apple/key -> val
	expected := map[string]string{
		bkey("apple"):           bval("apple"),
		bkey("apple", "banana"): bval("apple", "banana"),
		vkey("key", "apple"):    "val",
	}
	require.Equal(t, expected, f.Dump())
}

func TestBucketCreateDelete(t *testing.T) {
	t.Parallel()

	f := etcd.NewEtcdTestFixture(t)
	defer f.Cleanup()

	testBucketCreateDelete(t, f.NewBackend())

	expected := map[string]string{
		vkey("banana", "apple"): "value",
		bkey("apple"):           bval("apple"),
	}
	require.Equal(t, expected, f.Dump())
}

func TestReadCursorEmptyInterval(t *testing.T) {
	t.Parallel()

	f := etcd.NewEtcdTestFixture(t)
	defer f.Cleanup()

	testReadCursorEmptyInterval(t, f.NewBackend())
}
func TestReadCursorNonEmptyInterval(t *testing.T) {
	t.Parallel()

	f := etcd.NewEtcdTestFixture(t)
	defer f.Cleanup()

	testReadCursorNonEmptyInterval(t, f.NewBackend())
}
func TestReadWriteCursor(t *testing.T) {
	t.Parallel()

	f := etcd.NewEtcdTestFixture(t)
	defer f.Cleanup()

	testReadWriteCursor(t, f.NewBackend())

	expected := map[string]string{
		bkey("apple"):       bval("apple"),
		vkey("a", "apple"):  "0",
		vkey("c", "apple"):  "3",
		vkey("cx", "apple"): "x",
		vkey("cy", "apple"): "y",
		vkey("da", "apple"): "3",
		vkey("f", "apple"):  "5",
	}
	require.Equal(t, expected, f.Dump())
}

func TestReadWriteCursorWithBucketAndValue(t *testing.T) {
	t.Parallel()

	f := etcd.NewEtcdTestFixture(t)
	defer f.Cleanup()

	testReadWriteCursorWithBucketAndValue(t, f.NewBackend())

	expected := map[string]string{
		bkey("apple"):           bval("apple"),
		bkey("apple", "banana"): bval("apple", "banana"),
		bkey("apple", "pear"):   bval("apple", "pear"),
		vkey("key", "apple"):    "val",
	}
	require.Equal(t, expected, f.Dump())
}

func TestTxManualCommit(t *testing.T) {
	t.Parallel()

	f := etcd.NewEtcdTestFixture(t)
	defer f.Cleanup()

	testTxManualCommit(t, f.NewBackend())

	expected := map[string]string{
		bkey("apple"):            bval("apple"),
		vkey("testKey", "apple"): "testVal",
	}
	require.Equal(t, expected, f.Dump())
}

func TestTxRollback(t *testing.T) {
	t.Parallel()

	f := etcd.NewEtcdTestFixture(t)
	defer f.Cleanup()

	testTxRollback(t, f.NewBackend())

	require.Equal(t, map[string]string{}, f.Dump())
}
