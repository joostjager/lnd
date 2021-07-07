package kvdb

import (
	"testing"
)

func TestBoltReadCursorEmptyInterval(t *testing.T) {
	f := NewBoltFixture(t)
	defer f.Cleanup()

	testReadCursorEmptyInterval(t, f.NewBackend())
}
func TestBoltReadCursorNonEmptyInterval(t *testing.T) {
	f := NewBoltFixture(t)
	defer f.Cleanup()

	testReadCursorNonEmptyInterval(t, f.NewBackend())
}
func TestBoltReadWriteCursor(t *testing.T) {
	f := NewBoltFixture(t)
	defer f.Cleanup()

	testReadWriteCursor(t, f.NewBackend())
}

func TestBoltReadWriteCursorWithBucketAndValue(t *testing.T) {
	f := NewBoltFixture(t)
	defer f.Cleanup()

	testReadWriteCursorWithBucketAndValue(t, f.NewBackend())
}

func TestBoltBucketCreation(t *testing.T) {
	f := NewBoltFixture(t)
	defer f.Cleanup()

	testBucketCreation(t, f.NewBackend())
}

func TestBoltBucketDeletion(t *testing.T) {
	f := NewBoltFixture(t)
	defer f.Cleanup()

	testBucketDeletion(t, f.NewBackend())
}

func TestBoltBucketForEach(t *testing.T) {
	f := NewBoltFixture(t)
	defer f.Cleanup()

	testBucketForEach(t, f.NewBackend())
}

func TestBoltBucketForEachWithError(t *testing.T) {
	f := NewBoltFixture(t)
	defer f.Cleanup()

	testBucketForEachWithError(t, f.NewBackend())
}

func TestBoltBucketSequence(t *testing.T) {
	f := NewBoltFixture(t)
	defer f.Cleanup()

	testBucketSequence(t, f.NewBackend())
}

func TestBoltKeyClash(t *testing.T) {
	f := NewBoltFixture(t)
	defer f.Cleanup()

	testKeyClash(t, f.NewBackend())
}

func TestBoltBucketCreateDelete(t *testing.T) {
	f := NewBoltFixture(t)
	defer f.Cleanup()

	testBucketCreateDelete(t, f.NewBackend())
}

func TestBoltTxManualCommit(t *testing.T) {
	f := NewBoltFixture(t)
	defer f.Cleanup()

	testTxManualCommit(t, f.NewBackend())
}
func TestBoltTxRollback(t *testing.T) {
	f := NewBoltFixture(t)
	defer f.Cleanup()

	testTxRollback(t, f.NewBackend())
}
