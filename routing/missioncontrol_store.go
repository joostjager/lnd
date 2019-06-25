package routing

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/coreos/bbolt"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/lnwire"
)

var (
	// resultsKey is the fixed key under which the attempt results are
	// stored.
	resultsKey = []byte("missioncontrol-results")

	// Big endian is the preferred byte order, due to cursor scans over
	// integer keys iterating in order.
	byteOrder = binary.BigEndian

	// unknownErrorSourceIdx is the database encoding of an unknown error
	// source.
	unknownErrorSourceIdx = -1
)

type missionControlStore interface {
}

type bboltMissionControlStore struct {
	db *bbolt.DB
}

func newMissionControlStore(db *bbolt.DB) (*bboltMissionControlStore, error) {
	// Create buckets if not yet existing.
	err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(resultsKey)
		if err != nil {
			return fmt.Errorf("cannot create results bucket: %v",
				err)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return &bboltMissionControlStore{
		db: db,
	}, nil
}

// Clear removes all reports from the db.
func (b *bboltMissionControlStore) Clear() error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		if err := tx.DeleteBucket(resultsKey); err != nil {
			return err
		}

		_, err := tx.CreateBucket(resultsKey)
		return err
	})
}

// Fetch returns all reports currently stored in the database.
func (b *bboltMissionControlStore) Fetch() ([]*paymentResult, error) {

	var (
		results []*paymentResult
	)

	err := b.db.View(func(tx *bbolt.Tx) error {
		// Results.
		resultBucket := tx.Bucket(resultsKey)

		return resultBucket.ForEach(func(k, v []byte) error {
			result, err := deserializeResult(k, v)
			if err != nil {
				return err
			}

			results = append(results, result)
			return nil
		})

	})
	if err != nil {
		return nil, err
	}

	log.Debugf("Fetched from db: %v results", len(results))

	return results, nil
}

func deserializeResult(k, v []byte) (
	*paymentResult, error) {

	result := paymentResult{
		id: byteOrder.Uint64(k),
	}

	r := bytes.NewReader(v)

	// Read timestamp and error source index.
	var (
		timeFwd, timeReply uint64
		dbErrSourceIdx     int32
	)

	err := channeldb.ReadElements(
		r, &timeFwd, &timeReply, &result.success, &dbErrSourceIdx,
	)
	if err != nil {
		return nil, err
	}
	result.timeFwd = time.Unix(0, int64(timeFwd)).UTC()
	result.timeReply = time.Unix(0, int64(timeReply)).UTC()
	if dbErrSourceIdx != -1 {
		errSourceIdx := int(dbErrSourceIdx)
		result.errorSourceIndex = &errSourceIdx
	}

	// Read route.
	route, err := channeldb.DeserializeRoute(r)
	if err != nil {
		return nil, err
	}
	result.route = &route

	failureFlag, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	if failureFlag != 0 {
		// Read failure.
		result.failure, err = lnwire.DecodeFailure(r, 0)
		if err != nil {
			return nil, err
		}
	}

	return &result, nil
}

// Add adds a new report to the db.
func (b *bboltMissionControlStore) AddResult(rp *paymentResult) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(resultsKey)

		var b bytes.Buffer

		var dbErrSourceIdx int32
		if rp.errorSourceIndex == nil {
			dbErrSourceIdx = -1
		} else {
			dbErrSourceIdx = int32(*rp.errorSourceIndex)
		}

		// Write timestamp and error source.
		// TODO(joostjager): support unknown source.
		err := channeldb.WriteElements(
			&b,
			uint64(rp.timeFwd.UnixNano()),
			uint64(rp.timeReply.UnixNano()),
			rp.success, dbErrSourceIdx,
		)
		if err != nil {
			return err
		}

		// Write route.
		if err := channeldb.SerializeRoute(&b, *rp.route); err != nil {
			return err
		}

		// Write failure flag and failure.
		if rp.failure == nil {
			if err := b.WriteByte(0); err != nil {
				return err
			}
		} else {
			if err := b.WriteByte(1); err != nil {
				return err
			}
			err := lnwire.EncodeFailure(&b, rp.failure, 0)
			if err != nil {
				return err
			}
		}

		// Put into attempts bucket.
		return bucket.Put(idToKey(rp.id), b.Bytes())
	})
}

// idToKey returns a byte slice representing the provided id.
func idToKey(id uint64) []byte {
	var seqBytes [8]byte
	byteOrder.PutUint64(seqBytes[:], id)

	return seqBytes[:]
}
