package channeldb

import (
	"bytes"
	"encoding/binary"

	"github.com/roasbeef/btcd/btcec"
	"github.com/coreos/bbolt"
)

var (
	nodeHistoryBucket = []byte("nodehistory")
)

type nodeHistoryMetric uint16

const (
	successfulRoutesCount nodeHistoryMetric = iota
)

// NodeHistory keeps track of various node level metrics
type NodeHistory struct {
	db *DB
}

// IncreaseSuccessfulRoutesCount updates node metric
func (n *NodeHistory) IncreaseSuccessfulRoutesCount(nodePub *btcec.PublicKey) error {
	
	pub := nodePub.SerializeCompressed()

	return n.db.Update(func(tx *bolt.Tx) error {
		// First grab the nodes bucket which stores the mapping from
		// pubKey to node information.
		nodes, err := tx.CreateBucketIfNotExists(nodeHistoryBucket)
		if err != nil {
			return err
		}

		metric := successfulRoutesCount

		var b bytes.Buffer
		if err := binary.Write(&b, byteOrder, &metric); err != nil {
			return err
		}

		if _, err := b.Write(pub); err != nil {
			return err
		}

		key := b.Bytes()

		var count uint64 

		value := nodes.Get(key)
		if value != nil {
			reader := bytes.NewReader(value)
			if err := binary.Read(reader, byteOrder, &count); err != nil {
				return err
			}
		}

		count++

		log.Infof("New successfulRoutesCount for node %x updated to %v",
			pub, count)

		var newValue bytes.Buffer
		if err := binary.Write(&newValue, byteOrder, count); err != nil {
			return err
		}
		// Next we create the mapping from source to the targeted
		// public key.
		if err := nodes.Put(key, newValue.Bytes()); err != nil {
			return err
		}
		return nil
	})
}
