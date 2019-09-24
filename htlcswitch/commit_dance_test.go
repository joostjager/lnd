package htlcswitch

import (
	"fmt"
	"testing"
)

func TestCommitDance(t *testing.T) {
	a := simplifiedChannel{t: t}
	b := simplifiedChannel{t: t}

	print := func(node, step string) {
		var aWait, bWait int
		if a.waitForRevoke {
			aWait = 1
		}
		if b.waitForRevoke {
			bWait = 1
		}
		fmt.Printf("%-25v   A: %v:%v - %v:%v (%v), B: %v:%v - %v:%v (%v)\n",
			node+" "+step,
			a.localOur, a.localTheir, a.remoteOur, a.remoteTheir, aWait,
			b.localOur, b.localTheir, b.remoteOur, b.remoteTheir, bWait,
		)
	}
	a.print = func(step string) { print("a", step) }
	b.print = func(step string) { print("b", step) }

	print(" ", "start")

	txA := a.sendCommitSig(1)

	b.receiveCommitSig(txA)

	a.receiveRevoke()

	txA = a.sendCommitSig(1)

	txB := b.sendCommitSig(0)

	b.receiveCommitSig(txA)

	a.receiveRevoke()

	a.receiveCommitSig(txB)

	b.receiveRevoke()

	// THIS SHOULD NOT HAPPEN
	txA = a.sendCommitSig(0)
	b.receiveCommitSig(txA)
	a.receiveRevoke()

	// THIS SHOULD HAPPEN
	// txB = b.sendCommitSig(0)
	// a.receiveCommitSig(txB)
	// b.receiveRevoke()

}

type simplifiedTx struct {
	ourIdx, theirIdx int
}

type simplifiedChannel struct {
	t     *testing.T
	print func(string)

	localOur, localTheir   int
	remoteOur, remoteTheir int

	waitForRevoke bool

	lastTx simplifiedTx
}

func (s *simplifiedChannel) isSynced() bool {
	return s.localOur == s.remoteOur && s.localTheir == s.remoteTheir
}

func (s *simplifiedChannel) check() {
	s.t.Helper()

	if s.isSynced() {
		return
	}

	if s.lastTx.ourIdx == s.remoteOur && s.lastTx.theirIdx == s.localTheir {
		//s.t.Fatal("check failed")
	}
}

func (s *simplifiedChannel) receiveCommitSig(tx simplifiedTx) {
	s.t.Helper()

	if tx.ourIdx < s.localTheir {
		s.t.Fatal("unexpected index")
	}
	if tx.theirIdx < s.localOur {
		s.t.Fatal("unexpected index")
	}
	if tx.ourIdx == s.localTheir && tx.theirIdx == s.localOur {
		s.t.Fatal("no change")
	}

	s.localTheir = tx.ourIdx
	s.localOur = tx.theirIdx

	s.check()

	s.print(fmt.Sprintf("receiveCommitSig (%v:%v)", tx.ourIdx, tx.theirIdx))
}

func (s *simplifiedChannel) sendCommitSig(newUpdates int) simplifiedTx {
	s.t.Helper()

	ourIdx := s.remoteOur + newUpdates

	if s.waitForRevoke {
		s.t.Fatal("need revocation first")
	}
	s.waitForRevoke = true

	tx := simplifiedTx{
		ourIdx:   ourIdx,
		theirIdx: s.localTheir,
	}
	if tx == s.lastTx {
		s.t.Fatalf("identical commit tx sent (%v:%v)",
			tx.ourIdx, tx.theirIdx)

	}
	s.lastTx = tx

	s.remoteOur = tx.ourIdx
	s.remoteTheir = tx.theirIdx

	s.print(fmt.Sprintf("sendCommitSig (%v:%v)", tx.ourIdx, tx.theirIdx))

	return tx
}

func (s *simplifiedChannel) receiveRevoke() {
	s.t.Helper()

	if !s.waitForRevoke {
		s.t.Fatal("not waiting for revocation")
	}
	s.waitForRevoke = false

	s.print("receiveRevoke")
}
