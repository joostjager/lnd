package htlcswitch

import (
	"fmt"

	"github.com/go-errors/errors"
	"github.com/lightningnetwork/lnd/lnwire"
)

type exitLinkConfig struct {
	// circuits provides restricted access to the switch's circuit map,
	// allowing the link to open and close circuits.
	circuits CircuitModifier

	// forwardPackets attempts to forward the batch of htlcs through the
	// switch, any failed packets will be returned to the provided
	// ChannelLink. The link's quit signal should be provided to allow
	// cancellation of forwarding during link shutdown.
	forwardPackets func(chan struct{}, ...*htlcPacket) chan error

	invoices InvoiceDatabase
}

type exitLink struct {

	// mailBox is the main interface between the outside world and the
	// link. All incoming messages will be sent over this mailBox. Messages
	// include new updates from our connected peer, and new packets to be
	// forwarded sent by the switch.
	mailBox MailBox

	// downstream is a channel in which new multi-hop HTLC's to be
	// forwarded will be sent across. Messages from this channel are sent
	// by the HTLC switch.
	downstream chan *htlcPacket

	quit chan struct{}

	cfg *exitLinkConfig
}

func newExitLink(cfg *exitLinkConfig) *exitLink {
	mailbox := newMemoryMailBox()

	return &exitLink{
		mailBox:    mailbox,
		downstream: mailbox.PacketOutBox(),
		quit:       make(chan struct{}),
		cfg:        cfg,
	}
}

// HandleSwitchPacket handles the switch packets. This packets might be
// forwarded to us from another channel link in case the htlc update
// came from another peer or if the update was created by user
// initially.
//
// NOTE: This function MUST be non-blocking (or block as little as
// possible).
func (l *exitLink) HandleSwitchPacket(pkt *htlcPacket) error {
	log.Tracef("exit link received switch packet inkey=%v, outkey=%v",
		pkt.inKey(), pkt.outKey())

	l.mailBox.AddPacket(pkt)
	return nil
}

// Start/Stop are used to initiate the start/stop of the channel link
// functioning.
func (l *exitLink) Start() error {
	if err := l.mailBox.Start(); err != nil {
		return err
	}

	go func() {
		for {
			select {
			// A message from the switch was just received. This indicates
			// that the link is an intermediate hop in a multi-hop HTLC
			// circuit.
			case pkt := <-l.downstream:
				l.handleDownStreamPkt(pkt, false)

			case <-l.quit:
				return
			}
		}
	}()

	return nil
}

func (l *exitLink) handlePkt(pkt *htlcPacket) (*htlcPacket, error) {
	switch htlc := pkt.htlc.(type) {
	case *lnwire.UpdateAddHTLC:

		log.Tracef("Received downstream htlc: payment_hash=%x",
			htlc.PaymentHash[:])

		invoice, _, err := l.cfg.invoices.LookupInvoice(
			htlc.PaymentHash,
		)
		if err != nil {
			log.Error(err)

			failure := lnwire.NewFailUnknownPaymentHash(htlc.Amount)

			opaqueReason, err := pkt.obfuscator.EncryptFirstHop(failure)
			if err != nil {
				return nil, fmt.Errorf("unable to obfuscate error: %v",
					err)
			}

			return &htlcPacket{
				incomingChanID: pkt.incomingChanID,
				incomingHTLCID: pkt.incomingHTLCID,
				hasSource:      true,
				htlc: &lnwire.UpdateFailHTLC{
					Reason: opaqueReason,
				},
			}, nil
		}

		err = l.cfg.circuits.OpenCircuits(Keystone{
			InKey: CircuitKey{
				ChanID: pkt.incomingChanID,
				HtlcID: pkt.incomingHTLCID,
			},
			OutKey: CircuitKey{
				ChanID: exitHop,
				HtlcID: invoice.AddIndex,
			},
		})
		if err != nil {
			return nil, fmt.Errorf("unable to open circuit: %v",
				err)
		}

		err = l.cfg.invoices.SettleInvoice(
			htlc.PaymentHash, htlc.Amount,
		)
		if err != nil {
			return nil, fmt.Errorf("settle invoice: %v", err)
		}

		preimage := invoice.Terms.PaymentPreimage

		return &htlcPacket{
			incomingChanID: pkt.incomingChanID,
			incomingHTLCID: pkt.incomingHTLCID,
			outgoingChanID: pkt.outgoingChanID,
			outgoingHTLCID: pkt.outgoingHTLCID,
			hasSource:      true,
			htlc: &lnwire.UpdateFulfillHTLC{
				PaymentPreimage: preimage,
			},
		}, nil

	}

	return nil, errors.New("unexpected packet type received")
}

func (l *exitLink) handleDownStreamPkt(pkt *htlcPacket, isReProcess bool) {
	reply, err := l.handlePkt(pkt)
	if err != nil {
		log.Error(err)
		return
	}

	l.cfg.forwardPackets(l.quit, reply)
	// TODO: handle forward err chan

	// TODO: Delete circuit?

}

func (l *exitLink) Stop() {
	close(l.quit)
}
