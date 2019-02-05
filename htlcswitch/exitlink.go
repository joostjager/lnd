package htlcswitch

import (
	"github.com/lightningnetwork/lnd/invoices"
	"github.com/lightningnetwork/lnd/lnpeer"
	"github.com/lightningnetwork/lnd/lnwire"
)

type exitLink struct {
	invoices *invoices.InvoiceRegistry
}

// HandleSwitchPacket handles the switch packets. This packets might be
// forwarded to us from another channel link in case the htlc update
// came from another peer or if the update was created by user
// initially.
//
// NOTE: This function MUST be non-blocking (or block as little as
// possible).
func (l *exitLink) HandleSwitchPacket(*htlcPacket) error {
	panic("not implemented")
}

// HandleChannelUpdate handles the htlc requests as settle/add/fail
// which sent to us from remote peer we have a channel with.
//
// NOTE: This function MUST be non-blocking (or block as little as
// possible).
func (l *exitLink) HandleChannelUpdate(lnwire.Message) {
	panic("not implemented")
}

// ChanID returns the channel ID for the channel link. The channel ID
// is a more compact representation of a channel's full outpoint.
func (l *exitLink) ChanID() lnwire.ChannelID {
	panic("not implemented")
}

// ShortChanID returns the short channel ID for the channel link. The
// short channel ID encodes the exact location in the main chain that
// the original funding output can be found.
func (l *exitLink) ShortChanID() lnwire.ShortChannelID {
	return SwitchSettleHop
}

// UpdateShortChanID updates the short channel ID for a link. This may
// be required in the event that a link is created before the short
// chan ID for it is known, or a re-org occurs, and the funding
// transaction changes location within the chain.
func (l *exitLink) UpdateShortChanID() (lnwire.ShortChannelID, error) {
	panic("not implemented")
}

// UpdateForwardingPolicy updates the forwarding policy for the target
// ChannelLink. Once updated, the link will use the new forwarding
// policy to govern if it an incoming HTLC should be forwarded or not.
func (l *exitLink) UpdateForwardingPolicy(ForwardingPolicy) {
	panic("not implemented")
}

// HtlcSatifiesPolicy should return a nil error if the passed HTLC
// details satisfy the current forwarding policy fo the target link.
// Otherwise, a valid protocol failure message should be returned in
// order to signal to the source of the HTLC, the policy consistency
// issue.
func (l *exitLink) HtlcSatifiesPolicy(payHash [32]byte, incomingAmt lnwire.MilliSatoshi,
	amtToForward lnwire.MilliSatoshi,
	incomingTimeout, outgoingTimeout uint32,
	heightNow uint32) lnwire.FailureMessage {

	panic("not implemented")
}

// Bandwidth returns the amount of milli-satoshis which current link
// might pass through channel link. The value returned from this method
// represents the up to date available flow through the channel. This
// takes into account any forwarded but un-cleared HTLC's, and any
// HTLC's which have been set to the over flow queue.
func (l *exitLink) Bandwidth() lnwire.MilliSatoshi {
	panic("not implemented")
}

// Stats return the statistics of channel link. Number of updates,
// total sent/received milli-satoshis.
func (l *exitLink) Stats() (uint64, lnwire.MilliSatoshi, lnwire.MilliSatoshi) {
	panic("not implemented")
}

// Peer returns the representation of remote peer with which we have
// the channel link opened.
func (l *exitLink) Peer() lnpeer.Peer {
	panic("not implemented")
}

// EligibleToForward returns a bool indicating if the channel is able
// to actively accept requests to forward HTLC's. A channel may be
// active, but not able to forward HTLC's if it hasn't yet finalized
// the pre-channel operation protocol with the remote peer. The switch
// will use this function in forwarding decisions accordingly.
func (l *exitLink) EligibleToForward() bool {
	return true
}

// AttachMailBox delivers an active MailBox to the link. The MailBox may
// have buffered messages.
func (l *exitLink) AttachMailBox(MailBox) {
	panic("not implemented")
}

// Start/Stop are used to initiate the start/stop of the channel link
// functioning.
func (l *exitLink) Start() error {
	panic("not implemented")
}

func (l *exitLink) Stop() {
	panic("not implemented")
}
