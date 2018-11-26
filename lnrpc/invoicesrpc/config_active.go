// +build invoicesrpc

package invoicesrpc

import (
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/lightningnetwork/lnd/htlcswitch"
	"github.com/lightningnetwork/lnd/invoices"
	"github.com/lightningnetwork/lnd/macaroons"
)

// Config is the primary configuration struct for the invoices RPC server. It
// contains all the items required for the rpc server to carry out its
// duties. The fields with struct tags are meant to be parsed as normal
// configuration options, while if able to be populated, the latter fields MUST
// also be specified.
type Config struct {
	// NetworkDir is the main network directory wherein the invoices rpc
	// server will find the macaroon named DefaultInvoicesMacFilename.
	NetworkDir string

	// MacService is the main macaroon service that we'll use to handle
	// authentication for the invoices rpc server.
	MacService *macaroons.Service

	InvoiceRegistry *invoices.InvoiceRegistry

	// Switch is used by the invoices subserver to control acceptance and
	// cancelation of invoices.
	Switch *htlcswitch.Switch

	// ChainParams are required to properly decode invoice payment requests
	// that are marshalled over rpc.
	ChainParams *chaincfg.Params
}
