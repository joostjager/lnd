// +build invoicesrpc

package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/lnrpc/invoicesrpc"
	"github.com/urfave/cli"
)

// invoicesCommands will return nil for non-invoicesrpc builds.
func invoicesCommands() []cli.Command {
	return []cli.Command{subscribeSingleInvoiceCommand}
}

func getInvoicesClient(ctx *cli.Context) (invoicesrpc.InvoicesClient, func()) {
	conn := getClientConn(ctx, false)

	cleanUp := func() {
		conn.Close()
	}

	return invoicesrpc.NewInvoicesClient(conn), cleanUp
}

var subscribeSingleInvoiceCommand = cli.Command{
	Name:      "subscribesingleinvoice",
	Category:  "Payments",
	Usage:     "Subscribes to a stream that will provide invoice updates.",
	ArgsUsage: "paymenthash",
	Action:    actionDecorator(subscribeSingleInvoice),
}

func subscribeSingleInvoice(ctx *cli.Context) error {
	client, cleanUp := getInvoicesClient(ctx)
	defer cleanUp()

	var recv func() (*lnrpc.Invoice, error)

	if ctx.NArg() == 0 {
		cli.ShowCommandHelp(ctx, "subscribesingleinvoice")
		return nil
	}

	paymentHash, err := hex.DecodeString(ctx.Args().First())
	if err != nil {
		return fmt.Errorf("unable to parse preimage: %v", err)
	}
	req := &lnrpc.PaymentHash{
		RHash: paymentHash,
	}
	invoiceStream, err := client.SubscribeSingleInvoice(
		context.Background(), req,
	)
	if err != nil {
		return err
	}
	recv = invoiceStream.Recv

	for {
		invoice, err := recv()
		if err != nil {
			return err
		}

		printRespJSON(invoice)
	}
}
