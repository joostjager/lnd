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
	return []cli.Command{
		subscribeSingleInvoiceCommand,
		settleInvoiceCommand,
		cancelInvoiceCommand,
	}
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

var settleInvoiceCommand = cli.Command{
	Name:     "settleinvoice",
	Category: "Payments",
	Usage:    "Reveal a preimage and use it to settle the corresponding invoice.",
	Description: `
	Todo.`,
	ArgsUsage: "preimage",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name: "preimage",
			Usage: "the hex-encoded preimage (32 byte) which will " +
				"allow settling an incoming HTLC payable to this " +
				"preimage.",
		},
	},
	Action: actionDecorator(settleInvoice),
}

func settleInvoice(ctx *cli.Context) error {
	var (
		preimage []byte
		err      error
	)

	client, cleanUp := getInvoicesClient(ctx)
	defer cleanUp()

	args := ctx.Args()

	switch {
	case ctx.IsSet("preimage"):
		preimage, err = hex.DecodeString(ctx.String("preimage"))
	case args.Present():
		preimage, err = hex.DecodeString(args.First())
	}

	if err != nil {
		return fmt.Errorf("unable to parse preimage: %v", err)
	}

	invoice := &invoicesrpc.SettleInvoiceMsg{
		PreImage: preimage,
	}

	resp, err := client.SettleInvoice(context.Background(), invoice)
	if err != nil {
		return err
	}

	printJSON(resp)

	return nil
}

var cancelInvoiceCommand = cli.Command{
	Name:     "cancelinvoice",
	Category: "Payments",
	Usage:    "Cancels a (hold) invoice",
	Description: `
	Todo.`,
	ArgsUsage: "paymenthash",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name: "paymenthash",
			Usage: "the hex-encoded payment hash (32 byte) for which the " +
				"corresponding invoice will be canceled.",
		},
	},
	Action: actionDecorator(cancelInvoice),
}

func cancelInvoice(ctx *cli.Context) error {
	var (
		paymentHash []byte
		err         error
	)

	client, cleanUp := getInvoicesClient(ctx)
	defer cleanUp()

	args := ctx.Args()

	switch {
	case ctx.IsSet("paymenthash"):
		paymentHash, err = hex.DecodeString(ctx.String("paymenthash"))
	case args.Present():
		paymentHash, err = hex.DecodeString(args.First())
	}

	if err != nil {
		return fmt.Errorf("unable to parse preimage: %v", err)
	}

	invoice := &invoicesrpc.CancelInvoiceMsg{
		PaymentHash: paymentHash,
	}

	resp, err := client.CancelInvoice(context.Background(), invoice)
	if err != nil {
		return err
	}

	printJSON(resp)

	return nil
}
