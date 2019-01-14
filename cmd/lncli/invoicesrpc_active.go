// +build invoicesrpc

package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/lnrpc/invoicesrpc"
	"github.com/urfave/cli"
	"strconv"
)

// invoicesCommands will return nil for non-invoicesrpc builds.
func invoicesCommands() []cli.Command {
	return []cli.Command{
		subscribeSingleInvoiceCommand,
		cancelInvoiceCommand,
		addInvoiceV2Command,
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
	for {
		invoice, err := invoiceStream.Recv()
		if err != nil {
			return err
		}

		printRespJSON(invoice)
	}
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

var addInvoiceV2Command = cli.Command{
	Name:     "addinvoicev2",
	Category: "Payments",
	Usage:    "Add a new invoice.",
	Description: `
	Add a new invoice, expressing intent for a future payment.

	Invoices without an amount can be created by not supplying any
	parameters or providing an amount of 0. These invoices allow the payee
	to specify the amount of satoshis they wish to send.`,
	ArgsUsage: "value preimage",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name: "memo",
			Usage: "a description of the payment to attach along " +
				"with the invoice (default=\"\")",
		},
		cli.StringFlag{
			Name: "preimage",
			Usage: "the hex-encoded preimage (32 byte) which will " +
				"allow settling an incoming HTLC payable to this " +
				"preimage. If not set, a random preimage will be " +
				"created.",
		},
		cli.Int64Flag{
			Name:  "amt",
			Usage: "the amt of satoshis in this invoice",
		},
		cli.StringFlag{
			Name: "description_hash",
			Usage: "SHA-256 hash of the description of the payment. " +
				"Used if the purpose of payment cannot naturally " +
				"fit within the memo. If provided this will be " +
				"used instead of the description(memo) field in " +
				"the encoded invoice.",
		},
		cli.StringFlag{
			Name: "fallback_addr",
			Usage: "fallback on-chain address that can be used in " +
				"case the lightning payment fails",
		},
		cli.Int64Flag{
			Name: "expiry",
			Usage: "the invoice's expiry time in seconds. If not " +
				"specified an expiry of 3600 seconds (1 hour) " +
				"is implied.",
		},
		cli.BoolTFlag{
			Name: "private",
			Usage: "encode routing hints in the invoice with " +
				"private channels in order to assist the " +
				"payer in reaching you",
		},
	},
	Action: actionDecorator(addInvoiceV2),
}

func addInvoiceV2(ctx *cli.Context) error {
	var (
		preimage []byte
		descHash []byte
		amt      int64
		err      error
	)

	client, cleanUp := getClient(ctx)
	defer cleanUp()

	args := ctx.Args()

	switch {
	case ctx.IsSet("amt"):
		amt = ctx.Int64("amt")
	case args.Present():
		amt, err = strconv.ParseInt(args.First(), 10, 64)
		args = args.Tail()
		if err != nil {
			return fmt.Errorf("unable to decode amt argument: %v", err)
		}
	}

	switch {
	case ctx.IsSet("preimage"):
		preimage, err = hex.DecodeString(ctx.String("preimage"))
	case args.Present():
		preimage, err = hex.DecodeString(args.First())
	}

	if err != nil {
		return fmt.Errorf("unable to parse preimage: %v", err)
	}

	descHash, err = hex.DecodeString(ctx.String("description_hash"))
	if err != nil {
		return fmt.Errorf("unable to parse description_hash: %v", err)
	}

	invoice := &lnrpc.Invoice{
		Memo:            ctx.String("memo"),
		RPreimage:       preimage,
		Value:           amt,
		DescriptionHash: descHash,
		FallbackAddr:    ctx.String("fallback_addr"),
		Expiry:          ctx.Int64("expiry"),
		Private:         ctx.Bool("private"),
	}

	resp, err := client.AddInvoice(context.Background(), invoice)
	if err != nil {
		return err
	}

	printJSON(struct {
		RHash    string `json:"r_hash"`
		PayReq   string `json:"pay_req"`
		AddIndex uint64 `json:"add_index"`
	}{
		RHash:    hex.EncodeToString(resp.RHash),
		PayReq:   resp.PaymentRequest,
		AddIndex: resp.AddIndex,
	})

	return nil
}
