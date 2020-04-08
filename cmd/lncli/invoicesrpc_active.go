// +build invoicesrpc

package main

import (
	"context"
	"encoding/hex"
	"fmt"

	"strconv"

	"github.com/lightningnetwork/lnd/lnrpc/invoicesrpc"
	"github.com/urfave/cli"
)

// invoicesCommands will return nil for non-invoicesrpc builds.
func invoicesCommands() []cli.Command {
	return []cli.Command{
		cancelInvoiceCommand,
		addHoldInvoiceCommand,
		settleInvoiceCommand,
	}
}

func getInvoicesClient(ctx *cli.Context) (invoicesrpc.InvoicesClient, func()) {
	conn := getClientConn(ctx, false)

	cleanUp := func() {
		conn.Close()
	}

	return invoicesrpc.NewInvoicesClient(conn), cleanUp
}

var settleInvoiceCommand = cli.Command{
	Name:     "settleinvoice",
	Category: "Invoices",
	Usage: "Reveal a preimage and use it to settle the corresponding " +
		"invoice. In case lnd already knows the preimage, it is " +
		"sufficient to only provide the payment hash.",
	ArgsUsage: "[preimage]",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name: "hash",
			Usage: "the invoice hash. Only needs to be set when " +
				"the preimage is known in lnd already and " +
				"not specified with --preimage.",
		},
		cli.StringFlag{
			Name: "preimage",
			Usage: "the hex-encoded preimage (32 byte) which will " +
				"allow settling an incoming HTLC payable to this " +
				"preimage. Only needs to be set when the " +
				"preimage isn't known to lnd yet.",
		},
	},
	Action: actionDecorator(settleInvoice),
}

func settleInvoice(ctx *cli.Context) error {
	client, cleanUp := getInvoicesClient(ctx)
	defer cleanUp()

	args := ctx.Args()

	var (
		preimage []byte
		err      error
	)
	switch {
	case ctx.IsSet("preimage"):
		preimage, err = hex.DecodeString(ctx.String("preimage"))
	case args.Present():
		preimage, err = hex.DecodeString(args.First())
	}
	if err != nil {
		return fmt.Errorf("unable to parse preimage: %v", err)
	}

	hash, err := hex.DecodeString(ctx.String("hash"))
	if err != nil {
		return fmt.Errorf("unable to parse hash: %v", err)
	}

	invoice := &invoicesrpc.SettleInvoiceMsg{
		Preimage: preimage,
		Hash:     hash,
	}

	resp, err := client.SettleInvoice(context.Background(), invoice)
	if err != nil {
		return err
	}

	printRespJSON(resp)

	return nil
}

var cancelInvoiceCommand = cli.Command{
	Name:     "cancelinvoice",
	Category: "Invoices",
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

	printRespJSON(resp)

	return nil
}

var addHoldInvoiceCommand = cli.Command{
	Name:     "addholdinvoice",
	Category: "Invoices",
	Usage:    "Add a new hold invoice.",
	Description: `
	Add a new invoice, expressing intent for a future payment.

	Invoices without an amount can be created by not supplying any
	parameters or providing an amount of 0. These invoices allow the payee
	to specify the amount of satoshis they wish to send.`,
	ArgsUsage: "[hash [amt]]",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name: "memo",
			Usage: "a description of the payment to attach along " +
				"with the invoice (default=\"\")",
		},
		cli.Int64Flag{
			Name:  "amt",
			Usage: "the amt of satoshis in this invoice",
		},
		cli.Int64Flag{
			Name:  "amt_msat",
			Usage: "the amt of millisatoshis in this invoice",
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
				"specified, an expiry of 3600 seconds (1 hour) " +
				"is implied.",
		},
		cli.BoolTFlag{
			Name: "private",
			Usage: "encode routing hints in the invoice with " +
				"private channels in order to assist the " +
				"payer in reaching you",
		},
		cli.StringFlag{
			Name:  "hash",
			Usage: "the invoice hash",
		},
		cli.StringFlag{
			Name:  "preimage",
			Usage: "the invoice preimage",
		},
	},
	Action: actionDecorator(addHoldInvoice),
}

func addHoldInvoice(ctx *cli.Context) error {
	var (
		descHash []byte
		err      error
	)

	client, cleanUp := getInvoicesClient(ctx)
	defer cleanUp()

	args := ctx.Args()

	var (
		amt, amtMsat int64
		hash         []byte
	)

	// Parse hash positional argument or flag.
	switch {
	case args.Present():
		if ctx.IsSet("hash") {
			return fmt.Errorf("cannot set hash positional " +
				"argument and flag")
		}
		hash, err = hex.DecodeString(args.First())
		if err != nil {
			return fmt.Errorf("unable to parse positional hash "+
				"argument %v: %v", args.First(), err)
		}
		args = args.Tail()

	case ctx.IsSet("hash"):
		hash, err = hex.DecodeString(ctx.String("hash"))
		if err != nil {
			return fmt.Errorf("unable to parse hash flag: %v", err)
		}
	}

	// Parse amount positional argument or flag.
	if args.Present() {
		if ctx.IsSet("amt") || ctx.IsSet("amt_msat") {
			return fmt.Errorf("cannot set amount positional " +
				"argument and flag")
		}

		amt, err = strconv.ParseInt(args.First(), 10, 64)
		if err != nil {
			return fmt.Errorf("unable to decode amt argument: %v",
				err)
		}
	} else {
		amt = ctx.Int64("amt")
		amtMsat = ctx.Int64("amt_msat")
	}

	preimage, err := hex.DecodeString(ctx.String("preimage"))
	if err != nil {
		return fmt.Errorf("unable to parse preimage: %v", err)
	}

	descHash, err = hex.DecodeString(ctx.String("description_hash"))
	if err != nil {
		return fmt.Errorf("unable to parse description_hash: %v", err)
	}

	invoice := &invoicesrpc.AddHoldInvoiceRequest{
		Memo:            ctx.String("memo"),
		Hash:            hash,
		Preimage:        preimage,
		Value:           amt,
		ValueMsat:       amtMsat,
		DescriptionHash: descHash,
		FallbackAddr:    ctx.String("fallback_addr"),
		Expiry:          ctx.Int64("expiry"),
		Private:         ctx.Bool("private"),
	}

	resp, err := client.AddHoldInvoice(context.Background(), invoice)
	if err != nil {
		return err
	}

	printJSON(struct {
		PayReq string `json:"pay_req"`
	}{
		PayReq: resp.PaymentRequest,
	})

	return nil
}
