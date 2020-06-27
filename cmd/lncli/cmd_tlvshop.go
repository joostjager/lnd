package main

import (
	"context"
	"fmt"
	"time"

	"github.com/lightningnetwork/lnd/lntypes"

	"github.com/lightningnetwork/lnd/lnrpc"

	"github.com/lightningnetwork/lnd/lnrpc/invoicesrpc"
	"github.com/urfave/cli"
)

var ctxb = context.Background()

var tlvshopCommand = cli.Command{
	Name:   "tlvshop",
	Action: actionDecorator(tlvshop),
}

func tlvshop(ctx *cli.Context) error {
	conn := getClientConn(ctx, false)
	defer conn.Close()

	client := lnrpc.NewLightningClient(conn)
	invoicesClient := invoicesrpc.NewInvoicesClient(conn)

	allCtx, cancel := context.WithCancel(ctxb)
	defer cancel()

	stream, err := client.SubscribeInvoices(allCtx, &lnrpc.InvoiceSubscription{})
	if err != nil {
		return err
	}

	invoices := make(map[lntypes.Hash]struct{})
	for {
		invoice, err := stream.Recv()
		if err != nil {
			return err
		}

		// Don't need to track invoices without preimage.
		if invoice.RPreimage == nil {
			continue
		}

		hash, err := lntypes.MakeHash(invoice.RHash)
		if err != nil {
			return err
		}

		if _, ok := invoices[hash]; ok {
			continue
		}

		invoices[hash] = struct{}{}

		go func() {
			err := trackSingle(allCtx, invoicesClient, hash)
			if err != nil {
				fmt.Printf("error: %v\n", err)
				cancel()
			}
		}()
	}
}

func trackSingle(ctx context.Context, invoicesClient invoicesrpc.InvoicesClient,
	hash lntypes.Hash) error {

	singleStream, err := invoicesClient.SubscribeSingleInvoice(
		ctx,
		&invoicesrpc.SubscribeSingleInvoiceRequest{
			RHash: hash[:],
		},
	)
	if err != nil {
		return err
	}

	for {
		invoice, err := singleStream.Recv()
		if err != nil {
			return err
		}

		fmt.Printf("%x: state=%v, preimage=%x\n", invoice.RHash,
			invoice.State, invoice.RPreimage)

		if invoice.State == lnrpc.Invoice_ACCEPTED {
			// Check amount paid
			amtOk := invoice.Value > 500

			// Check webshop inventory here.
			time.Sleep(2 * time.Second)
			stockOk := true

			settlePayment := amtOk && stockOk

			if settlePayment {
				_, err := invoicesClient.SettleInvoice(
					ctxb,
					&invoicesrpc.SettleInvoiceMsg{
						Preimage: invoice.RPreimage,
					},
				)
				if err != nil {
					return err
				}
			} else {
				_, err := invoicesClient.CancelInvoice(
					ctxb,
					&invoicesrpc.CancelInvoiceMsg{
						PaymentHash: hash[:],
					},
				)
				if err != nil {
					return err
				}
			}
		}
	}
}
