package main

import (
	"context"
	"fmt"
	"time"

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

	invoices := make(map[[32]byte]struct{})
	for {
		invoice, err := stream.Recv()
		if err != nil {
			return err
		}

		paymentAddrBytes := invoice.PaymentAddr
		if paymentAddrBytes == nil {
			continue
		}

		var paymentAddr [32]byte
		copy(paymentAddr[:], paymentAddrBytes)

		if _, ok := invoices[paymentAddr]; ok {
			continue
		}

		invoices[paymentAddr] = struct{}{}

		go func() {
			err := trackSingle(allCtx, invoicesClient, paymentAddr)
			if err != nil {
				fmt.Printf("error: %v\n", err)
				cancel()
			}
		}()
	}
}

func trackSingle(ctx context.Context, invoicesClient invoicesrpc.InvoicesClient,
	paymentAddr [32]byte) error {

	singleStream, err := invoicesClient.SubscribeSingleInvoice(
		ctx,
		&invoicesrpc.SubscribeSingleInvoiceRequest{
			PaymentAddr: paymentAddr[:],
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

		fmt.Printf("%x: state=%v, preimage=%x\n", invoice.PaymentAddr,
			invoice.State, invoice.RPreimage)

		if invoice.State == lnrpc.Invoice_ACCEPTED {
			// Check amount paid
			amtOk := invoice.Value > 500

			// Check webshop inventory here.
			time.Sleep(2 * time.Second)
			stockOk := true

			settlePayment := amtOk && stockOk

			if settlePayment {
				_, err := invoicesClient.SettleAmp(
					ctxb,
					&invoicesrpc.SettleAmpMsg{
						PaymentAddr: paymentAddr[:],
					},
				)
				if err != nil {
					return err
				}
			} else {
				_, err := invoicesClient.CancelAmp(
					ctxb,
					&invoicesrpc.CancelAmpRequest{
						PaymentAddr: paymentAddr[:],
					},
				)
				if err != nil {
					return err
				}
			}
		}
	}
}
