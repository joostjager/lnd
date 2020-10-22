package main

import (
	"context"
	"fmt"

	"github.com/lightningnetwork/lnd/lnrpc/walletrpc"

	"github.com/btcsuite/btcd/chaincfg/chainhash"

	"github.com/urfave/cli"
)

var coinFilterCommand = cli.Command{
	Name:   "coinfilter",
	Action: actionDecorator(coinFilter),
}

func coinFilter(ctx *cli.Context) error {
	walletClient, cleanUp := getWalletClient(ctx)
	defer cleanUp()

	stream, err := walletClient.RegisterCoinFilter(context.Background())
	if err != nil {
		return err
	}

	for {
		request, err := stream.Recv()
		if err != nil {
			return err
		}

		hash, err := chainhash.NewHash(request.Outpoint.TxidBytes)
		if err != nil {
			return err
		}

		fmt.Printf("Coin filter request for %v:%v\n",
			hash, request.Outpoint.OutputIndex)

		stream.Send(&walletrpc.CoinFilterResponse{
			Outpoint: request.Outpoint,
			Include:  true,
		})
	}
}
