// +build routerrpc

package main

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"

	"github.com/lightningnetwork/lnd/lnrpc/routerrpc"

	"github.com/urfave/cli"
)

var buildRouteCommand = cli.Command{
	Name:     "buildroute",
	Category: "Payments",
	Usage:    "Build a route from pubkeys.",
	Action:   actionDecorator(buildRoute),
	Flags: []cli.Flag{
		cli.Int64Flag{
			Name:  "amt",
			Usage: "the amount to send expressed in satoshis",
		},
		cli.Int64Flag{
			Name: "final_cltv_delta",
			Usage: "number of blocks the last hop has to reveal " +
				"the preimage",
		},
		cli.StringFlag{
			Name:  "pubkeys",
			Usage: "comma separated pubkeys",
		},
	},
}

func buildRoute(ctx *cli.Context) error {
	conn := getClientConn(ctx, false)
	defer conn.Close()

	client := routerrpc.NewRouterClient(conn)

	if !ctx.IsSet("amt") {
		return errors.New("amt required")
	}
	if !ctx.IsSet("final_cltv_delta") {
		return errors.New("final_cltv_delta required")
	}
	if !ctx.IsSet("pubkeys") {
		return errors.New("pubkeys required")
	}

	pubkeys := strings.Split(ctx.String("pubkeys"), ",")
	rpcPubkeys := make([][]byte, 0, len(pubkeys))
	for _, k := range pubkeys {
		kBytes, err := hex.DecodeString(k)
		if err != nil {
			return err
		}
		if len(kBytes) != 33 {
			return errors.New("invalid key len")
		}
		rpcPubkeys = append(rpcPubkeys, kBytes)
	}

	req := &routerrpc.BuildRouteRequest{
		Amt:            ctx.Int64("amt"),
		FinalCltvDelta: int32(ctx.Int64("final_cltv_delta")),
		Hops:           rpcPubkeys,
	}

	rpcCtx := context.Background()
	route, err := client.BuildRoute(rpcCtx, req)
	if err != nil {
		return err
	}

	printJSON(route)

	return nil
}
