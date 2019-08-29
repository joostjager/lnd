// +build routerrpc

package main

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/lightningnetwork/lnd"
	"github.com/lightningnetwork/lnd/lnrpc/routerrpc"
	"github.com/lightningnetwork/lnd/routing/route"
	"github.com/urfave/cli"
)

var buildRouteCommand = cli.Command{
	Name:     "buildroute",
	Category: "Payments",
	Usage:    "Build a route from a list of channel ids and/or pubkeys.",
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
			Value: lnd.DefaultBitcoinTimeLockDelta,
		},
		cli.StringFlag{
			Name:  "hops",
			Usage: "comma separated channel ids and/or hex pubkeys",
		},
		cli.BoolFlag{
			Name: "use_min_amt",
			Usage: "return the final route with the minimum " +
				"amount that it can carry",
		},
	},
}

func buildRoute(ctx *cli.Context) error {
	conn := getClientConn(ctx, false)
	defer conn.Close()

	client := routerrpc.NewRouterClient(conn)

	if !ctx.IsSet("hops") {
		return errors.New("hops required")
	}

	// Build list of hop addresses for the rpc.
	hops := strings.Split(ctx.String("hops"), ",")
	rpcHops := make([]*routerrpc.HopAddress, 0, len(hops))
	for _, k := range hops {
		var rpcHop *routerrpc.HopAddress
		pubkey, err := route.NewVertexFromStr(k)
		if err == nil {
			rpcHop = &routerrpc.HopAddress{
				Type: &routerrpc.HopAddress_Pubkey{
					Pubkey: pubkey[:],
				},
			}
		} else {
			chanID, err := strconv.ParseUint(k, 10, 64)
			if err != nil {
				return errors.New("unrecognized hop address")
			}

			rpcHop = &routerrpc.HopAddress{
				Type: &routerrpc.HopAddress_Channel{
					Channel: chanID,
				},
			}
		}

		rpcHops = append(rpcHops, rpcHop)
	}

	// Determine build mode.
	amtSet := ctx.IsSet("amt")
	useMinAmt := ctx.Bool("use_min_amt")

	var mode routerrpc.BuildRouteMode
	switch {
	case amtSet && !useMinAmt:
		mode = routerrpc.BuildRouteMode_AMT
	case amtSet && useMinAmt:
		mode = routerrpc.BuildRouteMode_SUBSTITUTE_MIN_AMT
	case !amtSet && useMinAmt:
		mode = routerrpc.BuildRouteMode_MIN_AMT
	default:
		return errors.New("must specify either amt or set use_min_amt")
	}

	// Call BuildRoute rpc.
	req := &routerrpc.BuildRouteRequest{
		AmtMsat:        ctx.Int64("amt") * 1000,
		FinalCltvDelta: int32(ctx.Int64("final_cltv_delta")),
		Hops:           rpcHops,
		Mode:           mode,
	}

	rpcCtx := context.Background()
	route, err := client.BuildRoute(rpcCtx, req)
	if err != nil {
		return err
	}

	printJSON(route)

	return nil
}
