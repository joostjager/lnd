// +build routerrpc

package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/lightningnetwork/lnd/lntypes"

	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/lnrpc/routerrpc"
	"github.com/urfave/cli"
)

var testMcCommand = cli.Command{
	Name:     "testmc",
	Category: "Payments",
	Action:   actionDecorator(testMc),
}

func testMc(ctx *cli.Context) error {
	ctxb := context.Background()

	conn := getClientConn(ctx, false)
	defer conn.Close()

	client := lnrpc.NewLightningClient(conn)

	infoResp, err := client.GetInfo(ctxb, &lnrpc.GetInfoRequest{})
	if err != nil {
		return err
	}
	self, err := hex.DecodeString(infoResp.IdentityPubkey)
	if err != nil {
		return err
	}

	// Find longer distance destinations

	// graph, err := client.DescribeGraph(ctxb, &lnrpc.ChannelGraphRequest{})
	// if err != nil {
	// 	return err
	// }

	// for _, n := range graph.Nodes {
	// 	req := &lnrpc.QueryRoutesRequest{
	// 		PubKey: n.PubKey,
	// 		Amt:    1500000,
	// 		FeeLimit: &lnrpc.FeeLimit{
	// 			Limit: &lnrpc.FeeLimit_Fixed{
	// 				Fixed: 100000,
	// 			},
	// 		},
	// 		FinalCltvDelta:    40,
	// 		UseMissionControl: true,
	// 	}
	// 	route, err := client.QueryRoutes(ctxb, req)
	// 	if err != nil {
	// 		continue
	// 	}

	// 	hops := len(route.Routes[0].Hops)
	// 	if hops != 3 {
	// 		continue
	// 	}
	// 	fmt.Printf("%v\n", n.PubKey)
	// }

	routerClient := routerrpc.NewRouterClient(conn)

	args := ctx.Args()

	maxAmt, err := strconv.ParseInt(args.First(), 10, 64)
	if err != nil {
		return fmt.Errorf("unable to decode payment amount: %v", err)
	}

	args = args.Tail()

	var dests []string
	if args.Present() {
		dests = append(dests, args.First())
	} else {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			dest := strings.TrimSpace(scanner.Text())
			if dest == "" {
				continue
			}
			dests = append(dests, scanner.Text())
		}
	}

	amts := []int64{maxAmt / 100, maxAmt / 10, maxAmt}

	wSummary := csv.NewWriter(os.Stdout)
	wSummary.Write([]string{
		"payment_id", "dest", "amt", "queryroutes_time_ms",
		"sendtoroute_time_ms", "total_time_ms", "nof_attempts",
		"outcome", "fee_sat", "nof_hops",
	})
	wSummary.Flush()

	w := csv.NewWriter(os.Stderr)
	w.Write([]string{
		"payment_id", "hash", "dest", "amt", "queryroutes_time_ms",
		"sendtoroute_time_ms", "failure_code", "failure_src_idx", "fee_sat",
		"route",
	})
	w.Flush()

	paymentId := 0
	for _, dest := range dests {

	amtLoop:
		for _, amt := range amts {
			paymentId++

			req := &lnrpc.QueryRoutesRequest{
				PubKey: dest,
				Amt:    amt,
				FeeLimit: &lnrpc.FeeLimit{
					Limit: &lnrpc.FeeLimit_Fixed{
						Fixed: amt / 10,
					},
				},
				FinalCltvDelta:    40,
				UseMissionControl: true,
			}

			writeError := func(err string) {
				wSummary.Write([]string{
					strconv.Itoa(paymentId),
					fmt.Sprintf("%v", dest),
					strconv.FormatInt(amt, 10),
					"",
					"",
					"",
					"",
					err,
					"",
					"",
				})
				wSummary.Flush()
			}

			attempts := 0
			var totalQrTime, totalSendTime int

		paymentLoop:
			for {
				attempts++
				qrStart := time.Now()

				ctx, cancel := context.WithTimeout(ctxb, 5*time.Minute)
				defer cancel()
				routes, err := client.QueryRoutes(ctx, req)
				if err != nil {
					w.Write([]string{
						strconv.Itoa(paymentId),
						"",
						fmt.Sprintf("%v", dest),
						strconv.FormatInt(amt, 10),
						"",
						"",
						fmt.Sprintf("QueryRoutes error: %v", err),
					})
					w.Flush()

					writeError("queryroutes error")

					break amtLoop
				}

				sendStart := time.Now()

				rt := routes.Routes[0]

				// printRespJSON(route)

				var probeHash lntypes.Hash
				if _, err := rand.Read(probeHash[:]); err != nil {
					return err
				}

				ctx, cancel = context.WithTimeout(ctxb, 5*time.Minute)
				defer cancel()
				sendResp, err := routerClient.SendToRoute(ctx, &routerrpc.SendToRouteRequest{
					PaymentHash: probeHash[:],
					Route:       rt,
				})
				if err != nil {
					w.Write([]string{
						strconv.Itoa(paymentId),
						"",
						fmt.Sprintf("%v", dest),
						strconv.FormatInt(amt, 10),
						"",
						"",
						fmt.Sprintf("SendToRoute error: %v", err),
					})
					w.Flush()
					writeError("sendtoroute error")
					break amtLoop
				}

				// printRespJSON(sendResp)

				qrTime := int(sendStart.Sub(qrStart) / 1e6)
				sendTime := int(time.Now().Sub(sendStart) / 1e6)

				totalQrTime += qrTime
				totalSendTime += sendTime

				var b strings.Builder
				b.WriteString(fmt.Sprintf("%x -", self[:3]))
				for _, p := range rt.Hops {
					b.WriteString(fmt.Sprintf("(%v)- %v - ", p.ChanId, p.PubKey[:6]))
				}

				w.Write([]string{
					strconv.Itoa(paymentId),
					fmt.Sprintf("%x", probeHash[:3]),
					fmt.Sprintf("%v", dest),
					strconv.FormatInt(amt, 10),
					strconv.FormatInt(int64(qrTime), 10),
					strconv.FormatInt(int64(sendTime), 10),
					sendResp.Failure.Code.String(),
					fmt.Sprintf("%x", sendResp.Failure.FailureSourceIndex),
					strconv.FormatInt(rt.TotalFees, 10),
					b.String(),
				})
				w.Flush()

				switch sendResp.Failure.Code {
				case routerrpc.Failure_UNKNOWN_PAYMENT_HASH:
					wSummary.Write([]string{
						strconv.Itoa(paymentId),
						fmt.Sprintf("%v", dest),
						strconv.FormatInt(amt, 10),
						strconv.FormatInt(int64(totalQrTime), 10),
						strconv.FormatInt(int64(totalSendTime), 10),
						strconv.FormatInt(int64(totalQrTime+totalSendTime), 10),
						strconv.Itoa(attempts),
						"success",
						strconv.FormatInt(rt.TotalFees, 10),
						strconv.Itoa(len(rt.Hops)),
					})
					wSummary.Flush()

					break paymentLoop
				}
			}
		}
	}

	return nil
}
