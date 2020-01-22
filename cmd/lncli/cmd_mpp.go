package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/record"
	"google.golang.org/grpc"

	"github.com/lightningnetwork/lnd/lnrpc/routerrpc"
	"github.com/urfave/cli"
)

var mppCommand = cli.Command{
	Name:     "mpp",
	Category: "Payments",
	Action:   mpp,
}

func mpp(ctx *cli.Context) error {
	// Show command help if no arguments provided.
	if ctx.NArg() == 0 {
		_ = cli.ShowCommandHelp(ctx, "mpp")
		return nil
	}

	executor, err := newMppExecuter(ctx)
	if err != nil {
		return err
	}
	defer executor.closeConnection()

	err = executor.launch()
	if err != nil {
		return err
	}

	return nil
}

type mppExecutor struct {
	conn         *grpc.ClientConn
	mainClient   lnrpc.LightningClient
	routerClient routerrpc.RouterClient
	dest         string
	amt          int64
	invoiceHash  lntypes.Hash
	paymentAddr  []byte
}

func newMppExecuter(ctx *cli.Context) (*mppExecutor, error) {
	conn := getClientConn(ctx, false)

	mainClient := lnrpc.NewLightningClient(conn)
	routerClient := routerrpc.NewRouterClient(conn)

	payReqText := ctx.Args().First()
	payReq, err := mainClient.DecodePayReq(context.Background(), &lnrpc.PayReqString{
		PayReq: payReqText,
	})
	if err != nil {
		return nil, err
	}

	invoiceHash, err := lntypes.MakeHashFromStr(payReq.PaymentHash)
	if err != nil {
		return nil, err
	}

	return &mppExecutor{
		mainClient:   mainClient,
		routerClient: routerClient,
		dest:         payReq.Destination,
		amt:          payReq.NumSatoshis,
		conn:         conn,
		invoiceHash:  invoiceHash,
		paymentAddr:  payReq.PaymentAddr,
	}, nil
}

func (m *mppExecutor) closeConnection() {
	m.conn.Close()
}

type launchResult struct {
	id      int
	failure *routerrpc.Failure
	amt     int64
	err     error
}

const (
	minAmt = 5000
)

func (m *mppExecutor) launch() error {
	resultChan := make(chan launchResult, 0)

	var amtPaid, amtInFlight int64
	maxAmt := int64(math.MaxInt64)

	reduceMax := func(failedAmt int64) {
		newMax := failedAmt / 2
		if newMax < maxAmt {
			maxAmt = newMax
		}
	}

	nextId := 0
	inFlight := make(map[int]int64, 0)
	inFlightStr := func() string {
		var b bytes.Buffer
		first := true
		for id, amt := range inFlight {
			if first {
				first = false
			} else {
				fmt.Fprintf(&b, ", ")
			}
			fmt.Fprintf(&b, "%v:%v", id, amt)
		}
		return b.String()
	}

	for {
		shardAmt := m.amt - amtPaid - amtInFlight
		if shardAmt > maxAmt {
			shardAmt = maxAmt
		}

		// Launch shard
		if shardAmt > minAmt {
			id := nextId
			nextId++

			route, err := m.launchShard(id, shardAmt, resultChan)
			if err != nil {
				return err
			}

			if route != nil {
				inFlight[id] = shardAmt
				amtInFlight += shardAmt

				path := route.Hops
				pathText := fmt.Sprintf("%v", path[0].ChanId)
				for _, h := range path[1:] {
					pathText += fmt.Sprintf(" -> %v", h.ChanId)
				}
				fmt.Printf("%v: Launched shard for amt=%v, path=%v, timelock=%v (%v)\n",
					id, shardAmt, pathText, route.TotalTimeLock, inFlightStr())

			} else {
				fmt.Printf("%v: No route for amt=%v\n", id, shardAmt)
				reduceMax(shardAmt)
			}
		}

		// Wait for a result to become available.
		select {
		case r := <-resultChan:
			if r.err != nil {
				return r.err
			}

			amtInFlight -= r.amt
			delete(inFlight, r.id)
			if r.failure == nil {
				fmt.Printf("%v: Partial payment success for amt=%v (%v)\n", r.id, r.amt, inFlightStr())

				amtPaid += r.amt

				if amtPaid == m.amt {
					fmt.Printf("Payment complete\n")
					return nil
				}
			} else {
				fmt.Printf("%v: Partial payment failed for amt=%v: %v @ %v (%v)\n",
					r.id, r.amt, r.failure.Code, r.failure.FailureSourceIndex, inFlightStr())

				switch r.failure.Code {
				case routerrpc.Failure_INCORRECT_OR_UNKNOWN_PAYMENT_DETAILS:
					return errors.New("Incorrect details")
				case routerrpc.Failure_MPP_TIMEOUT:
					// Just retry.
				case routerrpc.Failure_TEMPORARY_CHANNEL_FAILURE:
					// reduceMax(r.amt)
				default:
					// Just retry.
				}
			}

		case <-time.After(time.Millisecond * 100):
		}
	}
}

func (m *mppExecutor) launchShard(id int, amt int64, resultChan chan launchResult) (
	*lnrpc.Route, error) {

	ctxb := context.Background()

	var preimage lntypes.Preimage
	if _, err := rand.Read(preimage[:]); err != nil {
		return nil, err
	}

	customRecords := map[uint64][]byte{
		record.HtlcPreimage: preimage[:],
		record.InvoiceHash:  m.invoiceHash[:],
	}

	routeResp, err := m.mainClient.QueryRoutes(ctxb, &lnrpc.QueryRoutesRequest{
		Amt:               amt,
		FinalCltvDelta:    40,
		PubKey:            m.dest,
		UseMissionControl: true,
		DestCustomRecords: customRecords,
		DestFeatures:      []lnrpc.FeatureBit{lnrpc.FeatureBit_MPP_OPT, lnrpc.FeatureBit_TLV_ONION_OPT, lnrpc.FeatureBit_PAYMENT_ADDR_OPT},
	})
	if err != nil {
		fmt.Printf("%v: query routes for amt %v: %v\n", id, amt, err)
		return nil, nil
	}
	route := routeResp.Routes[0]

	hash := preimage.Hash()

	finalHop := route.Hops[len(route.Hops)-1]

	finalHop.MppRecord = &lnrpc.MPPRecord{
		TotalAmtMsat: m.amt * 1000,
		PaymentAddr:  m.paymentAddr,
	}

	sendReq := &routerrpc.SendToRouteRequest{
		PaymentHash: hash[:],
		Route:       route,
	}

	go func() {
		resp, err := m.routerClient.SendToRoute(ctxb, sendReq)
		if err != nil {
			resultChan <- launchResult{id: id, err: err}
			return
		}

		if len(resp.Preimage) > 0 {

			resultChan <- launchResult{id: id, amt: amt}
			return
		}

		resultChan <- launchResult{id: id, failure: resp.Failure, amt: amt}
	}()

	return route, nil
}
