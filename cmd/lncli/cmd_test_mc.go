// +build routerrpc

package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
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

	routerClient := routerrpc.NewRouterClient(conn)

	infoResp, err := client.GetInfo(ctxb, &lnrpc.GetInfoRequest{})
	if err != nil {
		return err
	}
	self := infoResp.IdentityPubkey
	fmt.Fprintf(os.Stderr, "Self: %v\n", self)

	graphResp, err := client.DescribeGraph(ctxb, &lnrpc.ChannelGraphRequest{})
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "Graph: nodes=%v\n", len(graphResp.Nodes))

	alias := make(map[string]string)
	for _, n := range graphResp.Nodes {
		alias[n.PubKey[:6]] = n.Alias
	}

	chanResp, err := client.ListChannels(ctxb, &lnrpc.ListChannelsRequest{})
	if err != nil {
		return err
	}
	channelPeers := make(map[string]struct{})
	for _, c := range chanResp.Channels {
		channelPeers[c.RemotePubkey] = struct{}{}
	}

	p := prober{
		graphResp:    graphResp,
		client:       client,
		routerClient: routerClient,
		self:         self,
		channelPeers: channelPeers,
		alias:        alias,
	}

	p.run()

	return nil
}

type prober struct {
	self         string
	probed       map[string]struct{}
	alias        map[string]string
	graphResp    *lnrpc.ChannelGraph
	client       lnrpc.LightningClient
	routerClient routerrpc.RouterClient
	channelPeers map[string]struct{}

	results map[pair]int
	nodes   map[string]struct{}
}

type pair struct {
	from, to string
}

func (p *prober) run() {
	p.probed = make(map[string]struct{})
	p.results = make(map[pair]int)
	p.nodes = make(map[string]struct{})

	p.probePeers(0, []string{p.self})

	fmt.Printf(`
	<html>
<head>
    <script type="text/javascript" src="https://cdnjs.cloudflare.com/ajax/libs/vis/4.21.0/vis.min.js"></script>
    <link href="https://cdnjs.cloudflare.com/ajax/libs/vis/4.21.0/vis.min.css" rel="stylesheet" type="text/css" />

    <style type="text/css">
    </style>
</head>
<body>
<div id="mynetwork"></div>

<script type="text/javascript">
    // create an array with nodes
    var nodes = new vis.DataSet([
    `)

	ids := make(map[string]int)
	id := 1
	for n, _ := range p.nodes {
		color := "#b0c0ff"
		if n == p.self[:6] {
			color = "#ffc0c0"
		}

		fmt.Printf("{id: %v, label: '%v', color: '%v'},\n",
			id, p.alias[n], color)
		ids[n] = id
		id++
	}

	fmt.Printf(`
    ]);

    // create an array with edges
    var edges = new vis.DataSet([
`)
	for pair, latency := range p.results {
		f := ids[pair.from]
		t := ids[pair.to]
		fmt.Printf("{from: %v, to: %v, label:\"%v ms\"},\n", f, t, latency)
	}

	fmt.Printf(`
    ]);


    // create a network
    var container = document.getElementById('mynetwork');

    // provide the data in the vis format
    var data = {
        nodes: nodes,
        edges: edges
    };
    var options = {};

    // initialize your network!
    var network = new vis.Network(container, data, options);
</script>
</body>
</html>
	`)
}

func (p *prober) getPeers(from string) map[string]struct{} {
	if from == p.self {
		return p.channelPeers
	}

	peers := make(map[string]struct{})
	for _, e := range p.graphResp.Edges {
		if e.Node1Pub == from {
			peers[e.Node2Pub] = struct{}{}
		}
		if e.Node2Pub == from {
			peers[e.Node1Pub] = struct{}{}
		}
	}
	return peers
}

func (p *prober) probe(hops []string) (int, error) {
	fmt.Fprintf(os.Stderr, "Probing:")

	ctxb := context.Background()

	amt := int64(1000)

	rpcHops := make([][]byte, 0)
	for _, h := range hops {
		fmt.Fprintf(os.Stderr, " %v", h[:6])
		rpcHop, err := hex.DecodeString(h)
		if err != nil {
			return 0, err
		}
		rpcHops = append(rpcHops, rpcHop)
	}
	fmt.Fprintln(os.Stderr)
	req := &routerrpc.BuildRouteRequest{
		Hops:           rpcHops,
		Amt:            amt,
		FinalCltvDelta: 40,
	}

	ctx, cancel := context.WithTimeout(ctxb, 5*time.Minute)
	defer cancel()
	routes, err := p.routerClient.BuildRoute(ctx, req)
	if err != nil {
		return 0, fmt.Errorf("queryroutes error: %v", err)
	}

	rt := routes.Route

	// printRespJSON(rt)

	var probeHash lntypes.Hash
	if _, err := rand.Read(probeHash[:]); err != nil {
		return 0, err
	}

	const attempts = 3
	times := make([]int, attempts)
	for cnt := 0; cnt < attempts; cnt++ {
		sendStart := time.Now()

		ctx, cancel = context.WithTimeout(ctxb, 15*time.Second)
		defer cancel()
		sendResp, err := p.routerClient.SendToRoute(ctx, &routerrpc.SendToRouteRequest{
			PaymentHash: probeHash[:],
			Route:       rt,
		})
		if err != nil {
			return 0, fmt.Errorf("sendtoroute error: %v", err)
		}
		if sendResp.Failure.Code != routerrpc.Failure_UNKNOWN_PAYMENT_HASH {
			return 0, fmt.Errorf("sendtoroute error: %v", sendResp.Failure.Code)
		}
		sendTime := int(time.Now().Sub(sendStart) / 1e6)
		times[cnt] = sendTime

		fmt.Fprintf(os.Stderr, "sendtoroute success, time: %v\n", sendTime)
	}

	sort.Ints(times)

	median := times[(attempts-1)/2]

	return median, nil
}

func (p *prober) probePeers(latency int, path []string) {
	// Limit depth
	if len(path) > 3 {
		return
	}

	lastNode := path[len(path)-1]
	p.probed[lastNode] = struct{}{}

	peers := p.getPeers(lastNode)

	cnt := 0
	for peer, _ := range peers {
		// Prevent going back.
		if _, ok := p.probed[peer]; ok {
			continue
		}

		// Execute probe
		newPath := make([]string, len(path)+1)
		copy(newPath[:], path[:])
		newPath[len(path)] = peer
		newLatency, err := p.probe(newPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "latency %v <-> %v: %v ms\n",
				lastNode[:6], peer[:6], newLatency-latency)

			pair := pair{
				from: lastNode[:6],
				to:   peer[:6],
			}
			p.results[pair] = newLatency - latency
			p.nodes[pair.from] = struct{}{}
			p.nodes[pair.to] = struct{}{}

			p.probePeers(newLatency, newPath)
		}

		if lastNode != p.self {
			cnt++
			// Probe max nof peers
			if cnt >= 3 {
				break
			}
		}
	}
}
