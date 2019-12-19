#!/bin/bash

echo > $HOME/.lnd/logs/bitcoin/regtest/lnd.log
echo > $HOME/.eclair/eclair.log

LNCLI="$HOME/lightninglabs/lnd/lncli-debug --network=regtest"
ECLAIRCLI="$HOME/eclair/eclair-cli.sh -a localhost:8081 -p foobar"
ID1=`od -vAn -N4 -tu4 < /dev/urandom`
ID2=`od -vAn -N4 -tu4 < /dev/urandom`

BOLT1=`$ECLAIRCLI createinvoice --amountMsat=100000000 --description="test" | jq -r .serialized`
#BOLT2=`$LNCLI addinvoice 10 | jq -r .pay_req`

$LNCLI --network=regtest payinvoice -f $BOLT1
#$ECLAIRCLI payinvoice --invoice=$BOLT2

# run lnd in loop:
# make && while true; do ./lnd-debug ; sleep 1 ; done
