#!/bin/bash

# set -e

CLI="/home/joost/repo/bitcoin/src/bitcoin-cli"

$CLI --version

anchoramt=2
amt=1
addr1=`$CLI getnewaddress`
addr2=`$CLI getnewaddress`

committx=`$CLI sendmany "" "{\"$addr1\":$amt,\"$addr2\":$anchoramt}"`
tx=$committx
outidx=`$CLI getrawtransaction $tx true | jq -r ".vout[] | select(.value==$amt).n"`
outidx2=`$CLI getrawtransaction $tx true | jq -r ".vout[] | select(.value==$anchoramt).n"`

echo Commitment tx: $tx

echo Spend out $outidx with tree

while true
do
        addr=`$CLI getnewaddress`
        amt=`echo "$amt-0.01" | bc`
        txhex=`$CLI createrawtransaction "[{\"txid\":\"$tx\",\"vout\":$outidx}]" "[{\"$addr\":0$amt}]"`
        signedtx=$($CLI -named signrawtransactionwithwallet hexstring=$txhex | jq -r '.hex')
        tx=`$CLI sendrawtransaction $signedtx` 

        if [ $? -ne 0 ]
        then
                break
        fi

        echo $tx
done

echo Try spend other anchor with idx $outidx2

addr=`$CLI getnewaddress`
amt="1.99"
echo "Sending $amt to $addr"
txhex=`$CLI createrawtransaction "[{\"txid\":\"$committx\",\"vout\":$outidx2}]" "[{\"$addr\":$amt}]"`
signedtx=$($CLI -named signrawtransactionwithwallet hexstring=$txhex | jq -r '.hex')
tx=`$CLI sendrawtransaction $signedtx` 

echo "Other anchor spend: $tx"