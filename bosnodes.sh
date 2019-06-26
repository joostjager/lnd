#!/bin/bash

wget -O - https://nodes.lightning.computer/availability/v1/btc.json | jq ".scores[].public_key" -r | head -n $1
