#!/bin/bash

wget -O - https://nodes.lightning.computer/availability/v1/btctestnet.json | jq ".scores[].public_key" -r | head -n $1
