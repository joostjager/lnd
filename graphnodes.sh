#!/bin/bash

lncli describegraph | jq ".nodes[].pub_key" -r | head -n $1

