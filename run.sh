#!/bin/bash

MAELSTROM="./maelstrom/maelstrom"
BINARY="./target/debug/maelstrom-tutorial"

if [ "$1" = "lin-kv" ]; then
  cargo build && $MAELSTROM test -w lin-kv --bin $BINARY --time-limit 20 --log-stderr --node-count 3 --concurrency 2n --rate 1 # --latency 120
elif [ "$1" = "lin-kv-nemesis" ]; then
  cargo build && $MAELSTROM test -w lin-kv --bin $BINARY --time-limit 15 --log-stderr --node-count 3 --concurrency 4n --rate 20 --nemesis partition --nemesis-interval 3 # --latency 120
else
  echo "unknown command"
fi
