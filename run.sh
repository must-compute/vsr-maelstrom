#!/bin/bash

MAELSTROM="./maelstrom/maelstrom"
BINARY="./target/debug/maelstrom-tutorial"

if [ "$1" = "lin-kv" ]; then
  cargo build && $MAELSTROM test -w lin-kv --bin $BINARY --time-limit 10 --log-stderr --node-count 3 --concurrency 2n --rate 100 --latency 200
elif [ "$1" = "lin-kv-nemesis" ]; then
  cargo build && $MAELSTROM test -w lin-kv --bin $BINARY --time-limit 60 --log-stderr --node-count 3 --concurrency 4n --rate 30 --nemesis partition --nemesis-interval 10 --test-count 10
else
  echo "unknown command"
fi
