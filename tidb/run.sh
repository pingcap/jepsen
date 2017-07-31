#! /bin/sh
if [ $# -lt 6 ]
then
  echo "bad argument :(\n"
  echo "TiDB Jepsen argument:"
  echo "test name : bank"
  echo "nemesis1: none parts majority-ring start-stop-2 start-kill-2"
  echo "nemesis2: none parts majority-ring start-stop-2 start-kill-2"
  echo "time-limit: 60, 180, etc"
  echo "concurrency: 5, 10, etc"
  echo "test-count: >= 1"
  echo "for example: ./run.sh bank parts start-stop-2 60 5 10 stands for running bank test 5 times, each for 60 seconds, with nemesis parts and start-stop-2, the concurrency is 10"
else
  lein run test --test $1 --nemesis $2 --nemesis2 $3 --time-limit $4 --test-count $5 --concurrency $6
fi
