#!/usr/bin/env bash

#
# basic map-reduce test
#

#RACE=

# comment this to run the tests without the Go race detector.
RACE=-race

# run the test in a fresh sub-directory.
rm -rf mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1
rm -f mr-*

# make sure software is freshly built.
(cd ../../cmd/apps/wc && go build $RACE -buildmode=plugin wc.go) || exit 1
(cd ../../cmd/apps/indexer && go build $RACE -buildmode=plugin indexer.go) || exit 1
(cd ../../cmd/apps/mtiming && go build $RACE -buildmode=plugin mtiming.go) || exit 1
(cd ../../cmd/apps/rtiming && go build $RACE -buildmode=plugin rtiming.go) || exit 1
(cd ../../cmd/apps/jobcount && go build $RACE -buildmode=plugin jobcount.go) || exit 1
(cd ../../cmd/apps/early_exit && go build $RACE -buildmode=plugin early_exit.go) || exit 1
(cd ../../cmd/apps/crash && go build $RACE -buildmode=plugin crash.go) || exit 1
(cd ../../cmd/apps/nocrash && go build $RACE -buildmode=plugin nocrash.go) || exit 1
(cd ../../cmd/coordinator && go build $RACE mrcoordinator.go) || exit 1
(cd ../../cmd/worker && go build $RACE mrworker.go) || exit 1
(cd ../../cmd/sequential && go build $RACE mrsequential.go) || exit 1

failed_any=0

#########################################################
# first word-count

# generate the correct output
../../cmd/sequential/mrsequential ../../cmd/apps/wc/wc.so ../../data/pg*txt || exit 1
sort mr-out-0 > mr-correct-wc.txt
rm -f mr-out*

echo '***' Starting wc test.

timeout -k 2s 180s ../../cmd/coordinator/mrcoordinator ../../data/pg*txt &
pid=$!

# give the coordinator time to create the sockets.
sleep 1

# start multiple workers.
timeout -k 2s 180s ../../cmd/worker/mrworker ../../cmd/apps/wc/wc.so &
timeout -k 2s 180s ../../cmd/worker/mrworker ../../cmd/apps/wc/wc.so &
timeout -k 2s 180s ../../cmd/worker/mrworker ../../cmd/apps/wc/wc.so &

# wait for the coordinator to exit.
wait $pid

# since workers are required to exit when a job is completely finished,
# and not before, that means the job has finished.
sort mr-out* | grep . > mr-wc-all
if cmp mr-wc-all mr-correct-wc.txt
then
  echo '---' wc test: PASS
else
  echo '---' wc output is not the same as mr-correct-wc.txt
  echo '---' wc test: FAIL
  failed_any=1
fi

# wait for remaining workers and coordinator to exit.
wait

#########################################################
# now indexer
rm -f mr-*

# generate the correct output
../../cmd/sequential/mrsequential ../../cmd/apps/indexer/indexer.so ../../data/pg*txt || exit 1
sort mr-out-0 > mr-correct-indexer.txt
rm -f mr-out*

echo '***' Starting indexer test.

timeout -k 2s 180s ../../cmd/coordinator/mrcoordinator ../../data/pg*txt &
sleep 1

# start multiple workers
timeout -k 2s 180s ../../cmd/worker/mrworker ../../cmd/apps/indexer/indexer.so &
timeout -k 2s 180s ../../cmd/worker/mrworker ../../cmd/apps/indexer/indexer.so

sort mr-out* | grep . > mr-indexer-all
if cmp mr-indexer-all mr-correct-indexer.txt
then
  echo '---' indexer test: PASS
else
  echo '---' indexer output is not the same as mr-correct-indexer.txt
  echo '---' indexer test: FAIL
  failed_any=1
fi

wait

#########################################################
echo '***' Starting map parallelism test.

rm -f mr-*

timeout -k 2s 180s ../../cmd/coordinator/mrcoordinator ../../data/pg*txt &
sleep 1

timeout -k 2s 180s ../../cmd/worker/mrworker ../../cmd/apps/mtiming/mtiming.so &
timeout -k 2s 180s ../../cmd/worker/mrworker ../../cmd/apps/mtiming/mtiming.so

NT=`cat mr-out* | grep '^times-' | wc -l | sed 's/ //g'`
if [ "$NT" != "2" ]
then
  echo '---' saw "$NT" workers rather than 2
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

if cat mr-out* | grep '^parallel.* 2' > /dev/null
then
  echo '---' map parallelism test: PASS
else
  echo '---' map workers did not run in parallel
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

wait


#########################################################
echo '***' Starting reduce parallelism test.

rm -f mr-*

timeout -k 2s 180s ../../cmd/coordinator/mrcoordinator ../../data/pg*txt &
sleep 1

timeout -k 2s 180s ../../cmd/worker/mrworker ../../cmd/apps/rtiming/rtiming.so &
timeout -k 2s 180s ../../cmd/worker/mrworker ../../cmd/apps/rtiming/rtiming.so

NT=`cat mr-out* | grep '^[a-z] 2' | wc -l | sed 's/ //g'`
if [ "$NT" -lt "2" ]
then
  echo '---' too few parallel reduces.
  echo '---' reduce parallelism test: FAIL
  failed_any=1
else
  echo '---' reduce parallelism test: PASS
fi

wait

#########################################################
echo '***' Starting job count test.

rm -f mr-*

timeout -k 2s 180s ../../cmd/coordinator/mrcoordinator ../../data/pg*txt &
sleep 1

timeout -k 2s 180s ../../cmd/worker/mrworker ../../cmd/apps/jobcount/jobcount.so &
timeout -k 2s 180s ../../cmd/worker/mrworker ../../cmd/apps/jobcount/jobcount.so
timeout -k 2s 180s ../../cmd/worker/mrworker ../../cmd/apps/jobcount/jobcount.so &
timeout -k 2s 180s ../../cmd/worker/mrworker ../../cmd/apps/jobcount/jobcount.so

NT=`cat mr-out* | awk '{print $2}'`
if [ "0$NT" -ne "8" ]
then
  echo '---' map jobs ran incorrect number of times "($NT != 8)"
  echo '---' job count test: FAIL
  failed_any=1
else
  echo '---' job count test: PASS
fi

wait

#########################################################
# test whether any worker or coordinator exits before the
# task has completed (i.e., all output files have been finalized)
rm -f mr-*

echo '***' Starting early exit test.

timeout -k 2s 180s ../../cmd/coordinator/mrcoordinator ../../data/pg*txt &

# give the coordinator time to create the sockets.
sleep 1

# start multiple workers.
timeout -k 2s 180s ../../cmd/worker/mrworker ../../cmd/apps/early_exit/early_exit.so &
timeout -k 2s 180s ../../cmd/worker/mrworker ../../cmd/apps/early_exit/early_exit.so &
timeout -k 2s 180s ../../cmd/worker/mrworker ../../cmd/apps/early_exit/early_exit.so &

# wait for any of the coord or workers to exit
# `jobs` ensures that any completed old processes from other tests
# are not waited upon
jobs &> /dev/null

# wait -n ## -n flag not available on bash in certain systems

initial_proc_count=`expr $(pgrep -c mrworker) + $(pgrep -c mrcoordinator)`
proc_count=${initial_proc_count}

# Busy wait loop: check if any background process has exited or not.
# proc_count decreases by one when a process exits.
while [ $proc_count -eq $initial_proc_count ]; do
    proc_count=`expr $(pgrep -c mrworker) + $(pgrep -c mrcoordinator)`
done

# a process has exited. this means that the output should be finalized
# otherwise, either a worker or the coordinator exited early
sort mr-out* | grep . > mr-wc-all-initial

# wait for remaining workers and coordinator to exit.
wait

# compare initial and final outputs
sort mr-out* | grep . > mr-wc-all-final
if cmp mr-wc-all-final mr-wc-all-initial
then
  echo '---' early exit test: PASS
else
  echo '---' output changed after first worker exited
  echo '---' early exit test: FAIL
  failed_any=1
fi
rm -f mr-*

#########################################################
echo '***' Starting crash test.

# generate the correct output
../../cmd/sequential/mrsequential ../../cmd/apps/nocrash/nocrash.so ../../data/pg*txt || exit 1
sort mr-out-0 > mr-correct-crash.txt
rm -f mr-out*

rm -f mr-done
(timeout -k 2s 180s ../../cmd/coordinator/mrcoordinator ../../data/pg*txt ; touch mr-done ) &
sleep 1

# start multiple workers
timeout -k 2s 180s ../../cmd/worker/mrworker ../../cmd/apps/crash/crash.so &

# mimic rpc.go's coordinatorSock()
SOCKNAME=/var/tmp/5840-mr-`id -u`

( while [ -e $SOCKNAME -a ! -f mr-done ]
  do
    timeout -k 2s 180s ../../cmd/worker/mrworker ../../cmd/apps/crash/crash.so
    sleep 1
  done ) &

( while [ -e $SOCKNAME -a ! -f mr-done ]
  do
    timeout -k 2s 180s ../../cmd/worker/mrworker ../../cmd/apps/crash/crash.so
    sleep 1
  done ) &

while [ -e $SOCKNAME -a ! -f mr-done ]
do
  timeout -k 2s 180s ../../cmd/worker/mrworker ../../cmd/apps/crash/crash.so
  sleep 1
done

wait

rm $SOCKNAME
sort mr-out* | grep . > mr-crash-all
if cmp mr-crash-all mr-correct-crash.txt
then
  echo '---' crash test: PASS
else
  echo '---' crash output is not the same as mr-correct-crash.txt
  echo '---' crash test: FAIL
  failed_any=1
fi

#########################################################
if [ $failed_any -eq 0 ]; then
    echo '***' PASSED ALL TESTS
else
    echo '***' FAILED SOME TESTS
    exit 1
fi
