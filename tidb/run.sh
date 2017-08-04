#! /bin/sh

rm failed.log

for test in "bank" "sets" "register"
do
    for nemesis in "none" "parts" "majority-ring" "start-stop-2" "start-kill-2"
        do
	        lein run test --test ${test} --nemesis ${nemesis} --time-limit 60 --recovery-time 30 --concurrency 10
            if [ $? -ne 0 ]
            then
                echo ${test} ${nemesis} >> failed.log
            fi
    done
done

if [ ! -f "failed.log" ]
then
    echo "test passed :)"
    exit 0
else
    echo "test failed :(, see failed.log for detail"
    exit 1
fi
