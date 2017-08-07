#! /bin/sh

rm failed.log

for test in "bank" "sets"
do
    for nemesis in "none" "parts" "majority-ring" "start-stop-2" "start-kill-2"
        do
	        echo lein run test --test ${test} --nemesis ${nemesis} --time-limit $t --concurrency 10
            if [ $? -ne 0 ]
            then
                echo ${test} ${nemesis} >> failed.log
            fi
    done
done

for nemesis in "none" "parts" "majority-ring" "start-stop-2"
do
    if [ ${nemesis} = "start-stop-2" ]
    then
        t=30
    else
        t=60
    fi
    echo lein run test --test "register" --nemesis ${nemesis} --time-limit $t --concurrency 10
done

if [ ! -f "failed.log" ]
then
    echo "test passed :)"
    exit 0
else
    echo "test failed :(, see failed.log for detail"
    exit 1
fi
