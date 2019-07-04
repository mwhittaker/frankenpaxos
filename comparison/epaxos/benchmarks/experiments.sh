#!/bin/sh

cr_start=0
cr_end=100

wp_start=0
wp_end=100

nr_start=1
nr_end=7

for cr in 42;
do
    for wp in `seq 0 100`;
    do
        for nr in `seq 1 4`;
        do
            for alg in e g m;
            do
                for fast in true false;
                do
                    for thrifty in true false;
                    do
                        for eps in 0 1;
                        do
                            for num_servers in 3 5;
                            do
                                echo $cr $wp $nr $alg $fast $thrifty $eps $num_servers
                                servers=()
                                ../bin/master -N $num_servers &
                                master_id=`echo $!`

                                for cnt in `seq 1 $num_servers`;
                                do
                                    port=`expr 7070 + $cnt`
                                    ../bin/server -port $port -thrifty $thrifty -$alg true &
                                    server_id=`echo $!`
                                    echo "../bin/server -port $port -thrifty $thrifty -$alg true &"
                                    servers+=($server_id)
                                done
                                sleep 5
                                num_req=`echo 10^$nr | bc`
                                ../bin/client -c $cr -w $wp -q $num_req -eps $eps -f $fast | grep "Test took" | sed s/"Test took "//g >> client01.txt
                                echo $cr $wp $nr

                                kill -9 $master_id
                                for i in `seq 1 $num_servers`;
                                do
                                    index=`expr $i - 1`
                                    echo ${servers[$index]}
                                    kill -9 ${servers[$index]}
                                done
                            done
                        done
                    done
                done
            done
        done
    done
done
