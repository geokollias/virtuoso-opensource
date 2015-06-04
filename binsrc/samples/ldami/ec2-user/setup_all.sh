#!/bin/bash

START=1
END=$(head -n 1 hosts.conf | awk -F= '{print $2}')
for ((i=$START; i<=$END; i++ ))
do
    scp hosts.conf "a$i:./"
    ssh -t "a$i" "(sudo /home/ec2-user/setup_one.sh)"
    ssh -t -t "a$i" "(sudo ./mkvol.sh)" &
    sleep 1
done

wait


