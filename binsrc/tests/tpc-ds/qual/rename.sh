#!/bin/bash
for i in `seq 98 -1 0`
do
    j=$(echo "$i+1" | bc)
    mv $i.sql $j.sql
done
