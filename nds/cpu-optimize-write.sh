#!/bin/bash

RUN=${RUN:-1}

DELTA_DIR=/home/jihoons/data/tpcds/sf=10/delta-cpu

for i in $( seq 1 $RUN )
do
	rm -rf $DELTA_DIR/store_sales
	./spark-submit-template convert_submit_cpu_delta.template optimize_write.py --type cpu
done
