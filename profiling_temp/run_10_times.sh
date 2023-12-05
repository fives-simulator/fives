#!/bin/bash


for i in {0..9..1}
do
    # nice -n -20 ../build/storalloc_wrench ../configs/theta_config.yml ../../raw_data_processing/theta/theta2022_week4.yaml perf_test --wrench-default-control-message-size=0 --wrench-mailbox-pool-size=50000
    nice -n -20 ../build/storalloc_wrench ../configs/theta_config.yml ../../raw_data_processing/theta/theta2022_week4.yaml perf_test
done
