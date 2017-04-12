#! /usr/bin/env bash

out_dir='matrix_out_big'
#reducers_count=3

# Build project
ant clean && ant
# Remove previous results
hadoop fs -rm -r -skipTrash $out_dir*
# Run task
hadoop jar jar/MatrixMultiplicator.jar ru.mipt.MatrixMultiplicator /data/matrix/A /data/matrix/B $out_dir &&

# Check results
for num in `seq 0 $[$reducers_count - 1]`
do
    hadoop fs -cat $out_dir/part-r-0000$num | head
done
