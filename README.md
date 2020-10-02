# Lab2-MapReduce
# Theo Watkins and Alberto Rodriguez

# Overview

This is our distributed implementation of MapReduce for conducting a word count on text files.  Unfortunately we did not get the heartbeat protocol functioning as we had hoped, so there is no fault tolerance. We use the sync.WaitGroup library to ensure that our jobs finish execution.  


# Running the program

$ ./start.sh

The above command will run the program on the pg-metamorphosis.txt file and store the output in mr-distributed-out.  mr-sequential-out contains the output of running the given sequential version of the word count program which can be run with the command below.

$ ./sequential_start.sh

A diff of mr-distributed-out and mr-sequential-out will show that the outputs are identical. See ExampleRun.png for example.