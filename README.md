# k-Neighborhood-Benchmark
K-Neighborhood Scoring Algorithm is used to build a benchamrk suit that evaluates serial, parallel and MapReduce framework.

# Makefile instruction

Set the HADOOP_HOME in the makefile according to the hadoop installation in the system. 
Also edit the K and REPETITIONS as per user wishes.

To compile the code and build the jar file
    
    make build

To run the program on big-corpus and books with optional parameters K,REPETITIONS

    make run-hadoop

To clean the output files and class files

    make clean
