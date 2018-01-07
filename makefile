REPETITIONS = 1
K = 3

HADOOP_HOME = /usr/local/Cellar/hadoop/2.8.1/libexec
MY_CLASSPATH = $(HADOOP_HOME)/share/hadoop/common/hadoop-common-2.8.1.jar:$(HADOOP_HOME)/share/hadoop/mapreduce/hadoop-mapreduce-client-common-2.8.1.jar:$(HADOOP_HOME)/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.8.1.jar:out:.

all: build run

build: compile jar

compile:
	javac -cp $(MY_CLASSPATH) -d out ./src/org/neu/**/*.java
	javac -cp $(MY_CLASSPATH) -d out ./src/org/neu/*.java

jar:
	jar cvfm InitNeighborhoodScorerMapReduce.jar META-INF/MANIFEST.MF -C out/ .

run-hadoop:
	$(HADOOP_HOME)/bin/hadoop jar InitNeighborhoodScorerMapReduce.jar $(REPETITIONS) $(K) input/big-corpus/ output
	$(HADOOP_HOME)/bin/hadoop jar InitNeighborhoodScorerMapReduce.jar $(REPETITIONS) $(K) input/books/ output

run-serial:
	java -cp ./out/classes/ org.neu.InitNeighborhoodScorer serial $(K) ./input/big-corpus/ ./sampleOutput/ 1

run-parallel:
	java -cp ./out/classes/ org.neu.InitNeighborhoodScorer parallel $(K) 2 ./input/big-corpus/ ./sampleOutput/ 1

report:
	Rscript -e "rmarkdown::render(‘report/report.Rmd')"

setup: # this takes a while
	-$(HADOOP_HOME)/bin/hdfs dfs -mkdir -p books
	$(HADOOP_HOME)/bin/hdfs dfs -mkdir -p big-corpus
	$(HADOOP_HOME)/bin/hdfs dfs -put ./input/books/* /input/books
	$(HADOOP_HOME)/bin/hdfs dfs -put ./input/books/ books/
	$(HADOOP_HOME)/bin/hdfs dfs -put ./input/big-corpus/ big-corpus/

teardown:
	$(HADOOP_HOME)/bin/hdfs dfs -rm -r input

gzip:
	-gzip input/books/*; gzip input/big-corpus/*

gunzip:
	-gunzip input/books/*; gunzip input/big-corpus/*

clean:
	$(RM) out/*.class
	$(HADOOP_HOME)/bin/hdfs dfs -rm -r tmp-output;\
	$(HADOOP_HOME)/bin/hdfs dfs -rm -r letter-count; \
	$(HADOOP_HOME)/bin/hdfs dfs -rm -r letter-score;  \
	$(HADOOP_HOME)/bin/hdfs dfs -rm -r output;




