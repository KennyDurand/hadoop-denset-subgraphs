Densest Subgraph in Hadoop MapReduce
====================================

About
-----
This project is an open-source implementation of the Densest Subgraph algorithm. It is intended to work with Hadoop 2.2.0, others versions have not been tested.

How to run
----------
1) Launch a terminal as your Hadoop user, and launch your Hadoop daemons

2) Clone this repository and `cd` in it

3) Create a `classes` directory:

	$ mkdir classes

4) Download https://repository.cloudera.com/content/groups/public/org/apache/hadoop/hadoop-annotations/2.0.0-cdh4.0.1/hadoop-annotations-2.0.0-cdh4.0.1.jar and put it in $HADOOP_HOME/share/hadoop/common/hadoop-annotations-2.2.0.jar:

	$ wget https://repository.cloudera.com/content/groups/public/org/apache/hadoop/hadoop-annotations/2.0.0-cdh4.0.1/hadoop-annotations-2.0.0-cdh4.0.1.jar
	$ mv hadoop-annotations-2.0.0-cdh4.0.1.jar $HADOOP_HOME/share/hadoop/common/hadoop-annotations-2.2.0.jar

5) Compile all the sources using `javac`:

	$ javac -classpath $HADOOP_HOME/share/hadoop/common/hadoop-common-2.2.0.jar:$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.2.0.jar:$HADOOP_HOME/share/hadoop/common/lib/commons-cli-1.2.jar:$HADOOP_HOME/share/hadoop/common/hadoop-annotations-2.2.0.jar -d classes -Xlint:deprecation *.java */*.java

6) Put the Java classes into a JAR file:

	$ jar -cvf subgraph.jar -C classes/ .

7) Run the JAR file with Hadoop, specifying in arguments the input file and the output directory (check the `input/` directory for some examples):

	$ hadoop jar subgraph.jar sogara.hadoop.subgraph.DensestSubgraph [input_file] [output_directory]

Structure of the code
---------------------

The `Job/` directory regroups the four jobs used in the algorithm. Each Job contains an inner Mapper and an inner Reducer.

The `FileFormat/` directory contains custom classes that define how the files are read and transformed into key-value pairs.

The `DensestSubgraph` class contains the main function, which chains the job, runs the algorithm and takes care of the intermediate outputs.
