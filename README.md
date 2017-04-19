# QR_map_reduce
QR factorization for map reduce architecture

Computing a QR factorization of a tall-and-skinny matrix for nearly-terabyte sized matrices on MapReduce architectures

Implementing the algorithm proposed in  https://stanford.edu/~arbenson/papers/mrtsqr-bigdata2013.pdf using hadopp

How to run
-----------
1) Compile

    javac -classpath hadoop-0.18.3-core.jar:commons-logging-1.1.3.jar:Jama-1.0.3.jar -d classes Decomposition.java

2) Create the executable

    jar -cvf Decomposition.jar -C classes/ .

3) Run

    bin/hadoop jar Decomposition.jar org.myorg.Decomposition my_input my_output


Prerequisites
-------------
- hadoop
- commons-logging
- jama
