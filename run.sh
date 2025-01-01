mvn clean package
hadoop jar target/dominating-set-1.0-SNAPSHOT.jar org.inonu.edu.DominatingSetJob /input/min_30_line.txt /study-27/
hadoop fs -getmerge /study-25/ output.txt
python src/main/python/visualize.py output.txt graph-2.png