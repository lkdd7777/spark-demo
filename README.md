local 
- ./bin/spark-submit --class WordCount --master spark://master:7077 /home/panchen/code/spark-demo-wordcount.jar hdfs://master:9000/The_Man_of_Property.txt 

- ./bin/spark-submit --class com.demo.JavaWordCount --master spark://master:7077 /home/panchen/code/java-wordcount.jar hdfs://master:9000/The_Man_of_Property.txt

yarn-client 
- ./bin/spark-submit --master yarn-client --class WordCount /home/panchen/code/spark-demo-wordcount.jar hdfs://master:9000/The_Man_of_Property.txt
 
- ./bin/spark-submit --master yarn-client --class com.demo.JavaWordCount /home/panchen/code/java-wordcount.jar hdfs://master:9000/The_Man_of_Property.txt