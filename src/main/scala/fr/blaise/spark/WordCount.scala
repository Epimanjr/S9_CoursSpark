package fr.blaise.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(conf)

    val begin = System.currentTimeMillis();
    val textFile = sc.textFile("files/big.txt")
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .sortBy(-_._2)
      .take(10);
    counts.foreach(println)
    
    val end = System.currentTimeMillis();
  }
  
}