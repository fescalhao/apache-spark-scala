package com.github.fescalhao.spark.rdd

import com.github.fescalhao.PackageUtils._
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

object CountWords extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Creating spark context")
    val spark: SparkContext = getSparkContext("Counting Words")

    logger.info("Loading book.txt file")
    val lines = readFile(spark, "datasets/book.txt")

    logger.info("Splitting words from the book and setting them to lowercase")
    val words = lines.flatMap((line: String) => line.toLowerCase.split("\\W+"))

    logger.info("Counting and sorting each word")
    val wordCount = words
      .map(word => (word, 1))
      .reduceByKey((x, y) => x + y)
      .sortBy(word => word._2, ascending = false)
      .collect()

    logger.info("Printing results")
    wordCount.foreach(word => {
      println(s"${word._1} -> ${word._2}")
    })

    logger.info("Stopping Spark")
    spark.stop()
  }
}
