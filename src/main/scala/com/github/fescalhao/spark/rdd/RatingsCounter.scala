package com.github.fescalhao.spark.rdd

import com.github.fescalhao.PackageUtils._
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.Map

object RatingsCounter extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    logger.info("Creating a spark context")
    val spark: SparkContext = getSparkContext("Ratings Counter")

    logger.info("Reading file")
    val lines: RDD[String] = readFile(spark, "datasets/ml-100k/u.data")

    logger.info("Splitting lines and getting ratings")
    val ratings: RDD[String] = lines map(x => x.split("\t")(2))

    logger.info("Counting ratings")
    val results: Map[String, Long] = ratings.countByValue()

    logger.info("Sorting results by rating")
    val sortedResults = results.toSeq.sortBy(_._1)

    logger.info("Printing ordered results")
    sortedResults.foreach(println)

    logger.info("Stopping Spark")
    spark.stop()
  }
}
