package com.github.fescalhao.spark.rdd

import com.github.fescalhao.PackageUtils._
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object AverageFriendsByAge extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Creating spark context")
    val spark: SparkContext = getSparkContext("Average Friends By Age")

    logger.info("Loading fakefriends-header.csv file")
    val lines: RDD[String] = readFile(spark, "datasets/fakefriends.csv")

    logger.info("Parsing each line to get the age and the number of friends")
    val parsedLine: RDD[(Int, Int)] = lines.map(parseLine)

    logger.info("Calculating the average friends by age and sorting results")
    val averageFriendsByAge = parsedLine
      .mapValues(friends => (friends, 1))
      .reduceByKey((friends1, friends2)  => {
        (friends1._1 + friends2._1, friends1._2 + friends2._2)
      })
      .mapValues(friends => friends._1.toDouble / friends._2)
      .sortBy(x => x._2, ascending = false)
      .collect()

    logger.info("Printing results")
    averageFriendsByAge.foreach(result => {
      println(f"Age: ${result._1} -> Avg. Friends: ${result._2}%.2f")
    })

    logger.info("Stopping Spark")
    spark.stop()
  }

  private def parseLine(line: String): (Int, Int) = {
    val fields = line.split(",")
    val age = fields(2).toInt
    val friends = fields(3).toInt
    (age, friends)
  }
}
