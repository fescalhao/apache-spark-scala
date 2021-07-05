package com.github.fescalhao.spark.rdd

import com.github.fescalhao.PackageUtils.{getSparkContext, readFile}
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object AmountSpentByCustomer extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Creating spark context")
    val spark: SparkContext = getSparkContext("Total Amount Spent By Customer")

    logger.info("Loading customer-orders.csv file")
    val lines: RDD[String] = readFile(spark, "datasets/customer-orders.csv")

    logger.info("Parsing lines to get the customer and value spent")
    val purchase: RDD[(Int, Double)] = lines.map(parseLine)

    logger.info("Calculating the total amount spent by customer")
    val amountByCustomer = purchase
      .reduceByKey((v1, v2) => v1 + v2)
      .sortBy(x => x._2, ascending = false)
      .collect()

    logger.info("Printing results")
    amountByCustomer.foreach(result => {
      println(f"Customer ${result._1}%02d -> $$${result._2}%.2f")
    })

    logger.info("Stopping spark")
    spark.stop()
  }

  def parseLine(line: String): (Int, Double) = {
    val fields: Array[String] = line.split(",")
    val customer: Int = fields(0).toInt
    val value: Double = fields(2).toDouble

    (customer, value)
  }
}
