package com.github.fescalhao.spark.dataset.sparksqldatasets

import com.github.fescalhao.PackageUtils.getSparkSession
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQLDataset extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Creating spark session")
    val spark: SparkSession = getSparkSession("SparkSQL")

    logger.info("Loading fakefriends-header.csv file to a Dataset")
    import spark.implicits._
    val schemaPeople: Dataset[Person] = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("datasets/fakefriends-header.csv")
      .as[Person]

    logger.info("Printing the schema")
    schemaPeople.printSchema()

    logger.info("Creating a temporary view")
    schemaPeople.createOrReplaceTempView("people")

    logger.info("Selecting data")
    val teenagers: DataFrame = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

    logger.info("Collecting results")
    val results: Array[Row] = teenagers.collect()

    logger.info("Printing results")
    results.foreach(println)

    logger.info("Stopping spark session")
    spark.stop()
  }
}
