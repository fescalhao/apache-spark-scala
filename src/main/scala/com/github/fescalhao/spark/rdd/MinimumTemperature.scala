package com.github.fescalhao.spark.rdd

import com.github.fescalhao.PackageUtils._
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.math.min

object MinimumTemperature extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Creating spark context")
    val spark: SparkContext = getSparkContext("Minimum Temperature By Station")

    logger.info("Loading 1800.csv file")
    val lines: RDD[String] = readFile(spark, "datasets/1800.csv")

    logger.info("Parsing lines to get StationId, Temperature Type and Temperature")
    val parsedLines = lines.map(parseLine)

    logger.info("Retrieving minimum temperature by station")
    val minTempByStation = parsedLines
      .filter(measure => measure._2 == "TMIN")
      .map(measure => (measure._1, measure._3))
      .reduceByKey((temp1, temp2) => min(temp1, temp2))
      .sortBy(measure => measure._2, ascending = false)
      .collect()

    logger.info("Printing results")
    minTempByStation.foreach(measure => {
      println(s"Station: ${measure._1} -> Temp: ${measure._2}")
    })

    logger.info("Stopping Spark")
    spark.stop()
  }

  def parseLine(line: String): (String, String, Float) = {
    val fields: Array[String] = line.split(",")
    val stationId = fields(0)
    val tempType = fields(2)
    val temp = fields(3).toFloat

    (stationId, tempType, temp)
  }
}
