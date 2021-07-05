package com.github.fescalhao.spark.dataset.friendsbyage

import com.github.fescalhao.PackageUtils.getSparkSession
import org.apache.log4j.Logger
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object AverageFriendsByAge extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Creating spark session")
    val spark: SparkSession = getSparkSession("Average Friends By Age")

    logger.info("Reading the fakefriends-header.csv file")
    import spark.implicits._
    val lines = spark
      .read
      .option("header", "true")
      .schema(getFileSchema)
      .csv("datasets/fakefriends-header.csv")
      .as[Person]

    val avgFriendsByAge = lines
      .select("age", "friends")
      .groupBy("age")
      .agg(
        functions.round(functions.avg("friends"), 2).alias("avg_friends")
      )
      .sort(functions.desc("avg_friends"))

    avgFriendsByAge.show()
  }

  def getFileSchema: StructType = {
    StructType(List(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("age", IntegerType),
      StructField("friends", IntegerType)
    ))
  }
}
