
package ztf

import org.apache.spark.sql.{SparkSession, DataFrame, Dataset, Row}

import ZtfIngest._


object ZtfIngestMain {

  val spark = SparkSession.builder().appName("ZtfIngest").getOrCreate()
  import spark.implicits._

  def reformat_input(x: Dataset[Row]): Dataset[MatchWithCoordZone] = {
    x.withColumnRenamed("ra", "ra1").
      withColumnRenamed("dec", "dec1").
      withColumnRenamed("matchid1", "id1").
      withColumnRenamed("matchid2", "id2").
      as[MatchWithCoordZone]
  }

  def main(args: Array[String]) {
    val crossmatched = reformat_input(spark.read.parquet("just_crossmatch.parquet"))
    val unique_matches = crossmatched.groupByKey(_.zone).flatMapGroups(ZtfIngest.deduplicatePartition)
    unique_matches.write.mode("overwrite").parquet("crossmatch_unique.parquet")
  }
}
