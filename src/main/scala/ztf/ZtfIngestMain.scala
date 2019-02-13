
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
    /* val crossmatched = reformat_input(spark.read.parquet("just_crossmatch.parquet")) */
    val input_pos = spark.read.parquet("/data/epyc/data/ztf_scratch/matchfile_debug_10arcsec/rc??/*/*_pos.parquet")
    input_pos.createOrReplaceTempView("ztf")
    val crossmatched = spark.sql("""select ztf.zone,
                         |ztf.matchid as id1, ztf.ra as ra1, ztf.dec as dec1,
                         |ztf2.matchid as id2,ztf2.ra as ra2, ztf2.dec as dec2
                         |FROM ztf JOIN ztf as ztf2 ON ztf.zone = ztf2.zone
                         |AND (ztf.ra > ztf2.ra - 1.5/3600.0) AND (ztf.ra < ztf2.ra + 1.5/3600.0)
                         |WHERE pow(ztf.ra - ztf2.ra, 2) + pow(ztf.dec - ztf2.dec, 2) < pow(1.5/3600.0,2)""".stripMargin
                         ).as[MatchWithCoordZone]

    val unique_matches = crossmatched.groupByKey(_.zone).flatMapGroups(ZtfIngest.deduplicatePartition)
    unique_matches.write.mode("overwrite").parquet("crossmatch_unique.parquet")
  }
}
