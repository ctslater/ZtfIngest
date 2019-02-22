
package ztf

import org.apache.spark.sql.{SparkSession, DataFrame, Dataset, Row, Column}
import org.apache.spark.sql.functions

import ZtfIngest._


object ZtfIngestMain {

  def zone_func(dec: Column) = {
    val zone_height = 10/60.0
    functions.floor((dec + 90.0)/zone_height)
  }

  def main(args: Array[String]) {

    val usage = """
                |Must specify processing stage: either "crossmatch", "resolve_overlaps", or "joindata".
                """.stripMargin
    if (args.length == 0) {
      println(usage)
      sys.exit(1)
    }

    args(0) match {
      case "crossmatch" => crossmatch_task
      case "joindata" => joindata_task
      case "resolve_overlaps" => resolve_overlaps_task
      case _ => {
        println(usage)
        sys.exit(1)
      }
    }
  }


  def crossmatch_task() : Unit = {
    val spark = SparkSession.builder().appName("ZtfIngest").getOrCreate()
    import spark.implicits._

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
    unique_matches.write.parquet("crossmatch_unique.parquet")
  }

  def resolve_overlaps_task() : Unit = {
    val spark = SparkSession.builder().appName("ZtfIngest").getOrCreate()
    import spark.implicits._
    val unique_matches = spark.read.parquet("crossmatch_unique.parquet")

    val mean_coords = unique_matches.groupBy("zone", "id1").agg(Map("ra" -> "avg", "dec" -> "avg"))
    val good_primary_ids = mean_coords.where(zone_func(mean_coords("avg(dec)")) === mean_coords("zone"))
    val resolved_pairs = unique_matches.join(good_primary_ids, Seq("id1", "zone"))
                                       .select("id1", "id2").distinct()
    resolved_pairs.write.parquet("resolved_pairs.parquet")

  }

  def joindata_task(): Unit = {
    val spark = SparkSession.builder().appName("ZtfIngest").getOrCreate()
    import spark.implicits._

    val resolved_pairs = spark.read.parquet("resolved_pairs.parquet")
    resolved_pairs.createOrReplaceTempView("resolved_pairs")

    val raw_data = spark.read.parquet("/data/epyc/data/ztf_scratch/matchfile_debug_10arcsec/rc??/*/*_data.parquet")
    raw_data.createOrReplaceTempView("raw_data")

    val joined_data = spark.sql("""SELECT resolved_pairs.id1 as matchid,
                                |avg(ra) as ra, avg(dec) as dec,
                                |count(*) as nobs_avail,
                                |collect_list(matchid) as combined_matchids,
                                |collect_list(mjd) AS mjd, collect_list(programid) AS programid,
                                |collect_list(filterid) as filterid,
                                |collect_list(mag) AS mag, collect_list(magerr) as magerr,
                                |collect_list(psfmag) AS psfmag, collect_list(psfmagerr) as psfmagerr,
                                |collect_list(psfflux) AS psfflux, collect_list(psffluxerr) as psffluxerr,
                                |collect_list(chi) as chi, collect_list(catflags) as catflags, collect_list(sharp) as sharp,
                                |collect_list(xpos) as xpos, collect_list(ypos) as ypos, collect_list(rcid) as rcid,
                                |FROM raw_data
                                |JOIN resolved_pairs ON resolved_pairs.id2 = raw_data.matchid
                                |GROUP BY resolved_pairs.id1""".stripMargin)

    joined_data.write.parquet("/data/epyc/users/ctslater/big_join.parquet")

  }
}
