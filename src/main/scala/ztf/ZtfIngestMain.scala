
package ztf

import org.apache.spark.sql.{SparkSession, DataFrame, Dataset, Row, Column}
import org.apache.spark.sql.functions
import org.rogach.scallop._

import ZtfIngest._

class IngestConf(arguments: Seq[String], tasks: Seq[String]) extends ScallopConf(arguments) {
  val dataPath = opt[String]()
  val positionalGlob = opt[String](default=Some("rc??/*/*_pos.parquet"))
  val dataGlob = opt[String](default=Some("rc??/*/*_data.parquet"))
  val task = trailArg[String](required = true, validate = (tasks.contains(_)))
  verify()
}


object ZtfIngestMain {

  def zoneFunc(dec: Column) = {
    val zoneHeight = 10/60.0
    functions.floor((dec + 90.0)/zoneHeight)
  }

  def main(args: Array[String]) {

    val tasks = Seq("crossmatch", "resolve_overlaps", "joindata")
    val ingestConf = new IngestConf(args, tasks)

    ingestConf.task() match {
      case "crossmatch" => crossmatchTask(ingestConf.dataPath.toOption, ingestConf.positionalGlob.toOption)
      case "resolve_overlaps" => resolveOverlapsTask
      case "joindata" => joinDataTask(ingestConf.dataPath.toOption, ingestConf.dataGlob.toOption)
      case _ => {
        /* This should never happen if IngestConf validation worked correctly */
        println("Invalid task name")
        sys.exit(1)
      }
    }
  }


  def crossmatchTask(dataPath: Option[String], globPattern: Option[String]) : Unit = {
    val spark = SparkSession.builder().appName("ZtfIngest").getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.internal.SQLConf.SHUFFLE_PARTITIONS
    spark.sessionState.conf.setConf(SHUFFLE_PARTITIONS, 2000)

    val inputPos = spark.read.parquet(dataPath.getOrElse("") + globPattern.getOrElse(""))
    inputPos.createOrReplaceTempView("ztf")

    spark.sparkContext.setJobGroup("crossmatch", "Crossmatch Task")
    val crossmatched = spark.sql("""select ztf.zone,
                         |ztf.matchid as id1, ztf.ra as ra1, ztf.dec as dec1,
                         |ztf2.matchid as id2,ztf2.ra as ra2, ztf2.dec as dec2
                         |FROM ztf JOIN ztf as ztf2 ON ztf.zone = ztf2.zone
                         |AND (ztf.ra > ztf2.ra - 1.5/3600.0) AND (ztf.ra < ztf2.ra + 1.5/3600.0)
                         |WHERE pow(ztf.ra - ztf2.ra, 2) + pow(ztf.dec - ztf2.dec, 2) < pow(1.5/3600.0,2)""".stripMargin
                         ).as[MatchWithCoordZone]

    val uniqueMatches = crossmatched.groupByKey(_.zone).flatMapGroups(ZtfIngest.deduplicatePartition)
    uniqueMatches.write.parquet("crossmatch_unique.parquet")
  }

  def resolveOverlapsTask() : Unit = {
    val spark = SparkSession.builder().appName("ZtfIngest").getOrCreate()
    import spark.implicits._
    val uniqueMatches = spark.read.parquet("crossmatch_unique.parquet")

    spark.sparkContext.setJobGroup("resolve_overlaps", "Resolve Overlaps Task")

    val meanCoords = uniqueMatches.groupBy("zone", "id1").agg(Map("ra" -> "avg", "dec" -> "avg"))
    val goodPrimaryIds = meanCoords.where(zoneFunc(meanCoords("avg(dec)")) === meanCoords("zone"))
    val resolvedPairs = uniqueMatches.join(goodPrimaryIds, Seq("id1", "zone"))
                                       .select("id1", "id2").distinct()
    resolvedPairs.write.parquet("resolved_pairs.parquet")

  }

  def joinDataTask(dataPath: Option[String], globPattern: Option[String]): Unit = {
    val spark = SparkSession.builder().appName("ZtfIngest").getOrCreate()
    import spark.implicits._

    val resolvedPairs = spark.read.parquet("resolved_pairs.parquet")
    resolvedPairs.createOrReplaceTempView("resolved_pairs")

    val rawData = spark.read.parquet(dataPath.getOrElse("") + globPattern.getOrElse(""))
    rawData.createOrReplaceTempView("raw_data")

    spark.sparkContext.setJobGroup("joindata_task", "Join Data Task")

    val joinedData = spark.sql("""SELECT resolved_pairs.id1 as matchid,
                                |avg(ra) as ra, avg(dec) as dec,
                                |count(*) as nobs_avail,
                                |collect_list(matchid) as combined_matchids,
                                |collect_list(mjd) AS mjd, collect_list(programid) AS programid,
                                |collect_list(filterid) as filterid,
                                |collect_list(mag) AS mag, collect_list(magerr) as magerr,
                                |collect_list(psfmag) AS psfmag, collect_list(psfmagerr) as psfmagerr,
                                |collect_list(psfflux) AS psfflux, collect_list(psffluxerr) as psffluxerr,
                                |collect_list(chi) as chi, collect_list(catflags) as catflags, collect_list(sharp) as sharp,
                                |collect_list(xpos) as xpos, collect_list(ypos) as ypos
                                |FROM raw_data
                                |JOIN resolved_pairs ON resolved_pairs.id2 = raw_data.matchid
                                |GROUP BY resolved_pairs.id1""".stripMargin)

    joinedData.write.parquet("big_join.parquet")

  }
}
