package ztf

import org.scalatest.{FlatSpec, OptionValues, Matchers}
import ZtfIngest._
import ZtfIngest.{Ra, Dec, SourceId, deduplicatePartition}

abstract class UnitSpec extends FlatSpec with OptionValues with Matchers

object ZtfTest extends UnitSpec {

    def stripRaDec(x: Iterable[(Long, Long, Long, Double, Double)]) = {
        x.map( t => (t._2, t._3) )
    }
}

class ZtfTest extends UnitSpec {
    import ZtfTest._

    "ZtfPartitionDeduplicate" should "return something" in {
        val partitionData: Seq[MatchWithCoordZone] =
            Seq( MatchWithCoordZone(0, 1, 1.1, 1.2, 5, 1.1, 1.2),
                 MatchWithCoordZone(0, 5, 1.1, 1.2, 1, 1.1, 1.2)
            )
        val zone = 55
        deduplicatePartition(zone, partitionData.toIterator) should not be empty 
    }

    "ZtfPartitionDeduplicate" should "dedup a single pair" in {
        val partitionData: Seq[MatchWithCoordZone] =
            Seq( MatchWithCoordZone(0, 1, 1.1, 1.2, 5, 1.1, 1.2),
                 MatchWithCoordZone(0, 5, 1.1, 1.2, 1, 1.1, 1.2)
            )
        val zone = 55
        val results = deduplicatePartition(zone, partitionData.toIterator)
        stripRaDec(results) should contain (1, 5)
        stripRaDec(results) should not contain (5, 1)
    }

    "ZtfPartitionDeduplicate" should "dedup a triple-match" in {
        val partitionData: Seq[MatchWithCoordZone] =
            Seq( MatchWithCoordZone(0, 1, 1.1, 1.2, 5, 1.1, 1.2),
                 MatchWithCoordZone(0, 5, 1.1, 1.2, 1, 1.1, 1.2),
                 MatchWithCoordZone(0, 9, 1.1, 1.2, 1, 1.1, 1.2),
                 MatchWithCoordZone(0, 1, 1.1, 1.2, 9, 1.1, 1.2)
            )
        val zone = 55
        val results = deduplicatePartition(zone, partitionData.toIterator) 
        stripRaDec(results)  should contain allOf ((1,5), (1,9))
        stripRaDec(results)  should contain noneOf ((5,9), (9,5), (5,1), (9,1)) 
    }

    "ZtfPartitionDeduplicate" should "dedup a chained triple-match" in {
        val partitionData: Seq[MatchWithCoordZone] =
            Seq(
                 MatchWithCoordZone(0, 5, 1.1, 1.2, 1, 1.1, 1.2),
                 MatchWithCoordZone(0, 1, 1.1, 1.2, 5, 1.1, 1.2),
                 MatchWithCoordZone(0, 9, 1.1, 1.2, 1, 1.1, 1.2),
                 MatchWithCoordZone(0, 1, 1.1, 1.2, 9, 1.1, 1.2)
            )
        val zone = 55
        val results = deduplicatePartition(zone, partitionData.toIterator) 
        stripRaDec(results)  should contain allOf ((5,1), (5,9))
        stripRaDec(results)  should contain noneOf ((1,5), (9,5), (1,9), (9,1)) 
    }

    "ZtfPartitionDeduplicate" should "dedup a quadruple chain" in {
        val partitionData: Seq[MatchWithCoordZone] =
            Seq(
                 MatchWithCoordZone(0, 1, 1.1, 1.2, 5, 1.1, 1.2),
                 MatchWithCoordZone(0, 5, 1.1, 1.2, 1, 1.1, 1.2),
                 MatchWithCoordZone(0, 9, 1.1, 1.2, 5, 1.1, 1.2),
                 MatchWithCoordZone(0, 5, 1.1, 1.2, 9, 1.1, 1.2),
                 MatchWithCoordZone(0, 10, 1.1, 1.2, 9, 1.1, 1.2),
                 MatchWithCoordZone(0, 9, 1.1, 1.2, 10, 1.1, 1.2)
            )
        val zone = 55
        val results = deduplicatePartition(zone, partitionData.toIterator) 
        stripRaDec(results)  should contain allOf ((1,5), (1,9), (1,10))
        stripRaDec(results)  should contain noneOf ((10,9), (9,10), (10,5), (5,10), (9,5), (5,9)) 
    }

    "ZtfPartitionDeduplicate" should "pass single matches" in {
        val partitionData: Seq[MatchWithCoordZone] =
            Seq(MatchWithCoordZone(0, 23, 1.1, 1.2, 19, 1.1, 1.2),
                MatchWithCoordZone(0, 1, 1.1, 1.2, 1, 1.1, 1.2),
                MatchWithCoordZone(0, 2, 1.1, 1.2, 9, 1.1, 1.2),
                MatchWithCoordZone(0, 1, 1.1, 1.2, 3, 1.1, 1.2)
            )

        val zone = 55
        val results = deduplicatePartition(zone, partitionData.toIterator) 
        stripRaDec(results)  should contain allOf ((1,1), (2, 9), (1,3))
        stripRaDec(results)  should contain noneOf ((1,2), (1,9), (9,1), (2,1)) 
    }
}