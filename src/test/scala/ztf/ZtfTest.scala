package ztf

import org.scalatest.{FlatSpec, OptionValues, Matchers}
import ZtfIngest._
import ZtfIngest.{Ra, Dec, SourceId, deduplicatePartition}

abstract class UnitSpec extends FlatSpec with OptionValues with Matchers

object ZtfTest extends UnitSpec {

    def stripRaDec(x: Iterable[(Long, Long, Long, Double, Double)]) = {
        x.map( t => (t._2, t._3) )
    }

    // This avoids having to write out fake ra, dec coordinates, which at the moment
    // aren't used anyways.
    def makeMatchWithCoordZone(input: Iterable[(Long, Long)]) : Seq[MatchWithCoordZone] = {
        input.map(x => MatchWithCoordZone(0, x._1, 1.1, 1.2, x._2, 1.1, 1.2)).toSeq
    }
}

class ZtfTest extends UnitSpec {
    import ZtfTest._

    "ZtfPartitionDeduplicate" should "return something" in {
        val partitionData = makeMatchWithCoordZone(
            Seq( (1, 5), (5, 1)))
        val zone = 55
        deduplicatePartition(zone, partitionData.toIterator) should not be empty
    }

    "ZtfPartitionDeduplicate" should "dedup a single pair" in {
        val partitionData = makeMatchWithCoordZone(
            Seq( (1, 5), (5, 1)))
        val zone = 55
        val results = deduplicatePartition(zone, partitionData.toIterator)
        stripRaDec(results) should contain (1, 5)
        stripRaDec(results) should not contain (5, 1)
    }

    "ZtfPartitionDeduplicate" should "dedup a triple-match" in {
        val partitionData = makeMatchWithCoordZone(
            Seq( (1, 5), (5, 1), (9, 1), (1, 9)))
        val zone = 55
        val results = deduplicatePartition(zone, partitionData.toIterator)
        stripRaDec(results) should contain allOf ((1,5), (1,9))
        stripRaDec(results) should contain noneOf ((5,9), (9,5), (5,1), (9,1))
    }

    "ZtfPartitionDeduplicate" should "dedup a chained triple-match" in {
        val partitionData = makeMatchWithCoordZone(
            Seq( (5, 1), (1, 5), (9, 1), (1, 9)))
        val zone = 55
        val results = deduplicatePartition(zone, partitionData.toIterator)
        stripRaDec(results) should contain allOf ((5,1), (5,9))
        stripRaDec(results) should contain noneOf ((1,5), (9,5), (1,9), (9,1))
    }

    "ZtfPartitionDeduplicate" should "dedup a quadruple chain" in {
        val partitionData = makeMatchWithCoordZone(
            Seq( (1, 5), (5, 1), (9, 5), (5, 9), (10, 9), (9, 10)))
        val zone = 55
        val results = deduplicatePartition(zone, partitionData.toIterator)
        stripRaDec(results) should contain allOf ((1,5), (1,9), (1,10))
        stripRaDec(results) should contain noneOf ((10,9), (9,10), (10,5), (5,10), (9,5), (5,9))
    }

    "ZtfPartitionDeduplicate" should "pass single matches" in {
        val partitionData = makeMatchWithCoordZone(
            Seq( (23, 19), (1,1), (2, 9), (1,3)))

        val zone = 55
        val results = deduplicatePartition(zone, partitionData.toIterator)
        stripRaDec(results) should contain allOf ((1,1), (2, 9), (1,3))
        stripRaDec(results) should contain noneOf ((1,2), (1,9), (9,1), (2,1))
    }

    "ZtfPartitionDeduplicate" should "not duplicate matches" ignore {
        val partitionData = makeMatchWithCoordZone(
            Seq( (85, 72), (72, 85), (72, 72)))

        val zone = 55
        val results = deduplicatePartition(zone, partitionData.toIterator)
        stripRaDec(results) should contain theSameElementsAs List((85, 72), (85, 85))
    }
}