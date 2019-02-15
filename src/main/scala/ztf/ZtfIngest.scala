
package ztf

import scala.collection.mutable.{ListBuffer, HashMap, HashSet}


case class MatchWithCoordZone(zone: Long, id1: Long, ra1: Double, dec1: Double,
                                          id2: Long, ra2: Double, dec2: Double)

case class MatchSingleCoordZone(zone: Long, id1: Long, id2: Long, ra: Double, dec: Double)

object ZtfIngest {

  type RaDec = (Double, Double)
  type Ra = Double
  type Dec = Double
  type SourceId = Long

  def deduplicatePartition(zone: Long,
                           inputIter: Iterator[MatchWithCoordZone]) = {
    val matches = new ListBuffer[MatchSingleCoordZone]
    // matchedIds points from non-primary ID to primary.
    val matchedIds = new HashMap[SourceId, SourceId]
    val primaryIds = new HashSet[SourceId]

    inputIter foreach {
      case MatchWithCoordZone(_, id1, _, _, id2, ra2, dec2) =>
        if(primaryIds.contains(id1)) {
          if(! matchedIds.contains(id2)) {
            matches.append(MatchSingleCoordZone(zone, id1, id2, ra2, dec2))
            matchedIds += (id2 -> id1)
          }
        } else if (primaryIds.contains(id2)) {
            if(! matchedIds.contains(id1)) {
              matches.append(MatchSingleCoordZone(zone, id2, id1, ra2, dec2))
              matchedIds += (id1 -> id2)
            }
        } else {
          if (matchedIds.contains(id1)) {
            matches.append(MatchSingleCoordZone(zone, matchedIds(id1), id2, ra2, dec2))
            matchedIds += (id2 -> matchedIds(id1))
          } else if (matchedIds.contains(id2)) {
            matches.append(MatchSingleCoordZone(zone, matchedIds(id2), id1, ra2, dec2))
            matchedIds += (id1 -> matchedIds(id2))
          } else {
            matches.append(MatchSingleCoordZone(zone, id1, id2, ra2, dec2))
            primaryIds.add(id1)
            matchedIds += (id2 -> id1)
          }
        }
    }
    matches.toIterable
  }
}