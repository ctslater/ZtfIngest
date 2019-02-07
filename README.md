
1. Run `ztf_make_parquet.py`. This converts each hdf5 matchfile into two
parquet files; one suffiexed with `_data` containing `sourcedata` and
`transientdata`, and another suffixed by `_pos` which contains only the
`matchid`, `ra`, and `dec` values for sources and transients. Records in the
positional table are assigned to zones (currently 20 arcseconds wide in dec),
and records falling within 5 arcseconds of a zone boundary are duplicated
into the neighboring zone. Rows in the `_data` tables are not duplicated.

2. Crossmatch inside of each zone. This is done by:
```
SELECT ztf.zone, ztf.matchid as matchid1, ztf.ra as ra, ztf.dec as dec, ztf2.matchid as matchid2,
ztf2.ra as ra2, ztf2.dec as dec2 
FROM ztf JOIN ztf as ztf2 ON ztf.zone = ztf2.zone 
AND (ztf.ra > ztf2.ra - 1/3600.0) AND (ztf.ra < ztf2.ra + 1/3600.0)  
WHERE ((ztf.dec - ztf2.dec) BETWEEN -1/3600.0 AND 1/3600.0) 
```                        
This query should eventually get merged into the ZtfIngest scala code.


3. Run `ztf_ingest.jar`. This takes the contents of each zone and identifies
one "primary" matchid for each family of crossmatches. This eliminates
duplicates (e.g. keeping (1,2) and discarding (2,1)) and allows for a GROUP
BY on the primary matchid to select the entire crossmatch family.

This task tends to cause garbage collection thrashing when run in standalone
mode, so adding lots of driver memory is helpful. Submittion typically looks
like:
```
spark-submit --master 'local[5]' \
--driver-java-options "-XX:+UseG1GC " \
--driver-memory 10G --conf spark.memory.offHeap.enabled=true \
--conf spark.memory.offHeap.size=4G ztf-ingest_2.11-1.0.jar
```

4. Resolve zone overlaps. Because each zone should see all of the records
that are within ~2 arcseconds of the zone border, identical match families
will be produced in both zones. We resolve these by computing the mean
position of the family and keeping only the one that falls inside its own
zone. The other zone will see the mean position resolve to outside its zone,
and will discard it. This can be done with:
```
def zone_func(dec):
    zone_height = 10/60.0
    return sparkfunc.floor((dec + 90.0)/zone_height).cast("Integer")
mean_coords = unique_crossmatches.groupby("zone", "id1").agg({"ra": "avg", "dec": "avg"})
good_ids = mean_coords.where(zone_func(mean_coords['avg(dec)']) == mean_coords["zone"])
```
This should also get merged into the ZtfIngest scala code.

5. Join the full parquet dataset (generated in Step 1) with the deduplicated
matchid pairings; tagging each record with the primary matchid which it
should be grouped into.

6. GROUP BY the primary matchid; concatenating all of the measurement columns
into arrays.


