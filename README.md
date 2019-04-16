
ZTF Crossmatching and Ingest
============================

This repository contains tools for loading data from the Zwicky Transient
Facility into a format suitable for analysis with Spark. The input are "match
files" containing the photometry performed on the original science images,
not the difference images. The input files are organized by readout channel
(per CCD and per amplifier), then by on-sky field ID. Inside each of these
per channel, per field, files, sources are crossmatched and repeat
measurements of the same source on the sky are identified with a common
"matchid", yielding a light curve for that source.

From this starting point, there are a few improvements that are necessary to
maximize the utility of the data:

1. The crossmatches are only performed inside of a single field and readout
channel; even if other observations overlap the same region on the sky, the
data is not included in the light curve if it was not observed with the same
field ID and channel. Data from different filters are also handled separately.

2. Summary statistics per-source are computed over all observed data, even
though some of that data is proprietary to Caltech or to the public MSIP
program, and hence not available to the ZTF partners in the match files. For
consistency, these need to be recomputed on the available data if they are to
be used for selection of sources.

3. To better support large scale parallel analysis, we need to reformat the
data so it is compatible with Spark and our AXS extensions to Spark. This
means the data must be partitioned by into narrow bands of declination, often
called "zones", and with small amounts of data duplication on the zone edges,
to enable fast crossmatching with other catalogs. We also coalesce multiple
measurements of each source into a single row containing many array columns,
so that light curve processing can operate directly on rows without the need
for a "GROUP BY" operation. These operations all depend on storing the data
in parquet files to enable access from Spark. Additionally, we create a new
globally-unique matchid field to replace the file-specific IDs in the
original data.

Ingest Overview
===============

A high-level overview of how these tools accomplish the above goals is as
follows:

1. Individual hdf5 files are converted to parquet files, one-for-one. This
merges both "sources" and "transients", adds a globally-unique ID and a
"zone" id, duplicates data on zone boundaries, discards the summary
statistics, and performs column type conversion where necessary.

2. The ensemble of parquet files are loaded as a single Spark Dataframe. A
self-join is performed to crossmatch the data with itself, to identify
multiple records of the same source across different fields or channels.

3. The crossmatches are filtered so that each "family" of matches results in
a single set of records, each with a the same primary match ID. The goal is
for a GROUP BY operation to be able to select the entire set of light curves
that should be merged, without leaving additional pieces of the same match
family leaking out. This is performed per-zone to enable parallelism, 

4. Sources that appear twice, because they are in the overlap region near
zone boundaries, are resolved and filtered in order to keep only a single
copy of each light curve.

5. Steps 2 through 4 operate only on the columns necessary for crossmatching
and identification of records; they do not cary the full photometric data
with them. The last step is to perform the "big join" that combines the
photometric data with the properly crossmatched and de-duplicated source
records, coalescing multiple measurements into array columns, and writing out
the data in an AXS-compatible format.

Detailed Ingest Steps
=====================

1. Run `ztf_make_parquet.py`. This converts each hdf5 matchfile into two
parquet files; one suffiexed with `_data` containing `sourcedata` and
`transientdata`, and another suffixed by `_pos` which contains only the
`matchid`, `ra`, and `dec` values for sources and transients. Records in the
positional table are assigned to zones (currently 20 arcseconds wide in dec),
and records falling within 5 arcseconds of a zone boundary are duplicated
into the neighboring zone. Rows in the `_data` tables are not duplicated.

2. Crossmatch inside of each zone. This is performed by the "crossmatch" task
in ztf-ingest.jar. The SQL statement this task runs is:
```
SELECT ztf.zone, ztf.matchid as id1, ztf.ra as ra1, ztf.dec as dec1,
       ztf2.matchid as id2,ztf2.ra as ra2, ztf2.dec as dec2
FROM ztf 
JOIN ztf as ztf2 ON ztf.zone = ztf2.zone
                 AND (ztf.ra > ztf2.ra - 1.5/3600.0)
                 AND (ztf.ra < ztf2.ra + 1.5/3600.0)
WHERE pow(ztf.ra - ztf2.ra, 2) + pow(ztf.dec - ztf2.dec, 2) < pow(1.5/3600.0,2)
```                        

This task also maps over each partition and performs deduplication of the
crossmatches inside of each zone.

This task tends to cause garbage collection thrashing when run in standalone
mode, so adding lots of driver memory is helpful. An example submission looks
like:
```
spark-submit --master 'local[5]' \
--driver-java-options "-XX:+UseG1GC " \
--driver-memory 100G --conf spark.memory.offHeap.enabled=true \
--conf spark.memory.offHeap.size=60G ztf-ingest_2.11-1.0.jar crossmatch
```

This currently writes out to `crossmatch_unique.parquet`, but it should be
made configurable.

4. Resolve zone overlaps, with the "resolve_overlaps" task in ztf-ingest.jar.
Because each zone should see all of the records that are within ~2 arcseconds
of the zone border, identical match families will be produced in both zones.
We resolve these by computing the mean position of the family and keeping
only the one that falls inside its own zone. The other zone will see the mean
position resolve to outside its zone, and will discard it. 

This task reads in `crossmatch_unique.parquet` and writes out
`resolved_pairs.parquet`

5. Join the full parquet dataset (generated in Step 1) with the deduplicated
matchid pairings; tagging each record with the primary matchid which it
should be grouped into. In the same query, the records are grouped by the
primary matchid and the measurement columns are concatenated into arrays.
This is performed by the "joindata" task, which writes out
`big_join.parquet`. This took about 58 hours to run with 10 executors.

Match IDs
=========

The format of the globally-unique match IDs is FFFFFccQfTsssssss.

- F: Field ID, 5 digits.
- c: CCD ID, 2 digits.
- Q: Quadrant ID, 1 digit.
- f: Filter ID, 1 digit (1:g, 2:R, 3:i).
- T: Type; 1 for transient, 0 for non-transient.
- s: matchID from the original matchfile, 7 digits.  

To-Do
=====

- Add additional convenience fields, such as `nobs` per filter, mean magnitudes per band.
- Optimization of memory usage in `crossmatch` task.


