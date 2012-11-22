Partitioner
===========

Summary
-------

Qserv’s partitioners are implemented as standalone C++ programs
(`qserv-partition` or `qserv-partition-matches`, built from `qserv/admin/dupr/`
in the source tree) that separate input rows from a file (or set of files) and
write them into another set of files named according to their partition numbers.

`qserv-partition[-matches]` takes as input a set of CSV files, and based on a
specified partition configuration, produces output CSV files (one per partition).
Each partition is called a "chunk" and contains subpartitions ("subchunks"),
although a partition’s subpartitions are stored together in a file (and table).

Tables that are partitioned in Qserv must be partitioned identically within a
Qserv database.  This means that tables in a database share identical partition
boundaries and identical mappings of chunk ID->spatial partition.  In order to
facilitate table joining, a single table's columns are chosen to define the
partitioning space and all partitioned tables (within a database) are
partitioned according that pair of columns. Our current plan chooses the
`Object` table’s `ra_PS` and `decl_PS` columns, meaning that rows in the
`Source` and `ForcedSource` tables would be partitioned according to the
`Object`s they reference.

However, there is a wrinkle: prototype pipeline development involves comparing
pipeline outputs to a reference catalog, which might correspond to an
independent processing of the input data, or to a ground truth catalog from
which raw pipeline input images were synthesized. Such tables (`SimRefObject`
for example) share the same partitioning parameters as `Object`. But, there
is no obvious 1 to 1 mapping between reference objects and objects. Instead,
the pipelines compute a (spatial) match table between the two that provides
a many-to-many relationship between both tables. The complication is that a
match record might reference an object and reference object that fall on
opposite sides of a partition boundary. Qserv deals with this by taking
advantage of the overlap that must be stored alongside each partition (this
overlap is stored so that Qserv can avoid inter-worker communication when
performing spatial joins on the fly).

Requirements
------------

The Qserv partitioning code must be capable of processing:
 - CSV-format files
 - files that are very large
 - large quantities of files
 - tables containing both positions and positional match pairs

It must be possible to reorder and drop input columns. The output format shall
be CSV suitable for loading into the database instances running on Qserv worker
nodes.

The following features are desirable, but have yet to be implemented:
 - It should be possible to process FITS table files.
 - It should be possible to lookup the sky-positions of entities in a
   record by key. Currently, positions must be present in the records fed to the
   partitioner (but can be dropped from the output). Note that it must be
   possible to fall-back to another position in case the key is `NULL`. Such
   orphaned entities can be produced if for example the association pipeline
   doesn’t necessarily associate each `Source` to an `Object` (some `Source`s
   might be considered to be spurious).
 - Producing partitioned output tables in native database format (e.g.
   directly producing `.MYD` files for the MySQL MyISAM storage engine)
   may significantly speed up the loading process.

Design
------

The partitioner is expressed in the map-reduce paradigm. The map function
operates on one input record at a time. It computes the partition(s)
the input record must be assigned to and emits corresponding output
records. Output records are grouped by chunk and passed on to reducers,
which collect statistics on how many records are in each sub-chunk and
write output records to disk. This is all implemented in C++, using a small
in-memory map-reduce processing framework tailored to the task. The framework
is multi-threaded - file input, processing, and output are all block oriented
and parallel (see `qserv/admin/dupr/src/MapReduce.h` in the source tree for
details).

As a result of using this model, the code that deals with parallelization is
separated from the partitioning logic, making the implementation easier to
follow. Another benefit is that porting to the Hadoop framework would at
least be conceptually straightforward.

Usage
-----

Here is a fully worked single node example for the PT1.2 `Object`,
`SimRefObject`, and `RefObjMatch` tables. First, the tables are dumped
in TSV format:

~~~~sh
mysql -A \
      -h lsst10.ncsa.illinois.edu \
      -D rplante_PT1_2_u_pt12prod_im2000 \
      -B --quick --disable-column-names \
      -e "SELECT * FROM Object;" > Object.tsv

mysql -A \
      -h lsst10.ncsa.illinois.edu \
      -D rplante_PT1_2_u_pt12prod_im2000 \
      -B --quick --disable-column-names \
      -e "SELECT * FROM SimRefObject;" > SimRefObject.tsv
~~~~

For the`RefObjMatch` table, we’ll need to pull in the positions of the objects
and reference objects being matched up, so that the partitioner can determine
which partition each side of the match has been assigned to. That can be done via:

~~~~sql
SELECT m.*, r.ra, r.decl, o.ra_PS, o.decl_PS
    FROM RefObjMatch  AS m LEFT JOIN
                SimRefObject AS r ON (m.refObjectId = r.refObjectId) LEFT JOIN
                Object       AS o ON (m.objectId = o.objectId);
~~~~

Note the left joins - either side of a match pair can be NULL, indicating an
unmatched object or reference object. 

Assuming `QSERV_DIR` has been set to a Qserv install or checkout location,
the `Source` and `Object` tables can now be partitioned as follows:

~~~~sh
CFG_DIR=$QSERV_DIR/admin/dupr/config/PT1.2

for TABLE in Object Source; do
    qserv-partition \
        --config-file=$CFG_DIR/$TABLE.cfg \
        --config-file=$CFG_DIR/common.cfg \
        --in.csv.null=NULL \
        --in.csv.delimiter=$'\t' \
        --in.csv.escape=\\ \
        --in.csv.quote=\" \
        --in=$TABLE.tsv \
        --verbose \
        --mr.num-workers=6 --mr.pool-size=32768 --mr.block-size=16 \
        --out.dir=chunks/$TABLE
done
~~~~

The matches can be partitioned using:

~~~~sh
    qserv-partition-matches \
        --config-file=$CFG_DIR/RefObjMatch.cfg \
        --config-file=$CFG_DIR/common.cfg \
        --in.csv.null=NULL \
        --in.csv.delimiter=$'\t' \
        --in.csv.escape=\\ \
        --in.csv.quote=\" \
        --in=RefObjMatch.tsv \
        --verbose \
        --mr.num-workers=6 --mr.pool-size=32768 --mr.block-size=16 \
        --out.num-nodes=1 --out.dir=chunks/RefObjMatch
~~~~

Output chunk files are stored in the directory specified by `--out.dir`,
and can subsequently be distributed and loaded into the MySQL databases
on worker nodes. Examine the config files referenced above and run
`qserv-partition --help` or `qserv-partition-matches --help` for more
information on partitioning parameters.

