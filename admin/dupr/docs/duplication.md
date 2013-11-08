Duplication
===========

Motivation
----------

To date, none of the LSST data challenge outputs are sufficiently large to
test Qserv at scale - the sky regions that have been processed are of
relatively limited extent. Since image simulation and prototype LSST pipeline
runs over very large regions of sky are time, compute and manpower intensive,
some other method of generating lots of representative data quickly is
required.

The Qserv duplicator attempts to meet this need. It takes an existing table
containing positions, and copies and rotates small spherical triangles from
the original to fill in a user-specified region. It also rewrites primary and
foreign key columns so that table relationships are maintained.

Overview
--------

First, a pair of definitions. The _input table_ refers to the table to be
be duplicated. The _partitioning table_ refers to the table that defines the
positions by which a set of related tables are partitioned. For example,
when duplicating astronomical objects, `Object` is both the input and
partitioning table. When duplicating single exposure sources (associated
with objects), `Source` or `ForcedSource` is the input table and `Object` is
the partitioning table.

Data duplication involves two phases. The first is the HTM indexing phase,
which is essentially a bucket-sort of the input table. For each input row R,
the HTM triangle (at some user specified subdivision level) containing the 
partitioning position is computed, and the primary key (currently assumed
to be a 64 bit integer) is extracted. The indexer writes both the row and
the associated primary key value to separate files - one file for each
non-empty HTM triangle. The indexing code also tracks and stores the number
of records in each HTM triangle.

Once indexing is complete, the set of non-empty HTM triangles is known, and
the records and/or primary keys for any particular triangle are easily
accessible via sequential IO. Note also that an index too large to fit on the
local storage of a single node can be spread over multiple nodes by
distributing a subset of triangles to each node (more on this later).

The second phase is the duplication phase. First, the set of HTM triangles
overlapping the duplication region, specified as a box in longitude and
latitude, is computed. For each overlapping triangle with ID T, a non-empty
“source” triangle S must be found. Let A be the sorted array of the IDs for
non-empty triangles in the HTM index of the partitioning table. If T is in A,
then S = T. Otherwise, S = A[hash(T) mod size(A)].

To generate data for T, the duplicator must take records from S and
transform sky positions to make them look like they lie in T.

Transforming Positions
----------------------

The vertices of a spherical triangles are linearly independent, and thus form
a basis for R^3. Coordinates in such a basis are known as spherical barycentric
coordinates. Let M be the 3x3 matrix with column vectors V1, V2 and V3,
corresponding to the vertices of the triangle. Multiplying a column vector by
the inverse of M transforms to spherical barycentric coordinates. Multiplying
by M transforms back to the original basis.

A position U from source triangle S is mapped to a position V from target
triangle T as follows:

> V = M_T * (M_S^-1 * U) = (M_T * M_S^-1) * U

In other words, U is transformed by the matrix that maps the vertices of
S to the vertices of T. Since the area and proportions of different HTM
triangles do not vary all that much, one can think of M_T * M_S^-1 as
something fairly close to a rotation. The fact that the transform is not quite
length preserving does not matter; after all, cartesian coordinates V and
k * V (k > 0) map to the same spherical coordinates. Unlike an approach
that shifts around copies of an input data set in spherical coordinate space,
there are no serious distortion issues to worry about near the poles.

Transforming Keys
-----------------

The duplicator must also adjust primary key column values. This is because
a particular source triangle can and usually will be mapped to more than one
target triangle, causing uniqueness constraint violations unless corrective
action is taken. Once a primary key column has been updated, the corresponding
foreign key columns must of course be updated to match.

Dealing with key updates is the reason for the special treatment of primary
keys during the indexing phase. Given source triangle S and target triangle T,
the HTM index of the input / partitioning table can be used to quickly obtain
all primary / foreign key values for an input record in triangle S. Let A be
the sorted array of key values for S, and let J be the original key value.
Then the output key value K is constructed by placing the HTM ID T in the
32 most significant bits of K, and the index of J in A in the 32 least
significant bits. This guarantees uniqueness for the primary key since a
triangle T is mapped to at most once. It also only requires localized
knowledge of key values (A) to compute.

Final Steps
-----------

Armed with the ability to transform positions and key values, data can be
generated for all triangles overlapping the duplication region. The only
remaining task is to partition the generated data. Rather than writing
generated data to disk, the duplicator partitions as it generates,
thereby avoiding one full read and write of the output table.

Example
=======

Here is a fully worked single node example for the PT1.2 `Source` and `Object`
tables. First, the `Source` and `Object` tables are dumped in TSV format:

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
      -e "SELECT * FROM Source;" > Source.tsv
~~~~

Now, assuming `QSERV_DIR` has been set to a Qserv install or checkout
directory, the `Source` and `Object` tables can be indexed as follows:

~~~~sh
CFG_DIR=$QSERV_DIR/admin/dupr/config/PT1.2

for TABLE in Object Source; do
    qserv-htm-index \
        --config-file=$CFG_DIR/$TABLE.cfg \
        --htm.level=8 \
        --in.csv.null=NULL \
        --in.csv.delimiter=$'\t' \
        --in.csv.escape=\\ \
        --in.csv.quote='"' \
        --in=$TABLE.tsv \
        --verbose \
        --mr.num-workers=6 \
        --mr.pool-size=32768 \
        --mr.block-size=16 \
        --out.dir=index/$TABLE
done
~~~~

Here, `--htm.level` specifies the subdivision level of the index to generate.
Using level 8 yields triangles that are about 0.25 degrees on a side; there
are 1102 level 8 triangles containing at least one PT1.2 object. Note that the
HTM subdivision level used for related tables must be consistent, or you
will get failures later on.

The `--in.csv.*` arguments describe the format of the dump file to the
indexer and `--in` gives the input file to use. Note that the `--in` option
can be specified multiple times. If `--in` is set to the name of a directory, 
then every regular file in that directory is treated as an input file.

The number of indexer threads to launch can be set via `--mr.num-workers`. The
preferred size of IO requests is specified in units of MiB via `--mr.block-size`,
and the amount of memory to use is (roughly) capped by `--mr.pool-size`, again
in units of MiB.

Further options are specified in a config file. Here is an extract of
`Object.cfg`:

    # Object table primary key column.
    id = objectId


    # Position columns other than the partitioning position.
    pos = [
        'ra_SG, decl_SG' # small galaxy model position.
    ]

    # Partitioning parameters.
    part = {
        # The partitioning position is the object's point-source model position.
        pos     = 'ra_PS, decl_PS'
        # The overlap radius in degrees.
        overlap = 0.01667
    }

    # Output CSV format.
    out.csv = {
        null      = '\\N'
        delimiter = '\t'
        escape    = '\\'
        no-quote  = true
    }

    in.csv = {
        # List of Object table column names, in order of occurrence.
        field = [
            objectId
            iauId
            ra_PS
            ...
        ]
    }

This config file is equivalent to the following command line options:
~~~~sh
    --id=objectId \
    --pos=ra_SG,decl_SG \
    --part.pos=ra_PS,decl_PS \
    --overlap=0.01667 \
    --out.csv.null=\\N \
    --out.csv.delimiter=$'\t' \
    --out.csv.escape=\\ \
    --out.csv.no-quote=true \
    --in.csv.field=objectId \
    --in.csv.field=iauId \
    --in.csv.field=ra_PS \
    ...
~~~~

Here, `--id` gives the name of the primary key column and `--part.pos` gives
the names of the RA and Dec columns for the partitioning position. The
`--out.csv.*` options control the output format, and the repeated
`--in.csv.field` option yields the input column name list.

Note: in general, command line options are processed first, followed by
options in config files. If more than one config file is specified, they are
processed in the order given. The first occurrence of an option specifies its
value, meaning that options given on the command line override those present
in config files, and that options in earlier config files override those in
later config files.

Both `qserv-htm-index` and `qserv-duplicate` skip unrecognized options. This
allows the same set of config files to be useful for both executables,
minimizing stutter.

Here is how to generate `Source` and `Object` chunks overlapping the
duplication region 60 <= RA <= 72 and -30 <= Dec <= -18 using the indexes
just generated:

~~~~sh
CFG_DIR=$QSERV_DIR/admin/dupr/config/PT1.2

for TABLE in Object Source; do
    qserv-duplicate \
        --config-file=$CFG_DIR/$TABLE.cfg \
        --config-file=$CFG_DIR/common.cfg \
        --in.csv.null=\\N \
        --in.csv.delimiter=$'\t' \
        --in.csv.escape=\\ \
        --in.csv.no-quote=true \
        --index=index/$TABLE/htm_index.bin \
        --part.index=index/Object/htm_index.bin \
        --verbose \
        --lon-min=60 --lon-max=72 --lat-min=-30 --lat-max=-18 \
        --mr.num-workers=6 \
        --mr.pool-size=32768 \
        --mr.block-size=16 \
        --out.dir=chunks/$TABLE
done
~~~~

For a description of the possible options, see the config files referenced
above. Alternatively, run `qserv-htm-index --help` or `qserv-duplicate --help`
for a complete list.

Estimating Duplicated Data Volumes
----------------------------------

A natural question is whether there is a way to estimate how large a duplicated
data set will be without actually generating it. Answering this question is the
purpose of the `qserv-estimate-stats` utility. Passing the same parameters you
would pass to `qserv-duplicate` will yield statistics over an estimate of the
duplicated chunk and sub-chunk populations, including an estimate of the total
record count. Continuing with the example above:

~~~~sh
qserv-estimate-stats \
    --config-file=config/PT1.2/Object.cfg \
    --config-file=config/PT1.2/common.cfg  \
    --index=index/Object/htm_index.bin \
    --lon-min=60 --lon-max=72 --lat-min=-30 --lat-max=-18 \
    --out.dir=.
~~~~

will print out estimated statistics for the duplicated `Object` table in JSON
format, and save the estimated chunk/sub-chunk populations to a file in the
current directory.

Distributed Operation
=====================

This works pretty well until the boss calls and demands 200TiB of data on
the double. In this situation, running the duplicator on a single node is
not going to cut it. Another problem arises when the cluster available
to you is a repurposed HPC cluster with very small amounts of storage per
node. In the example above, the PT1.2 `Object` table index is roughly 5GiB
and the `Source` index is roughly 90GiB. The Winter 2013 `ForcedSource` table
requires about 3TiB of space. What to do if a node does not have enough
local storage? It is possible to place the indexes and/or input on networked
storage (if available), but then indexing and duplication performance is
not likely to scale well with node count.

Both `qserv-htm-index` and `qserv-duplicate` have been designed with such
scenarios in mind, and can be run in distributed fashion. Here’s how.

Distributed Indexing
--------------------

The first task is to split up the input into pieces that fit on the local
storage of the N indexing nodes. Each indexing node should have free space
slightly larger than the amount of input it is processing, so that there is
room for it’s portion of the index.

Assuming that input pieces are stored in some well known location across the
cluster, one would then run something like:

~~~~sh
INPUT_DIR=/my/input/directory/
INDEX_DIR=/my/index/directory/
CFG_DIR=/my/config/directory/
TABLE=Table

qserv-htm-index \
    --config-file=$CFG_DIR/$TABLE.cfg \
    --in=$INPUT_DIR/$TABLE/ \
    --out.dir=$INDEX_DIR/$TABLE/ \
    --out.num-nodes=$M
~~~~

on each of the N duplication nodes. When M > 1, the contents of
`$INDEX_DIR/$TABLE` on each node will look something like:

    $INDEX_DIR/$TABLE/
     |___ htm_index.bin
     |___ node_00000/
     |    |___ htm_80000.ids
     |    |___ htm_80000.txt
     |____ node_00001/
     |    |___ htm_80001.ids
     |    |___ htm_80001.txt
     .
     .
     .

Note that if M = 1, the `node_XXXXX` directories are omitted. The indexer
assigns non-empty triangles to a duplication node by hashing, so that each
duplication node is assigned a roughly identical number of triangle files.
The `htm_index.bin` file at the root of the output directory tree stores a record
count for each non-empty triangle in the index. It is stored in a binary file
format that is “catenable”, meaning that the index file obtained by
concatenating the index files for inputs I1, I2, ... in any order is valid and
equivalent to the file that would be obtained by indexing the union of I1,
I2, etc. The `.ids` files are simply arrays of 8 byte integer IDs, and the
`.txt` files are CSV files containing one line per input record. Thus,
they too are catenable.

Once indexing is complete, the input data can be deleted from the indexing
nodes.

Gathering Index Data
--------------------

The next task is to gather/aggregate the index data such that the content of
a triangle is never spread over multiple nodes, and so that all nodes have an
`htm_index.bin` file describing the complete input.

Why are these things important? Recall that the duplicator needs to have all
the IDs of records in a particular triangle available in order to transform
key values from that triangle. Furthermore, the complete set of non-empty
triangles must be known to each duplication node so that they all consistently
choose a source triangle for a given target triangle.

Each of the M duplication nodes must therefore contact each of the N indexing
nodes. This is preferably done in randomized order so as to avoid all M nodes
simultaneously requesting data from a single indexing node at a time.
Duplication node D asks indexing node I for `$INDEX_DIR/htm_index.bin` and
all the files in `$INDEX_DIR/node_D/`, and either copies them or appends them
to existing files of the same name.

Once duplication nodes have gathered their data, the index data can be deleted
from the indexing nodes.

Distributed Duplication
-----------------------

Something like the following can now be run on each duplication node:

~~~~sh
INDEX_DIR=/my/index/directory/
CHUNK_DIR=/my/chunk/directory/
CFG_DIR=/my/config/directory/
TABLE=Table

qserv-duplicate \
    --config-file=$CFG_DIR/$TABLE \
    --index=$INDEX_DIR/$TABLE/htm_index.bin \
    --part.index=$INDEX_DIR/Object/htm_index.bin \
    --out.dir=$CHUNK_DIR/$TABLE/ \
    --out.num-nodes=$W
~~~~

The W in the script above refers to the number of Qserv workers - if W > 1,
then the duplicators assign chunks to worker nodes by hashing, and store the
pieces of the chunks they are capable of generating in dedicated subdirectories
named `node_XXXXX/`, as before:

    $CHUNK_DIR/$TABLE/
     |___ chunk_index.bin
     |___ node_00000/
     |    |___ chunk_4784_full.txt
     |    |___ chunk_4784_self.txt
     |    |___ chunk_4784.txt
     |____ node_00001/
     |    |___ chunk_4785_full.txt
     |    |___ chunk_4785_self.txt
     |    |___ chunk_4785.txt
     .
     .
     .

The file name prefix (`chunk_` by default) can be changed using the
`--part.prefix` option. The Qserv worker nodes can gather the chunks
assigned to them in exactly the same way as the duplication nodes
gather index data.

If the size of the indexes is small relative to the total amount of space
available on a single Qserv worker node, then a potentially more efficient
way of running distributed duplication is to copy the entire index to each
duplication node. Then, the following flags can be added:

    --out.num-nodes=$W
    --out.node=$w

This instructs the duplicator to only generate chunks belonging to Qserv
worker node w out of a total of W. If the duplication and worker nodes are
one and the same, then all post-index-copy network transfer can be avoided.

