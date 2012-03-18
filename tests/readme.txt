
format of the files containing queries: <idA>_<descr>.sql
where <idA>:
  0xxx - supported, trivial (single object)
  1xxx - supported, simple (small area)
  2xxx - supported, medium difficulty (full scan)
  3xxx - supported, difficult /expensive (e.g. full sky joins)
  4xxx - supported, very difficult (eg near neighbor for large area)
  8xxx - will be supported in the future
  9xxx - unknown support

Corresponding results can be found in <idA>_<descr>.result files.

For select count(*) queries, the result should have form:
count:<value>

For select <columns> queries, the result should have form:
md5:<value of md5 of the entire result>
<result>
