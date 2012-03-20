
directory structure:
  case<number>
    queries
    dataSet<number>

data from case<number> must be
loaded into database called qservTest_case<number>



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



## Example of producing a subset of data for testing:

create table Filter like rplante_PT1_2_u_pt12prod_im3000.Filter;
insert into Filter select * from rplante_PT1_2_u_pt12prod_im3000.Filter;

create table Object like rplante_PT1_2_u_pt12prod_im3000.Object;
insert into Object select * from rplante_PT1_2_u_pt12prod_im3000.Object where objectId % 50000 = 0;

create table Source like rplante_PT1_2_u_pt12prod_im3000.Source;
insert into Source select * from rplante_PT1_2_u_pt12prod_im3000.Source where objectId % 50000 = 0;

