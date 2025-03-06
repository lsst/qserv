
This data set was created using the following commands:

create database xx;
use xx;
create table Object select * from rplante_PT1_2_u_pt12prod_im3000.Object where objectId % 49999=1;
create table Source select * from rplante_PT1_2_u_pt12prod_im3000.Source where objectId % 49999=1;
create table Filter select * from rplante_PT1_2_u_pt12prod_im3000.Filter;

create table Science_Ccd_Exposure 
    select * 
    from rplante_PT1_2_u_pt12prod_im3000.Science_Ccd_Exposure 
    where scienceCcdExposureId in (select scienceCcdExposureId FROM Source);

create table RefSrcMatch
    select rsm.* 
    from rplante_PT1_2_u_pt12prod_im3000.RefSrcMatch rsm
    join Source s where rsm.sourceId=s.sourceId;

create table SimRefObject
    select sro.*
    from rplante_PT1_2_u_pt12prod_im3000.SimRefObject sro
    join RefSrcMatch srm
    where srm.refObjectId=sro.refObjectId;


Then:
mysqldump -u<u> -p<p> xx Filter -T/tmp/
mysqldump -u<u> -p<p> xx Object -T/tmp/
mysqldump -u<u> -p<p> xx Source -T/tmp/
mysqldump -u<u> -p<p> xx Science_Ccd_Exposure -T/tmp/
mv /tmp/Filter.* /tmp/Object.* /tmp/Source.* /tmp/Science_Ccd_Exposure.* case01/data/
rename files: .txt --> .tsv, .sql --> .schema
gzip .tsv files
drop database xx;
