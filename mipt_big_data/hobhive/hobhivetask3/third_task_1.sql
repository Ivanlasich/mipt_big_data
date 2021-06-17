add jar /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar; 
SET hive.cli.print.header=false; 
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;

USE lazichnyjiv; 
DROP TABLE IF EXISTS task3_text; 
create table task3_text
    STORED AS TEXTFILE
    LOCATION '/tmp/hob2021258_task2'
AS select kkt_text.content.userinn as index, DAY(kkt_text.content.datetime.date) as day, SUM(coalesce(kkt_text.content.totalsum,0)) as total from kkt_text
group by kkt_text.content.userinn,  DAY(kkt_text.content.datetime.date);
