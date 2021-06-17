add jar /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;
SET hive.cli.print.header=false; 
SET mapred.input.dir.recursive=true; 
SET hive.mapred.supports.subdirectories=true;
USE lazichnyjiv;

DROP TABLE IF EXISTS kkt_text;
CREATE TABLE kkt_text
   STORED AS TEXTFILE
   LOCATION '/tmp/hob2021258_task2'
   AS
select * from kkt_document_json;

select kkt_text.content.userinn, SUM(kkt_text.content.totalsum) as total from kkt_text 
where kkt_text.subtype="receipt" GROUP BY kkt_text.content.userinn
ORDER BY total DESC LIMIT 1;
