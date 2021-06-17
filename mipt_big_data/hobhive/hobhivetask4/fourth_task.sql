add jar /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;
SET hive.cli.print.header=false; 
SET mapred.input.dir.recursive=true; 
SET hive.mapred.supports.subdirectories=true;
USE lazichnyjiv;



select tmp.index1 as index, round(tmp.avg_sum1) as avg_sum1, round(tmp.avg_sum2) as avg_sum2 from( 
select ex1.index1 as index1, ex1.avg_sum1 as avg_sum1, coalesce(ex2.avg_sum2,0) as avg_sum2 from( 
select kkt_text.content.userinn as index1, coalesce(AVG(kkt_text.content.totalsum),0) as avg_sum1 from kkt_text 
where HOUR(kkt_text.content.datetime.date) < 13 and kkt_text.subtype="receipt" 
GROUP BY kkt_text.content.userinn) ex1
   left OUTER join 
(select kkt_text.content.userinn as index2, coalesce(AVG(kkt_text.content.totalsum),0) as avg_sum2 from kkt_text 
where HOUR(kkt_text.content.datetime.date) >= 13 and kkt_text.subtype="receipt" 
GROUP BY kkt_text.content.userinn) as ex2 
on (ex1.index1 = ex2.index2)) tmp 
where avg_sum1 > avg_sum2
order by avg_sum1 limit 50;
