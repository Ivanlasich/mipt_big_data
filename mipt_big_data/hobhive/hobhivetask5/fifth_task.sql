add jar /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar; 
SET hive.cli.print.header=false; 
SET mapred.input.dir.recursive=true; 
SET hive.mapred.supports.subdirectories=true; 
USE lazichnyjiv;

select DISTINCT(tmp1.index) from(
select index, subtype, sum(tmp.check) over(partition by tmp.index order by tmp.date rows between 1 preceding and 0 following) as ex_sum from(
select kkt_text.content.userinn as index, kkt_text.subtype as subtype, kkt_text.content.datetime.date as date, case kkt_text.subtype 
when 'openShift' then 1 
when 'closeShift' then -1 
when 'receipt' then 0
end as check from kkt_text
where kkt_text.subtype != 'fiscalReport'
order by date
) tmp
) tmp1
where tmp1.ex_sum = -1 and tmp1.subtype='receipt'
order by tmp1.index limit 50;
