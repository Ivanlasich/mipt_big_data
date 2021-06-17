add jar /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar; 
SET hive.cli.print.header=false; 
SET mapred.input.dir.recursive=true;
SET hive.mapred.supports.subdirectories=true;


USE lazichnyjiv;

select index1, min(dat) as date, total1 from( 
select * from( 
select task3_text.index as index1, MAX(task3_text.total) as total1 from task3_text 
GROUP BY task3_text.index) ex1 
left OUTER join (
select task3_text.index as index2, task3_text.day as dat, task3_text.total as total2 from task3_text ) ex2 
on (ex1.index1 = ex2.index2) and (ex1.total1 = ex2.total2) 
) tmp
where index1 is not NULL 
group by index1,total1 
order by index1;
