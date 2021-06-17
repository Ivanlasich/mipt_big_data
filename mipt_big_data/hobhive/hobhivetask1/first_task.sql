add jar /opt/cloudera/parcels/CDH/lib/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar; 
SET hive.cli.print.header=false; 
SET mapred.input.dir.recursive=true; 
SET hive.mapred.supports.subdirectories=true; 
USE lazichnyjiv;
DROP TABLE IF EXISTS kkt_document_json;
CREATE external TABLE kkt_document_json (
    id struct<soid: string>,
    fsId string,
    kktRegId string,
    subtype string,
    receiveDate struct<date: TIMESTAMP>,
    protocolVersion string,
    ofdId string,
    protocolSubversion bigint,
    content struct<nds18:string, cashTotalSum:string, totalSum:string, receiptCode:string, taxationType:string,requestNumber:string,ecashTotalSum:string,operationType: string,fiscalDriveNumber:string,operator:string,rawData:string,shiftNumber:bigint,user:string,items:string,kktRegId:string,userInn:string,fiscalSign:bigint,fiscalDocumentnumber:bigint,code:bigint,dateTime:struct<date:TIMESTAMP>>,
    documentId bigint)

ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH serdeproperties( 'ignore.malformed.json' = 'true', 'mapping.id' = '_id', 'mapping.soid' = '$oid', 'mapping.date' = '$date')
STORED AS TEXTFILE LOCATION '/data/hive/fns2';

select * from kkt_document_json limit 50;
