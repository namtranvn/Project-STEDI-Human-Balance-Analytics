CREATE EXTERNAL TABLE `customer_landing`(
  `customername` string COMMENT 'from deserializer', 
  `email` string COMMENT 'from deserializer', 
  `phone` bigint COMMENT 'from deserializer', 
  `birthday` date COMMENT 'from deserializer', 
  `serialnumber` string COMMENT 'from deserializer', 
  `registrationdate` timestamp COMMENT 'from deserializer', 
  `lastupdatedate` timestamp COMMENT 'from deserializer', 
  `sharewithresearchasofdate` timestamp COMMENT 'from deserializer', 
  `sharewithpublicasofdate` timestamp COMMENT 'from deserializer', 
  `sharewithfriendsasofdate` timestamp COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://stedi-data-nam/temp/customer_landing_temp/'
TBLPROPERTIES (
  'CreatedByJob'='Customer Landing', 
  'CreatedByJobRun'='jr_9e0ed9810df48c05adf647f502b9d25a4fe945da2bcf27978a5f420745952c6f', 
  'classification'='json')