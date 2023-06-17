CREATE EXTERNAL TABLE `accelerometer_landing`(
  `user` string COMMENT 'from deserializer', 
  `timestamp` timestamp COMMENT 'from deserializer', 
  `x` double COMMENT 'from deserializer', 
  `y` double COMMENT 'from deserializer', 
  `z` double COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://stedi-data-nam/temp/accelerometer_landing_temp/'
TBLPROPERTIES (
  'CreatedByJob'='Accelerometer Landing', 
  'CreatedByJobRun'='jr_6b6070ab1a1d11073418cfa9b6993e66cc63698fdb5db406a166abf748ae648a', 
  'classification'='json')