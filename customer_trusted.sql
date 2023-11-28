CREATE EXTERNAL TABLE `customer_trusted`(
  `customername` string,
  `email` string,
  `birthday` string,
  `serialnumber` string,
  `sharewithresearchasofdate` bigint)
LOCATION
  's3://udacity-spark-bucket/customer/trusted'

INSERT INTO "stedi"."customer_trusted" (customername, email, birthday, serialnumber, sharewithresearchasofdate)
SELECT customername, email, birthday, serialnumber, sharewithresearchasofdate
FROM "stedi"."customer_landing"
WHERE sharewithresearchasofdate IS NOT NULL;