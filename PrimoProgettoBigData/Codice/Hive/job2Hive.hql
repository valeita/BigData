CREATE TABLE frequencyword (id INT,
			    productID STRING,
			    userID STRING,
			    profileName STRING,
			    num INT,
			    den INT,
			    score FLOAT,
			    time STRING,
			    summary STRING) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA LOCAL INPATH './Reviews.csv' OVERWRITE INTO TABLE frequencyword;
add jar unixTimeToYear.jar;
CREATE TEMPORARY FUNCTION unix_date AS 'com.example.hive.udf.Unix2Date';

CREATE TABLE medie as
SELECT cast(unix_date(time) as INT) AS time,productID,score
FROM frequencyword;

CREATE TABLE final as
SELECT time,productID,round(avg(score),2) AS media
FROM medie
WHERE time>2002 and time!=9999
GROUP BY time,productID;

INSERT OVERWRITE DIRECTORY 'outputJob2Hive'

SELECT productID,COLLECT_LIST(time) AS arrayanni, COLLECT_LIST(media) AS arraymedie
FROM (select * from final order by productID, time) t
GROUP BY productID;

