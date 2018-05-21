CREATE TABLE frequencyword (id INT,
			    productID STRING,
			    userID STRING,
			    profileName STRING,
			    num INT,
			    den INT,
			    score INT,
			    time STRING,
			    summary STRING) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA LOCAL INPATH './Reviews.csv' OVERWRITE INTO TABLE frequencyword;
add jar unixTimeToYear.jar;
CREATE TEMPORARY FUNCTION unix_date AS 'com.example.hive.udf.Unix2Date';
CREATE TEMPORARY FUNCTION format_summary AS 'com.example.hive.udf.FormatSummary';

CREATE VIEW support as 
SELECT id, 
       productID, 
       userId, 
       unix_date(time) as time,
       word  
       
FROM frequencyword LATERAL VIEW explode(split(format_summary(summary), ' ')) wordTable AS word;

CREATE VIEW allyears AS
SELECT time, word, Count(*) as count
FROM support
GROUP BY time, word
ORDER BY time, count DESC;

CREATE VIEW allyearsrows AS
select time, 
       word, 
       count,
       ROW_NUMBER() OVER (PARTITION BY time ORDER BY count DESC, word) as riga 
from allyears;

INSERT OVERWRITE DIRECTORY 'outputJob1Hive'

select time,word,count 
from allyearsrows 
where riga<11 and time !=9999 and time !=1970;
