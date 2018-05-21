CREATE TABLE produser (id INT,
		       prodID STRING,
		       userID STRING) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA LOCAL INPATH './Reviews.csv' OVERWRITE INTO TABLE produser;

CREATE TABLE couples AS
SELECT t1.prodID as prod1, t2.prodID as prod2
FROM produser as t1 JOIN produser as t2 ON t1.userID=t2.userID
WHERE t1.prodID<t2.prodID;


INSERT OVERWRITE DIRECTORY 'outputJob3Hive'

SELECT prod1, prod2, count(*) as utenti 
FROM couples
GROUP BY prod1,prod2;
