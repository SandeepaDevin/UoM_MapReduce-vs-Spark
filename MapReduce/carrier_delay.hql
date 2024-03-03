SET hive.compute.query.using.stats=false;
SET hive.optimize.index.filter=false;

DROP TABLE IF EXISTS delayed_flights;
CREATE TABLE IF NOT EXISTS delayed_flights(
   Year int,
   Month int,
   DayofMonth int,
   DayOfWeek int,
   DepTime int,
   CRSDepTime int,
   ArrTime int,
   CRSArrTime int,
   UniqueCarrier string,
   FlightNum int,
   TailNum string,
   ActualElapsedTime int,
   CRSElapsedTime int,
   AirTime int,
   ArrDelay int,
   DepDelay int,
   Origin string,
   Dest string,
   Distance int,
   TaxiIn int,
   TaxiOut int,
   Cancelled int,
   CancellationCode string,
   Diverted int,
   CarrierDelay int,
   WeatherDelay int,
   NASDelay int,
   SecurityDelay int,
   LateAircraftDelay int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH 's3://sparkvshive/DelayedFlights-updated.csv' INTO TABLE delayed_flights;

DROP TABLE IF EXISTS carrier_delay; 
CREATE EXTERNAL TABLE carrier_delay 
(
Year int,
Delay double )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE LOCATION 's3://sparkvshive/logs/';

Select 
   Year,
   avg((CarrierDelay/ArrDelay)*100) as Delay
FROM delayed_flights Group by Year;

