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

DROP TABLE IF EXISTS weather_delay; 
CREATE EXTERNAL TABLE weather_delay 
(
Year int,
Delay double )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE LOCATION 's3://sparkvshive/logs/';

INSERT OVERWRITE TABLE weather_delay
Select 
   Year,
   avg((WeatherDelay/ArrDelay)*100) as Delay
FROM delayed_flights Group by Year;



