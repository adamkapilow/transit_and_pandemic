-- Assumes we have the following tables loaded in our database:
-- MTA_Subway_Stations
    -- Stop_Name: Text 
    -- Borough: Text
    -- GTFS_Latitude: Double
    -- GTFS_Longitude: Double
    -- Georeference: Text
-- Turnstile_Usage_Data_Jan_202x: One table for each of Jan 2020, 2021, 2022. 
    -- CA: Varchar(12)
    -- Unit: Varchar(12)
    -- SCP: Varchar(12)
    -- (First three identify a Turnstile)
    -- Station: Text
    -- Date: Date
    -- Time: Time
    -- Entries: INT
        -- Number on entry counter
    -- Exits: INT
        -- Number on exit counter

-- Getting rid of duplicate entries for north and south stops.
CREATE TABLE Subway_Stations AS
SELECT
Stop_Name,
MAX(Borough) AS Borough,
MAX(Latitude) AS Latitude,
MAX(Longitude) AS Longitude
FROM MTA_Subway_Stations
GROUP BY Stop_Name;

DROP TABLE MTA_Subway_Stations;


-- First we merge the three separate turnstile usage tables into one single table. 
-- Since the data is vastly oversampled at once every 4 hours,
-- We downsample to hours 0, 3, 6, 9, etc.
-- This won't preference any scheduling routine as 3 and 4 are coprime.

CREATE TABLE Turnstile_Usage_Data AS
SELECT * FROM 
(SELECT *
FROM Turnstile_Usage_Data_Jan_2020
-- WHERE MOD(Hour(Time), 3) = 0
UNION 
SELECT * FROM Turnstile_Usage_Data_Jan_2021
-- WHERE MOD(Hour(Time), 3) = 0
) AS cte
UNION SELECT * FROM Turnstile_Usage_Data_Jan_2022
-- WHERE MOD(Hour(Time), 3) = 0
;

-- Preparing for our analysis, we add in a Timestamp column,
-- And reorganize the turnstile ID into a single column 


ALTER TABLE Turnstile_Usage_Data
ADD (Timestamp Timestamp, Turnstile_ID Text);

UPDATE Turnstile_Usage_Data
SET Timestamp = Timestamp(Date, Time);

UPDATE Turnstile_Usage_Data
SET Turnstile_ID = Concat(CA, ' ', Unit, ' ', SCP);

-- Dropping redundant columns
ALTER TABLE Turnstile_Usage_Data
DROP CA,
DROP Unit,
DROP SCP,
DROP Date,
Drop Time;

-- The turnstiles have a counter that ticks up every time someone uses them. 
-- The Entry and Exit columns display that counter. 
-- So to get the amount of people that use a turnstile in a given time period, 
-- we take the difference in that counter between the beginning and end of that period.

-- We create an updated table keeping track of turnstile activity, and drop the old table.

CREATE TABLE Turnstile_Activity AS
SELECT *, Entries - LAG(Entries) OVER(PARTITION BY Turnstile_ID, Station, YEAR(Timestamp) ORDER BY Timestamp ASC) AS Entry_Activity, 
Exits - LAG(Exits) OVER(PARTITION BY Turnstile_ID, Station, YEAR(Timestamp) ORDER BY Timestamp ASC) AS Exit_Activity 
FROM Turnstile_Usage_Data;

DROP TABLE Turnstile_Usage_Data;

-- We now go about cleaning up this data. First we delete any time periods where negative people used
-- a given turnstile. 

SELECT (SELECT COUNT(*) from Turnstile_Activity WHERE  Entry_Activity < 0 OR Exit_Activity < 0) / COUNT(*)
FROM Turnstile_Activity;

-- Returns ~1%, safe to delete.

DELETE FROM Turnstile_Activity
WHERE Entry_Activity < 0 OR Exit_Activity < 0;

-- Now we use histograms to find erroneous remaining data, due to the counter rolling over or
-- other inconsistencies in the data.

SELECT FLOOR(Entry_Activity/1000)*1000 AS Entry_Activity_Bin, COUNT(*)
FROM Turnstile_Activity
GROUP BY Entry_Activity_Bin
ORDER BY Entry_Activity_Bin;

SELECT FLOOR(Exit_Activity/1000)*1000 AS Exit_Activity_Bin, COUNT(*)
FROM Turnstile_Activity
GROUP BY Exit_Activity_Bin
ORDER BY Exit_activity_Bin;

-- These both show the vast majority of the data between 0 and 1000 riders in a given time period
-- and it's safe to delete records with more than 10000 in either bin, as there are only a single digit number
-- in each of these bins

DELETE FROM Turnstile_Activity
WHERE Entry_Activity >= 10000
OR Exit_Activity >= 10000;

-- We add up all the turnstile activity to get the total number of people using a given
-- subway station in a given year.

CREATE TABLE Station_Ridership AS
SELECT Station, Year(Timestamp) AS Year, Sum(Entry_Activity) + Sum(Exit_Activity) AS Ridership
FROM Turnstile_Activity
GROUP BY Station, Year(Timestamp)
ORDER BY Station, Year;


-- Now our goal is to match up this ridership info with our table of Station names and coordinates
-- so we can display this result on a map. Unfortunately different namning conventions were used
-- in these two tables so we have to do some fixing. 

ALTER TABLE Subway_Stations
ADD COLUMN Matching_Name TEXT;

ALTER TABLE Ridership_Compared_2020
ADD COLUMN Matching_Name TEXT;

UPDATE Subway_Stations
SET Matching_Name = NULL;

UPDATE Ridership_Compared_2020
SET Matching_Name = NULL;


-- We remove all special characters and spaces, and set everything to lowercase
UPDATE Subway_Stations
SET Matching_Name = Lower(REGEXP_REPLACE(Stop_Name, "\\.| |-|/|'|", ''));

UPDATE Ridership_Compared_2020
SET Matching_Name = Lower(REGEXP_REPLACE(Station, "\\.| |-|/|'|", ''));

-- Finding unmatched stops in the station and turnstile database.
-- Match means either prefix or suffix matching.

SELECT Stop_Name
FROM Subway_Stations AS stations
LEFT JOIN
Ridership_Compared_2020 AS riders
ON
stations.Matching_Name LIKE CONCAT('%', riders.Matching_Name, '%')
WHERE riders.Station IS NULL
ORDER BY Stop_Name;


SELECT Station
FROM Subway_Stations AS stations
RIGHT JOIN
Ridership_Compared_2020 AS riders
ON
stations.Matching_Name LIKE CONCAT('%', riders.Matching_Name, '%')
WHERE stations.Stop_Name IS NULL
ORDER BY Station;

-- These results yielded about 50 mismatched stations. I matched them by hand 
-- in the table "Station Renaming"

CREATE TABLE Station_Renaming
(Station_Stop_Name TEXT ,
Turnstile_Stop_Name TEXT ,
New_Turnstile_Stop_Name TEXT NULL);

SELECT * FROM Ridership_Compared_2020;

-- I will use this small table to update the ridership_compared_2020
-- table, giving it new station names when needed.

ALTER TABLE Ridership_Compared_2020
ADD Updated_Station_Name Text;

WITH new_name_table AS (SELECT Station, CASE WHEN New_Turnstile_Stop_Name IS NOT NULL THEN New_Turnstile_Stop_Name 
		ELSE Station END AS Updated_Station_Name
FROM (SELECT * FROM Ridership_Compared_2020
LEFT JOIN
Station_Renaming
ON
Station = Turnstile_Stop_Name) as cte)

UPDATE Ridership_Compared_2020
JOIN new_name_table ON Ridership_Compared_2020.Station = new_name_table.Station
SET Ridership_Compared_2020.Updated_Station_Name = new_name_table.Updated_Station_Name;

-- With the new station names, we again apply text processing.

UPDATE Ridership_Compared_2020
SET Matching_Name = REPLACE(Lower(REGEXP_REPLACE(Updated_Station_Name, "\\.| |-|/|'|\t|", '')), CHAR(13), '');

-- Now we can package everything into our final result table, ready for visualization.
-- This table contains subway stations, their percent change of pre-pandemic ridership in 2021 and 2022,
-- their borough, and their latitude and longitude coordinates.

CREATE TABLE Result
SELECT Stop_Name, Ridership_2021_vs_2020, Ridership_2022_vs_2020, Borough, Latitude, Longitude
FROM Subway_Stations JOIN Ridership_Compared_2020
ON Subway_Stations.Matching_Name = Ridership_Compared_2020.Matching_Name
ORDER BY Stop_Name;