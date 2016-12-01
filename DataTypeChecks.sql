.headers on
.mode csv

-- check if any of the values in Date_Of_Stop column is not a valid date
.output SqlChecksDTViolate.Date_Of_Stop.csv
SELECT *
FROM TrafficViolation
WHERE DATE(substr(Date_Of_Stop, 7, 4) || '-' || substr(Date_Of_Stop, 1, 2) || '-' || substr(Date_Of_Stop, 4, 2)) IS NULL;

-- check if any of the values in Time_Of_Stop column is not a valid time
.output SqlChecksDTViolate.Time_Of_Stop.csv
SELECT *
FROM TrafficViolation
WHERE TIME(Time_Of_Stop) IS NULL;

-- check if all the available values in latitude are numeric.
-- In more sophisticated databases like postgres, we could have used geospatial
-- data libraries to see if latitude is valid.
.output SqlChecksDTViolate.latitude.csv
SELECT *
FROM TrafficViolation
WHERE ifnull(latitude, '') != '' -- only check for available latitudes
	 -- successful cast means valid latitude
	AND CAST(latitude AS NUMERIC) IS NOT latitude;
	
-- check if all the available values in longitude are numeric.
-- In more sophisticated databases like postgres, we could have used geospatial
-- data libraries to see if longitude is valid.
.output SqlChecksDTViolate.longitude.csv
SELECT *
FROM TrafficViolation
WHERE ifnull(longitude, '') != '' -- only check for available longitude;
	 -- successful cast means valid longitude
	AND CAST(longitude AS NUMERIC) IS NOT longitude;

