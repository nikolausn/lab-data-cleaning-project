-- verify that year of traffic violations doesn't have unrealistic values like 0000 etc.
SELECT year_of_violation
	,count(*)
FROM (
	SELECT strftime('%Y', DATE (substr(Date_Of_Stop, 7, 4) || '-' || substr(Date_Of_Stop, 1, 2) || '-' || substr(Date_Of_Stop, 4, 2))) AS year_of_violation
	FROM TrafficViolation
	)
GROUP BY year_of_violation;

-- See if there are any records where year of violation is in future.
SELECT year_of_violation
FROM (
	SELECT strftime('%Y', DATE (substr(Date_Of_Stop, 7, 4) || '-' || substr(Date_Of_Stop, 1, 2) || '-' || substr(Date_Of_Stop, 4, 2))) AS year_of_violation
	FROM TrafficViolation
	)
GROUP BY year_of_violation
HAVING cast(year_of_violation AS INTEGER) > strftime('%Y', DATE ('now'));


-- Number of fatalities should be less than or equal to number of accidents.
SELECT Fatal
	,count(*)
FROM TrafficViolation
GROUP BY Fatal;

SELECT Accident
	,count(*)
FROM TrafficViolation
GROUP BY Accident;
-- Number of fatalities = 176; Number of accidents = 0

-- Number of distinct states are 67 when it should have been 50.
SELECT count(*)
FROM (
	SELECT DISTINCT STATE
	FROM TrafficViolation
	);

-- Filter rows where year of make is future.
-- 697 records have year of make in future, clearly all of them are dirty.
SELECT Year as year_of_make
	,count(*)
FROM TrafficViolation
GROUP BY Year
HAVING cast(year AS INTEGER) > strftime('%Y', DATE ('now'));
-- There are other observations for which year of make is invalid(1000, 1011, 10) etc.

-- Number of violations that contributed to accident don't match with number of accidents.
-- Number of accidents: 0; Contributed to accident: 18913.
SELECT Contributed_To_Accident
	,count(*)
FROM TrafficViolation
GROUP BY Contributed_To_Accident;

-- Total Number of distinct states should have been 50 but the following query returns 66 states.
SELECT Driver_State,count(*)
FROM TrafficViolation
GROUP BY Driver_State;

-- Similarly as above, there are 68 distinct DL states.
SELECT DL_State,count(*)
FROM TrafficViolation
GROUP BY DL_State;

SELECT Accident
	,count(*)
FROM TrafficViolation
GROUP BY Accident;

SELECT Model_1_1
	,count(*)
FROM TrafficViolation
GROUP BY Model_1_1;

