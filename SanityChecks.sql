.headers on
.mode csv

-- verify that year of traffic violations doesn't have unrealistic values like 0000 etc.
.output SqlChecksDistinct.year_of_violation.csv
SELECT year_of_violation
	,count(*)
FROM (
	SELECT strftime('%Y', DATE (substr(Date_Of_Stop, 7, 4) || '-' || substr(Date_Of_Stop, 1, 2) || '-' || substr(Date_Of_Stop, 4, 2))) AS year_of_violation
	FROM TrafficViolation
	)
GROUP BY year_of_violation;


-- See if there are any records where year of violation is in future.
.output SqlChecksSanity.future_year_of_violation.csv
SELECT year_of_violation
FROM (
	SELECT strftime('%Y', DATE (substr(Date_Of_Stop, 7, 4) || '-' || substr(Date_Of_Stop, 1, 2) || '-' || substr(Date_Of_Stop, 4, 2))) AS year_of_violation
	FROM TrafficViolation
	)
GROUP BY year_of_violation
HAVING cast(year_of_violation AS INTEGER) > strftime('%Y', DATE ('now'));


-- Number of fatalities should be less than or equal to number of accidents.
.output SqlChecksDistinct.Fatal.csv
SELECT Fatal
	,count(*)
FROM TrafficViolation
GROUP BY Fatal;

.output SqlChecksDistinct.Accident.csv
SELECT Accident
	,count(*)
FROM TrafficViolation
GROUP BY Accident;
-- Number of fatalities = 176; Number of accidents = 0

-- Filter rows where year of make is future.
-- 697 records have year of make in future, clearly all of them are dirty.
.output SqlChecksSanity.future_year_of_make.csv
SELECT Year as year_of_make
	,count(*)
FROM TrafficViolation
GROUP BY Year
HAVING cast(year AS INTEGER) > strftime('%Y', DATE ('now'));
-- There are other observations for which year of make is invalid(1000, 1011, 10) etc.

-- Number of violations that contributed to accident don't match with number of accidents.
-- Number of accidents: 0; Contributed to accident: 18913.
.output SqlChecksDistinct.ContributedToAccident.csv
SELECT Contributed_To_Accident
	,count(*)
FROM TrafficViolation
GROUP BY Contributed_To_Accident;

.output SqlChecksDistinct.Model.csv
SELECT Model_1_1
	,count(*)
FROM TrafficViolation
GROUP BY Model_1_1;

