-- We have created a bunch of look-up tables for gender, arrest type etc.

-- Following query is checking if gender is there in the lookup table.
-- Query returns traffic violation rows for which violator's gender is not in lookup.
SELECT *
FROM TrafficViolation
WHERE Gender NOT IN (
		SELECT Gender
		FROM Gender
		);
		

-- In the existing load, we have created all these lookup tables just by taking
-- distinct of the values in the violations table. So, we won't see any violations,
-- but the idea is that if we had a daily load, then we could capture new records
-- violating foreign key.
