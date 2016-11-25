#!/bin/bash
# add rowid
echo "add rowid"
./data-cleaning-framework.py id -in $1 -out prodTrafficId -f "ROWID"

# data cleaning init using data-cleaning-config-2.json
echo "init cleaning using configuration: $2"
./data-cleaning-framework.py init -in prodTrafficId -out prodTraffic1 -c $2

#select particular fields for merging
echo "select particular fields for merging"
./data-cleaning-framework.py merge -in prodTraffic1 -out prodTrafficMerge -f "Model 1,Make 1,Description 1,Driver City 1,Location 1"

#massedit using config from openrefine
echo "massedit using config from openrefine"
./data-cleaning-framework.py massedit -in prodTraffic1 -out prodTraffic2 -c openrefineconfig.json

#select particular cleaned field for loading
echo "select particular cleaned field for loading into table"
./data-cleaning-framework.py select -in prodTraffic2 -out prodTraffic3 -f "ROWID,Date Of Stop,Time Of Stop,Agency,SubAgency,Latitude,Longitude,Accident,Belts,Personal Injury,Property Damage,Fatal,Commercial License,HAZMAT,Commercial Vehicle,Alcohol,Work Zone,State,Year,Violation Type,Article,Contributed To Accident,Race,Gender,Driver State,DL State,Geolocation,Location 1,Color 1,Arrest Type 1,Arrest Type 2,VehicleType 1,VehicleType 2,Charge 1 1,Charge 1 2,Description 1 1,Make 1 1,Model 1 1,Driver City 1 1"

#load to database
echo "load to database"
./data-cleaning-framework.py loadtable -in prodTraffic3 -out $3 -t TrafficViolation

#generate lookup value
echo "generate lookup value"
./data-cleaning-framework.py group -in prodTraffic3 -f "SubAgency,Property Damage,State,VehicleType 2,Violation Type,Article,Race,Gender,Arrest Type 2" -out "prodTraffic"

#load lookup value to table
echo "load lookup value to table"
./data-cleaning-framework.py loadtable -in "prodTraffic.SubAgency" -out $3 -t "SubAgency"
./data-cleaning-framework.py loadtable -in "prodTraffic.Property Damage" -out $3 -t "PropertyDamage"
./data-cleaning-framework.py loadtable -in "prodTraffic.State" -out $3 -t "State"
./data-cleaning-framework.py loadtable -in "prodTraffic.VehicleType 2" -out $3 -t "VehicleType"
./data-cleaning-framework.py loadtable -in "prodTraffic.Violation Type" -out $3 -t "ViolationType"
./data-cleaning-framework.py loadtable -in "prodTraffic.Article" -out $3 -t "Article"
./data-cleaning-framework.py loadtable -in "prodTraffic.Race" -out $3 -t "Race"
./data-cleaning-framework.py loadtable -in "prodTraffic.Gender" -out $3 -t "Gender"
./data-cleaning-framework.py loadtable -in "prodTraffic.Arrest Type 2" -out $3 -t "ArrestType"


# cleanup intermediate files.
rm -rf prodTraffic*
#sql check cleaning result and ordered by frequency
#select "Location 1",count(1) as total from TrafficViolation
#group by "Location 1" order by total desc;
#
#select "Description 1 1",count(1) as total from TrafficViolation
#group by "Description 1 1" order by total desc;
#
#select "Make 1 1",count(1) as total from TrafficViolation
#group by "Make 1 1" order by total desc;
#
#
#concentrate for top 10 frequency car maker
#HOND -> HONDA
#MERZ -> MERCEDES
#MITS -> MITSUBISHI
#CADI -> CADILLAC
#LINC -> LINCOLN
#TOY -> TOYOTA
#TOYTA -> TOYOTA
#INTL -> INTERNATIONAL
#TOTOYA -> TOYOTA
#TOYOTOA -> TOYOTA
#
#select "Driver City 1 1",count(1) as total from TrafficViolation
#group by "Driver City 1 1" order by total desc;
#
#X -> ""
#
