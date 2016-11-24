#!/bin/bash
# add rowid

#@begin main_script @desc work flow for cleaning Traffic Violation file
#@in TrafficViolationFile @uri file:{trafficviolationfile}.csv
#@in InitConfigFile @uri file:{initconfigfile}.json
#@in MassEditOpenRefine
#@param DBNameOutput
#@out CleanedFile @uri file:{cleanedfile}.csv
#@out CleanedDB @uri file:{DBNameOutput}.db
#@out MergedFile @uri file:{mergedfile}.csv
#@out ProdTrafficMergedFile

#@begin Add_Rowid @desc adding rowid from the input file
#@in TrafficViolationFile

#@out ProdTrafficIdFile @uri file:prodTrafficId
echo "add rowid"
./data-cleaning-framework.py id -in $1 -out prodTrafficId -f "ROWID"
#@end Add_Rowid

#@begin Init_Cleaning @desc init cleaning using data-cleaning-config-2.json
#@in ProdTrafficIdFile
#@in InitConfigFile
#@out ProdTrafficInitFile @uri file:prodTraffic1
# data cleaning init using data-cleaning-config-2.json
echo "init cleaning using configuration: $2"
./data-cleaning-framework.py init -in prodTrafficId -out prodTraffic1 -c $2
#@end Init_Cleaning

#@begin MergedFile @desc Merge fields for open refine cluster and merging purpose
#@in ProdTrafficInitFile
#@param field_names as field_name
#@out ProdTrafficMergedFile @uri file:prodTrafficMerge
#select particular fields for merging
echo "select particular fields for merging"
./data-cleaning-framework.py merge -in prodTraffic1 -out prodTrafficMerge -f "Model 1,Make 1,Description 1,Driver City 1,Location 1"
#@end MergedFile

#@begin OpenRefineMerge @desc merging values using open refine
#@in ProdTrafficMergedFile
#@out MassEditOpenRefine @uri file:testproduction.json
#@end OpenRefineMerge

#@begin MassEdit @desc massedit values in fields using openrefine extract config file
#@in ProdTrafficInitFile
#@in MassEditOpenRefine @uri file:testproduction.json
#@out ProdTrafficMassEditFile @uri file:prodTraffic2
#massedit using config from openrefine
echo "massedit using config from openrefine"
./data-cleaning-framework.py massedit -in prodTraffic1 -out prodTraffic2 -c testproduction.json
#@end MassEdit


#@begin SelectField @desc select fields to be loaded into database
#@in ProdTrafficMassEditFile
#@param selected_field
#@out CleanedFile @uri file:prodTraffic3
#select particular cleaned field for loading
echo "select particular cleaned field for loading into table"
./data-cleaning-framework.py select -in prodTraffic2 -out prodTraffic3 -f "ROWID,Date Of Stop,Time Of Stop,Agency,SubAgency,Latitude,Longitude,Accident,Belts,Personal Injury,Property Damage,Fatal,Commercial License,HAZMAT,Commercial Vehicle,Alcohol,Work Zone,State,Year,Violation Type,Article,Contributed To Accident,Race,Gender,Driver State,DL State,Geolocation,Location 1,Color 1,Arrest Type 1,Arrest Type 2,VehicleType 1,VehicleType 2,Charge 1 1,Charge 1 2,Description 1 1,Make 1 1,Model 1 1,Driver City 1 1"
#@end SelectField

#@begin LoadDatabase @desc load selected fields to database
#@in CleanedFile
#@in DBNameOutput
#@out CleanedDB @uri file:{DBNameOutput}
#load to database
echo "load to database"
./data-cleaning-framework.py loadtable -in prodTraffic3 -out $3 -t TrafficViolation
#@end LoadDatabase


#@begin Generatelookup @desc generate lookup values file from cleaned file
#@in CleanedFile
#@param lookup_fields as lookup_field
#@param output_prefix
#@out lookup_file @uri file:{output_prefix}.{lookup_field}
#generate lookup value
echo "generate lookup value"
./data-cleaning-framework.py group -in prodTraffic3 -f "SubAgency,Property Damage,State,VehicleType 2,Violation Type,Article,Race,Gender,Arrest Type 2" -out "prodTraffic"
#@end GenerateLookup

#@begin LoadLookupValue @desc load lookup values to database
#@in lookup_file
#@out CleanedDB @uri file:{DBNameOutput}
#load lookup value to table
echo "load lookup value to table"
./data-cleaning-framework.py loadtable -in "prodTraffic.SubAgency" -out $3 -t "SubAgency"
./data-cleaning-framework.py loadtable -in "prodTraffic.Property Damage" -out $3 -t "Property Damage"
./data-cleaning-framework.py loadtable -in "prodTraffic.State" -out $3 -t "State"
./data-cleaning-framework.py loadtable -in "prodTraffic.VehicleType 2" -out $3 -t "VehicleType"
./data-cleaning-framework.py loadtable -in "prodTraffic.Violation Type" -out $3 -t "Violation Type"
./data-cleaning-framework.py loadtable -in "prodTraffic.Article" -out $3 -t "Article"
./data-cleaning-framework.py loadtable -in "prodTraffic.Race" -out $3 -t "Race"
./data-cleaning-framework.py loadtable -in "prodTraffic.Gender" -out $3 -t "Gender"
./data-cleaning-framework.py loadtable -in "prodTraffic.Arrest Type 2" -out $3 -t "Arrest Type"
#@end LoadLookupValue

#@end main_script