# DCF - A generic framework to automate parts of data cleaning
Based on the problems that we faced with OpenRefine we decided to build an inhouse data cleaning program which can do data cleaning operations just like OpenRefine but consume less memory and can automate our cleaning process. First of all, we build the framework using Python as text processing tool. Next the data cleaning framework use case are:
* can do common transform for Trim, Upper, Lower, Regular Expression Replace, Collapse Whit Space Characters
* can merge values in some fields as an input for OpenRefine cluster and merge process
* can do merging Operation by reading OpenRefine extracted configuration json file and apply the replaced value to the dataset
* can upload the cleaned values into SQLite Database

Finally, we named the tool as Data Cleaning Framework (DCF).

## Data Cleaning Framework (DCF) 
DCF features are
* Common Transform: reading input file, applying common transform operation to particular fields in the dataset and write new output cleaned file.
	* upper: transform the fields into upper case characters value
	* lower: transform the fields into lower case characters value
	* trim: erase / trim whitespace characters in left side or right side of the value on some fields
	* colspace: collapse white space characters (more than one space) inside the fields value
	* regexrep: replace value in a field that match regular expression pattern into other characters
* Aggregate: select some fields, group and count values in a field, merge all aggregated values in each predefined fields into one file.
	* select: select particular fields defined from parameter into new output file.
	* group: group and count values in each fields defined in parameter and output the aggregated value into separated output files based on the field names.
	* merge: aggregate values in predefined fields and combine those aggregated values into one small output file. This output file can be used for OpenRefine cluster and merge operation.
* Automated Process: run cleanup operation based on predefined configuratin
	* init: read DCF json configuration file and apply cleanup operation based on the step defined in the configuration.
	* massedit: read OpenRefine extracted json configuration file and apply openrefine massedit operation to the dataset.
* SQL Operation: load the dataset into database and perform select query from the database;
	* loadtable: read csv dataset and directly load the dataset into SQLite database file.
	* runquery: read file that contains sql query and perform select query to the database defined in parameter and output the result into new csv dataset.

## Running the Script for Montgomery Traffic Violation Data
To run the script for Montgomery Traffic Violation Dataset:

1. Clone this github repository using
```
git clone https://github.com/nikolausn/lab-data-cleaning-project
```
2. Make sure you have Python 3.5 installed in your system. 
3. For the script to automatically called the correct python make sure to make symbolic link to python 3.5 in /usr/local/bin.
Example of making a symbolic link
```
ln -s /usr/python/anaconda/bin/python3.5 /usr/local/bin/python
```
... If by any chance you can't make the symbolic link then you can run the script by calling the python3.5 executable first for single call or
add them in the script_prod_execute_yw.sh for all python scripts call.
4. Download the dataset from [Montgomery Traffic Violation Dataset](https://data.montgomerycountymd.gov/api/views/4mse-ku6q/rows.csv?accessType=DOWNLOAD) . 
...Be warned that the original dataset is quite big with more then 800k rows and 35 columns. Another option is using sample data Traffic_Violations.csv 
provided in this github with 5,000 rows.
5. run the main batch script using this command.
```
./script_prod_execute_yw.sh <dataset_name> <init_configuration> <output_database_filename>
```
For example
```
./script_prod_execute_yw.sh Traffic_Violations.csv data-cleaning-config-2.json output_montgomery.sqlite
```
data-cleaning-config-2.json is a configuration file created for cleaning this Montgomery dataset only.

If you use the original dataset, execution time for the whole batch process to finish is approximately within 35 to 40 minutes.
so be patient :D, cleaning process is tedious if you run it manually :)

