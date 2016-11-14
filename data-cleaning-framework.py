#!/usr/local/bin/python
import re;
import csv;
import json;
import dcvalue;
import argparse;
from optparse import OptionParser;
import sys;

class DCCommand:    
    def __init__(self,dcValue):
        self.dcValue = dcValue;
    
    def command(self,cmdObject,row=None,rownum=None):
        fname = cmdObject['fname'];
        if(fname=='leftTrim'):
            self.dcValue.leftTrim();
        elif(fname=='rightTrim'):
            self.dcValue.rightTrim();
        elif(fname=='trim'):
            self.dcValue.trim();
        elif(fname=='collapseWhiteSpace'):
            self.dcValue.collapseWhiteSpace();
        elif(fname=='toUpper'):
            self.dcValue.toUpper();
        elif(fname=='toLower'):
            self.dcValue.toLower();
        elif(fname=='regexReplace'):
            regex = cmdObject['fparam']['regex'];
            replace = cmdObject['fparam']['replace'];
            self.dcValue.regexReplace(regex,replace);
        else:
            #check if it's custom function
            if (cmdObject['fcustom'] and not row is None):
                module = __import__(cmdObject['file']);
                customFunction = getattr(module, cmdObject['module']);
                #customFunction(self.dcValue.value,'Color',row);
                self.dcValue.customFunction(customFunction,field,row,rownum);
                                            
        return self.dcValue;      
        
class DCColumnCommand:    
    def __init__(self,dcColumn):
        self.dcColumn = dcColumn;
    
    def split(self,splitstring,column,rowdata,split):
        myval = column.value;
        splitvals = myval.split(splitstring);
        i=1;
        for oneval in splitvals:
            if not "%s %s" % (field,i) in row.keys():
                row["%s %s" % (field,i)] = [None] * len(row[field]);
            myvalue = dcvalue.DCValue(oneval);
            row["%s %s" % (field,i)][rownum] = myvalue.trim().value;
            i=i+1    

def runConfig():                               
    #read jsonconfig file
    jsonConfig = {};
    with open("data-cleaning-config.json") as json_file:
        jsonConfig = json.load(json_file)
    
    #start data cleaning workflow
    #change fields array into tuple
    #fieldName = tuple(jsonConfig['fields']);
    #read csv file base on fields definition
    #open file
    writer = csv.writer(open("test", 'w'))
    dcFile = open(jsonConfig['fileName'], 'r');
    #skip header
    i = 0;
    #for i in range(1,jsonConfig['skipRow']):
    next(dcFile);
    
    #i=0;
    #with dcFile as csvRow:
    #    row = csv.reader(csvRow, jsonConfig['fields'],delimiter=jsonConfig['delimiter']);
    #    print("%i %s %s" %(i,row['Date Of Stop'],row['Time Of Stop']));    
    #    i = i + 1;
    reader = csv.DictReader(dcFile, jsonConfig['fields'],delimiter=jsonConfig['delimiter']);
    attrarr = jsonConfig['fields'].copy();
    #attrarr = [];
    
    #Build vertical set for vertical cleaning
    vcleaning = jsonConfig['vcleaning'];
    
    vlist = {};
    for vset in vcleaning:
        vlist[vset['field']] = [];    
    
    i = 0;
    for row in reader:
        #print("%i %s %s" %(i,row['Date Of Stop'],row['Time Of Stop']));
    #    print(jsonConfig['hcleaning'][0]['VehicleType'][0]);
    #    print("%s" %(cmd.command(jsonConfig['hcleaning'][0]['VehicleType'][0])).value);            
        #Do Horizontal cleaning
        hcleaning = jsonConfig['hcleaning'];
        for hcvalue in hcleaning:
            field = hcvalue['field'];
            if not hcvalue['newField'] in attrarr:
                attrarr.append(hcvalue['newField']);
            #row[hcleaning[i]]['newField']] = row[field];
            dcField = dcvalue.DCValue(row[field]);
            dcCommand = DCCommand(dcField);
            for opvalue in hcvalue['operation']:
                dcCommand.command(opvalue,row);
            row[hcvalue['newField']] = dcField.value;
            #print(row[hcvalue['newField']]);
            #print(dcField.value);
        rowarr = [];
    #    for rowattr,rowval in row.items():
            #add header information
            #need to figur better way to do this
    #        if(i==0):
    #            attrarr.append(rowattr)
    #        rowarr.append(rowval);
        #second approach, use attrarr
        for attr in attrarr:
            rowarr.append(row[attr]);
        #print header
        if(i==0):
            writer.writerow(attrarr);
        #print row data
        writer.writerow(rowarr);
        #append vertical list
        for attr,value in vlist.items():
            value.append(row[attr]);            
        #add counter
        i = i + 1;
    
    #Do vertical cleaning (spliting etc)
    for vrow in vcleaning:
        field = vrow['field'];
        rownum = 0;
        for vcvalue in vlist[field]:
            #row[hcleaning[i]]['newField']] = row[field];
            dcField = dcvalue.DCValue(vcvalue);
            dcCommand = DCCommand(dcField);
            for opvalue in vrow['operation']:
                dcCommand.command(opvalue,vlist,rownum);
            rownum = rownum + 1;
            
    #print(vlist);
    dcFile.close();
    dcFile = open("test", 'r');
    next(dcFile);
    writer = csv.writer(open("testtwo", 'w'));
    reader = csv.DictReader(dcFile, attrarr, delimiter=jsonConfig['delimiter']);
    vattrarr = attrarr.copy();
    i = 0;
    for row in reader:
        for vattr in vlist.keys():        
            if not vattr in vattrarr:
                vattrarr.append(vattr);
            if not vattr in attrarr:
                #print("%s %s" %(vattr,i))
                row[vattr] = vlist[vattr][i];
        #make new array row
        rowarr = [];
        #print header
        if(i==0):
            writer.writerow(vattrarr);    
        for attr in vattrarr:
            rowarr.append(row[attr]);    
        writer.writerow(rowarr);
        i = i + 1;
    
    print('rows: %s' %(i));

def groupColumn(groupArr,row,groupResult):
    for group in groupArr:
        if not group in groupResult.keys():
            groupResult[group] = {};
        if not row[group] in groupResult[group].keys():
            groupResult[group][row[group]] = 0;
        groupResult[group][row[group]] = groupResult[group][row[group]] + 1;
    return groupResult;

def mergeColumn(groupArr,row,groupResult):
    for group in groupArr:
        if not group in groupResult.keys():
            groupResult[group] = {};
        if not row[group] in groupResult[group].keys():
            groupResult[group][row[group]] = 0;
        groupResult[group][row[group]] = groupResult[group][row[group]] + 1;
    return groupResult;

def openWriteFile(filename):
    writer = csv.writer(open(filename, 'w'));
    return writer;    

def openReadFile(filename,delimiter):
    dcFile = open(filename, 'r');
    headreader = csv.reader(dcFile);
    header = next(headreader);
    reader = csv.DictReader(dcFile, header,delimiter=delimiter);
    return { 'header': header, 'rows': reader};


def saveGroupColumn(groupResult):
#    print(groupResult);
    for groupattr,groupval in groupResult.items():
        writer = openWriteFile("%s.%s"%('test',groupattr));
        head = [groupattr,'count'];
        writer.writerow(head);
        for rowattr,rowval in groupval.items():
            row = [rowattr,rowval];
            writer.writerow(row);
            
def mergeGroupColumn(groupResult,filename):
#    print(groupResult);
    writer = openWriteFile(filename);
    head = [];
    for groupattr,groupval in groupResult.items():
        head.append(groupattr);
    #write header
    tempGroup = {};
    writer.writerow(head);
    max = 0;
    for field in head:
        tempGroup[field] = [];        
        i = 0;
        for rowattr,rowval in groupResult[field].items():
            tempGroup[field].append(rowattr);
            i = i +  1;
        if(i>max):
            max = i;
    #write group file
    i = 0
    while (i < max):
        tempRow = [];
        for field in head:
            if(i<len(tempGroup[field])):
                tempRow.append(tempGroup[field][i]);
            else:
                tempRow.append('');
        writer.writerow(tempRow);
        i = i + 1;


def selectColumn(row,selection,writer):
    rowarr = [];
    for select in selection:
        rowarr.append(row[select]);
    writer.writerow(rowarr);

def autoDefineColumn(headarr,field):
    i=1;
    while "%s %s" % (field,i) in headarr:        
        i = i + 1;
    return "%s %s" % (field,i);    

def regexReplace(row,field,newfield,pattern,replace,writer):
    dcField = dcvalue.DCValue(row[field]);
    dcField.regexReplace(pattern,replace);
    row[newfield]=dcField.value;
    writer.writerow(row); 

def trim(row,regarr,fields,newFields,writer):
    for field in fields:
        dcField = dcvalue.DCValue(row[field]);
        dcField.trim();
        row[newFields[field]] = dcField.value;
    tempRow = [];
    for field in regarr:
        tempRow.append(row[field]);
    writer.writerow(tempRow);

def dspace(row,regarr,fields,newFields,writer):
    for field in fields:
        dcField = dcvalue.DCValue(row[field]);
        dcField.collapseWhiteSpace();
        row[newFields[field]] = dcField.value;
    tempRow = [];
    for field in regarr:
        tempRow.append(row[field]);
    writer.writerow(tempRow);

def upper(row,regarr,fields,newFields,writer):
    for field in fields:
        dcField = dcvalue.DCValue(row[field]);
        dcField.toUpper();
        row[newFields[field]] = dcField.value;
    tempRow = [];
    for field in regarr:
        tempRow.append(row[field]);
    writer.writerow(tempRow);

def lower(row,regarr,fields,newFields,writer):
    for field in fields:
        dcField = dcvalue.DCValue(row[field]);
        dcField.toLower();
        row[newFields[field]] = dcField.value;
    tempRow = [];
    for field in regarr:
        tempRow.append(row[field]);
    writer.writerow(tempRow);

    
def main(argv):
    str = "";
    parser = argparse.ArgumentParser(description='Data Cleaning Framework v0.1')
#    parser.add_argument('integers', metavar='N', type=int, nargs='+',
#                       help='an integer for the accumulator')
#    parser.add_argument('-g','--group',
#                        help='group and count predefined fields');
#    parser.add_argument('-f','--file',
#                        help='file to clean');
#    parser.add_argument('-s','--select',
#                        help='select column into output');
#    parser.add_argument('-o','--outfile',
#                        help='file output from selecting columns');
    subparsers = parser.add_subparsers(help='sub-command help')
    # create the parser for the "hcleaning" command
    #group
    parser_group = subparsers.add_parser('group', help='group and count predefined fields, file output will be determined by column name')
    parser_group.add_argument('-in','--infile',
                        help='file to clean');        
    parser_group.add_argument('-f','--fields',
                        help='fields that one to be grouped');

    #merge
    parser_merge = subparsers.add_parser('merge', help='merge distincted fields value in one file, file can be used as input for cluster and merging operation on openrefine');
    parser_merge.add_argument('-in','--infile',
                        help='file to clean');        
    parser_merge.add_argument('-f','--fields',
                        help='fields that one to be grouped');
    parser_merge.add_argument('-out','--outfile',
                        help='output merge file');
                        
    #select
    parser_select = subparsers.add_parser('select', help='select particular fields into new output file')
    parser_select.add_argument('-in','--infile',
                        help='file to clean');
    parser_select.add_argument('-f','--fields',
                        help='selected fields that want to be separated');
    parser_select.add_argument('-out','--outfile', help='output file for select fields operation') 
    
    #regexreplace
    parser_regex = subparsers.add_parser('regexrep', help='replace pattern defined by regular expression to some value')
    parser_regex.add_argument('-in','--infile',
                        help='file to clean');        
    parser_regex.add_argument('-f','--field',
                        help='field to replace');
    parser_regex.add_argument('-p','--pattern',
                        help='pattern to be searched by regex');
    parser_regex.add_argument('-r','--replace',
                        help='replaced value');
    parser_regex.add_argument('-nf','--newfield',
                        help='new field for replaced value, otherwise it will automatically create new field');
    parser_regex.add_argument('-out','--outfile',
                        help='output file for replaced value');
    
    #trim
    parser_trim = subparsers.add_parser('trim', help='trim left and right particular fields, separate using comma')
    parser_trim.add_argument('-in','--infile',
                        help='file to clean');        
    parser_trim.add_argument('-f','--fields',
                        help='fields to clean, separate multi field using comma');
    parser_trim.add_argument('-out','--outfile',
                        help='output file for replaced value');
    
    #parser collapse space
    parser_colspace = subparsers.add_parser('colspace', help='colapse multiple white space for some fields')
    parser_colspace.add_argument('-in','--infile',
                        help='file to clean');        
    parser_colspace.add_argument('-f','--fields',
                        help='fields to clean, separate multi field using comma');
    parser_colspace.add_argument('-out','--outfile',
                        help='output file for replaced value');

    #parser toupper
    parser_upper = subparsers.add_parser('upper', help='upper case some fields')
    parser_upper.add_argument('-in','--infile',
                        help='file to clean');        
    parser_upper.add_argument('-f','--fields',
                        help='fields to clean, separate multi field using comma');
    parser_upper.add_argument('-out','--outfile',
                        help='output file for replaced value');
    
    #parser lower
    parser_lower = subparsers.add_parser('lower', help='lower case some fields')
    parser_lower.add_argument('-in','--infile',
                        help='file to clean');        
    parser_lower.add_argument('-f','--fields',
                        help='fields to clean, separate multi field using comma');
    parser_lower.add_argument('-out','--outfile',
                        help='output cleaned file');

    #read openrefine mass edit
    parser_massedit = subparsers.add_parser('massedit', help='Reading mass edit from openrefine config for merging operation')
    parser_massedit.add_argument('-in','--infile',
                        help='file to clean');        
    parser_massedit.add_argument('-c','--config',
                        help='open refine json config file');
    parser_massedit.add_argument('-nf','--newfield',
                        help='new cleaned field, if not define will be defiend automatically');
    parser_massedit.add_argument('-out','--outfile',
                        help='output cleaned file');
                        
    args = parser.parse_args(argv[1:]);
                
    if len(argv)>1:
        if argv[1]=='group':             
            args = parser_group.parse_args(argv[2:]);
            argobj = vars(args);
            if (argobj['infile'] is not None) :
                readfile = openReadFile(argobj['infile'],',');
                #initialization for group
                if (argobj['fields'] is not None) :
                    groupArr = argobj['fields'].split(',');
                    groupResult = {};
                    for row in readfile['rows']:                
                        #group column
                        groupColumn(groupArr,row,groupResult);
                    saveGroupColumn(groupResult);
                else:
                    print('fields not defined, you must define fields to be grouped using -f [fields]');
            else:
                print('infile not defined, you must define fields to be grouped using -in [file_name]');
                
        if argv[1]=='merge':             
            args = parser_merge.parse_args(argv[2:]);
            argobj = vars(args);
            if (argobj['infile'] is not None) :
                readfile = openReadFile(argobj['infile'],',');
                #initialization for group
                if (argobj['fields'] is not None) :
                    if (argobj['outfile'] is not None) :                    
                        groupArr = argobj['fields'].split(',');
                        groupResult = {};
                        for row in readfile['rows']:                
                            #group column
                            groupColumn(groupArr,row,groupResult);
                        mergeGroupColumn(groupResult,argobj['outfile']);
                    else:
                        print("you must define output file with -out [select_outfile]");                        
                else:
                    print('fields not defined, you must define fields to be grouped using -f [fields]');
            else:
                print('infile not defined, you must define fields to be grouped using -in [file_name]');
        
        if argv[1]=='select':
            args = parser_select.parse_args(argv[2:]);
            argobj = vars(args);            
            if (argobj['infile'] is not None) :
                readfile = openReadFile(argobj['infile'],',');            
                #initialization for select
                if (argobj['fields'] is not None) :
                    if (argobj['outfile'] is not None) :
                        selectArr = argobj['fields'].split(',');
                        # make new output file
                        selectwriter = openWriteFile(argobj['outfile']);
                        #print header
                        selectwriter.writerow(selectArr);
                        for row in readfile['rows']:                
                            #group column                        
                            selectColumn(row,selectArr,selectwriter);                        
                    else:
                        print("you must define output file with -out [select_outfile]");
                else:
                    print("you must define fields to be selected using -f [fields]");
            else:
                print('infile not defined, you must define fields to be grouped using -in [file_name]');
                                
        if argv[1]=='regexrep':
            args = parser_regex.parse_args(argv[2:]);
            argobj = vars(args);            
            if (argobj['infile'] is not None) :
                readfile = openReadFile(argobj['infile'],',');            
                #initialization for regular expression cleaning 
                if (argobj['pattern'] is not None) :
                    if (argobj['replace'] is not None) :
                        if (argobj['field'] is not None) :
                            if (argobj['outfile'] is not None) :
                                regArr = readfile['header'].copy();
                                #make output file                          
                                regWriter = openWriteFile(argobj['outfile']);
                                #define new column
                                newColumn = autoDefineColumn(regarr,argobj['regfield']);
                                regArr.append(newColumn);
                                #write new header file
                                regWriter.writerow(regArr);
                                #copy row so it won't affect another operation
                                for row in readfile['rows']:                
                                    #replace using horizontal cleaning                                   
                                    regexReplace(row,argobj['pattern'],newColumn,argobj['field'],argobj['replace'],regWriter);
                            else:
                                print("you must output file for regexrep operation with -out [output file name]");                                
                        else:
                            print("you must define field to replace with -f [field name]");
                    else:
                        print("you must define output replace value with -r [replace value]");                                  
                else:
                    print("you must define pattern to replace using -p [regex pattern]");
            else:
                print('infile not defined, you must define fields to be grouped using -in [file_name]');                    
                
        #trim command        
        if argv[1]=='trim':
            args = parser_trim.parse_args(argv[2:]);
            argobj = vars(args);            
            if (argobj['infile'] is not None) :
                readfile = openReadFile(argobj['infile'],',');            
                #initialization for trim
                if (argobj['fields'] is not None) :
                    if (argobj['outfile'] is not None) :
                        regArr = readfile['header'].copy();                                
                        commonWriter = openWriteFile(argobj['outfile']);
                        #define new column
                        fields = argobj['fields'].split(',');
                        newField = {};
                        for field in fields:
                            newField[field] = autoDefineColumn(regArr,field);
                            regArr.append(newField[field]);                            
                        #write new header file
                        commonWriter.writerow(regArr);                                                
                        for row in readfile['rows']:                
                            #trim fields 
                            trim(row,regArr,fields,newField,commonWriter);                                                    
                    else:
                        print("you must define output file with -out [select_outfile]");
                else:
                    print("you must define fields to be trimed using -f [fields]");
            else:
                print('infile not defined, you must define fields to be trimed using -in [file_name]');

        #collapse white space
        if argv[1]=='colspace':
            args = parser_colspace.parse_args(argv[2:]);
            argobj = vars(args);            
            if (argobj['infile'] is not None) :
                readfile = openReadFile(argobj['infile'],',');            
                #initialization for trim
                if (argobj['fields'] is not None) :
                    if (argobj['outfile'] is not None) :
                        regArr = readfile['header'].copy();                                
                        commonWriter = openWriteFile(argobj['outfile']);
                        #define new column
                        fields = argobj['fields'].split(',');
                        newField = {};
                        for field in fields:
                            newField[field] = autoDefineColumn(regArr,field);
                            regArr.append(newField[field]);                            
                        #write new header file
                        commonWriter.writerow(regArr);                                                
                        for row in readfile['rows']:                
                            #collapse white space fields
                            dspace(row,regArr,fields,newField,commonWriter);                        
                    else:
                        print("you must define output file with -out [select_outfile]");
                else:
                    print("you must define fields to be cleaned using -f [fields]");
            else:
                print('infile not defined, you must define fields to be cleaned using -in [file_name]');
        
        #uper
        if argv[1]=='upper':
            args = parser_upper.parse_args(argv[2:]);
            argobj = vars(args);            
            if (argobj['infile'] is not None) :
                readfile = openReadFile(argobj['infile'],',');            
                #initialization for trim
                if (argobj['fields'] is not None) :
                    if (argobj['outfile'] is not None) :
                        regArr = readfile['header'].copy();                                
                        commonWriter = openWriteFile(argobj['outfile']);
                        #define new column
                        fields = argobj['fields'].split(',');
                        newField = {};
                        for field in fields:
                            newField[field] = autoDefineColumn(regArr,field);
                            regArr.append(newField[field]);                            
                        #write new header file
                        commonWriter.writerow(regArr);                                                
                        for row in readfile['rows']:                
                            #collapse white space fields
                            upper(row,regArr,fields,newField,commonWriter);                                                                    
                    else:
                        print("you must define output file with -out [select_outfile]");
                else:
                    print("you must define fields to be cleaned using -f [fields]");
            else:
                print('infile not defined, you must define fields to be cleaned using -in [file_name]');
            
        #lower
        if argv[1]=='lower':
            args = parser_lower.parse_args(argv[2:]);
            argobj = vars(args);            
            if (argobj['infile'] is not None) :
                readfile = openReadFile(argobj['infile'],',');            
                #initialization for trim
                if (argobj['fields'] is not None) :
                    if (argobj['outfile'] is not None) :
                        regArr = readfile['header'].copy();                                
                        commonWriter = openWriteFile(argobj['outfile']);
                        #define new column
                        fields = argobj['fields'].split(',');
                        newField = {};
                        for field in fields:
                            newField[field] = autoDefineColumn(regArr,field);
                            regArr.append(newField[field]);                            
                        #write new header file
                        commonWriter.writerow(regArr);                                                
                        for row in readfile['rows']:                
                            #collapse white space fields
                            lower(row,regArr,fields,newField,commonWriter);                                                                    
                    else:
                        print("you must define output file with -out [select_outfile]");
                else:
                    print("you must define fields to be cleaned using -f [fields]");
            else:
                print('infile not defined, you must define fields to be cleaned using -in [file_name]');
        
        #lower
        if argv[1]=='massedit':
            args = parser_massedit.parse_args(argv[2:]);
            argobj = vars(args);            
            if (argobj['infile'] is not None) :
                readfile = openReadFile(argobj['infile'],',');
                header = readfile['header'].copy();
                #initialization for trim
                if (argobj['config'] is not None) :
                    if (argobj['outfile'] is not None) :
                        masseditWriter = openWriteFile(argobj['outfile']);
                        
                        #read json config file
                        with open(argobj['config']) as json_file:
                            jsonConfig = json.load(json_file)
                        
                        if(argobj['newfield'] is not None):
                            newfields = argobj['newfield'].split(',');
                            if(len(newfields)!=len(jsonConfig)):
                                print("fields defined must be the same as config length, config: %s vs newfield: %s" %(len(jsonConfig),len(newfields)));
                                return;
                            
                        j = 0;
                        #field to replace
                        for config in jsonConfig:
                            #mass edit only
                            if(config['op']=="core/mass-edit"):
                                field = config['columnName'];
                                if(argobj['newfield'] is None):
                                    newfield = autoDefineColumn(header,field);
                                else:
                                    newfield = newfields[j];
                                header.append(newfield);
                                masseditWriter.writerow(header);
                                #start fetching
                                for row in readfile['rows']:
                                    dcField = dcvalue.DCValue(row[field]);                                    
                                    #fetch massedit config
                                    for edit in config['edits']:
                                        #pattern must exactly match
                                        pattern = '^('+edit['from'][0];
                                        # add aditional pattern
                                        if(len(edit['from'])>1):
                                            i = 1;
                                            while (i<len(edit['from'])):
                                                pattern = pattern + '|' + edit['from'][i];
                                                i = i + 1;                                            
                                        # close pattern
                                        pattern = pattern +')$';
                                        replace = edit['to'];
                                        dcField.regexReplace(pattern,replace);
                                        row[newfield]=dcField.value;
                                    
                                    temprow = [];
                                    for tempField in header:
                                        temprow.append(row[tempField]);
                                    masseditWriter.writerow(temprow);
                            j = j + 1;
                    else:
                        print("you must define output file with -out [select_outfile]");
                else:
                    print("you must define fields to be cleaned using -f [fields]");
            else:
                print('infile not defined, you must define fields to be cleaned using -in [file_name]');

#    print(vars(args));
#    print(parser_a.parse_args(argv[1:]));
#    print(args.accumulate(args.integers))

if __name__ == "__main__":
    main(sys.argv)


    
