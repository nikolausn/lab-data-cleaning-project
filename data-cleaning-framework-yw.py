#!/usr/local/bin/python
import re;
import csv;
import json;
import dcvalue;
import argparse;
from optparse import OptionParser;
import sys;
import sqlite3;
import os;
import time;

class DCCommand:    
    def __init__(self,dcValue):
        self.dcValue = dcValue;
    
    def command(self,cmdObject,row=None,rownum=None,field=None,rowid=None,logStream=None):
        tempValue = self.dcValue.value;
        
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
                fname = cmdObject['file']+'.'+cmdObject['module'];
                module = __import__(cmdObject['file']);
                customFunction = getattr(module, cmdObject['module']);
                #customFunction(self.dcValue.value,'Color',row);
                self.dcValue.customFunction(customFunction,field,row,rownum);
        
        # print provenance log
        if(rowid is not None and logStream is not None and tempValue!=self.dcValue.value):
            tempRow = [rowid,field,time.strftime("%d/%m/%y %H:%M:%S"),fname,tempValue,self.dcValue.value];
            logStream.writerow(tempRow);
             
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

def runConfig(infile,outfile,dcConfig):
    #make log stream for provenance
    logstream = openWriteFile(outfile+'.'+time.strftime("%y%m%d%H%M%S")+'.log');
    
    logstream.writerow(['id','field','timestamp','function','oldval','newval']);
                  
    #read jsonconfig file
    jsonConfig = {};
    with open(dcConfig) as json_file:
        jsonConfig = json.load(json_file)
    
    #start data cleaning workflow
    #change fields array into tuple
    #fieldName = tuple(jsonConfig['fields']);
    #read csv file base on fields definition
    #open file
    writer = csv.writer(open(outfile, 'w'))
    dcFile = open(infile, 'r');
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
                dcCommand.command(opvalue,row,field=field,rowid=i+1,logStream=logstream);
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
    isVertical = False;
    for vrow in vcleaning:
        field = vrow['field'];
        rownum = 0;
        for vcvalue in vlist[field]:
            #row[hcleaning[i]]['newField']] = row[field];
            isVertical = True;
            dcField = dcvalue.DCValue(vcvalue);
            dcCommand = DCCommand(dcField);
            for opvalue in vrow['operation']:
                dcCommand.command(opvalue,vlist,rownum,field,rowid=rownum+1,logStream=logstream);
            rownum = rownum + 1;
            
    #print(vlist);
    dcFile.close();
    
    if isVertical:
        #rename output file to temporary
        os.rename(outfile, outfile+'.temp');    
        dcFile = open(outfile+'.temp', 'r');
        next(dcFile);
        writer = csv.writer(open(outfile, 'w'));
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
        os.remove(outfile+'.temp');
    
#    print('rows: %s' %(i));

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


def saveGroupColumn(groupResult,prefix):
#    print(groupResult);
    for groupattr,groupval in groupResult.items():
        writer = openWriteFile("%s.%s"%(prefix,groupattr));
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

def regexReplace(row,field,newfield,pattern,replace,writer,logstream=None,rowid=None):
    dcField = dcvalue.DCValue(row[field]);
    dcCommand = DCCommand(dcField);
    command = {
					"fname": "regexReplace",
					"fparam": {
						"regex": pattern,
						"replace": replace
					}
				};
    dcCommand.command(command,row,field=field,rowid=rowid,logStream=logstream);    
#    dcField.regexReplace(pattern,replace);
    row[newfield]=dcField.value;
    writer.writerow(row);    

def trim(row,regarr,fields,newFields,writer,logstream=None,rowid=None):
    for field in fields:
        dcField = dcvalue.DCValue(row[field]);
        dcCommand = DCCommand(dcField);        
        command = {
    					"fname": "trim",
    				};
        dcCommand.command(command,row,field=field,rowid=rowid,logStream=logstream);            
        #dcField.trim();
        row[newFields[field]] = dcField.value;
    tempRow = [];
    for field in regarr:
        tempRow.append(row[field]);
    writer.writerow(tempRow);

def dspace(row,regarr,fields,newFields,writer,logstream=None,rowid=None):
    for field in fields:
        dcField = dcvalue.DCValue(row[field]);
        dcCommand = DCCommand(dcField);        
        command = {
    					"fname": "collapseWhiteSpace",
    				};
        dcCommand.command(command,row,field=field,rowid=rowid,logStream=logstream);
        #dcField.collapseWhiteSpace();
        row[newFields[field]] = dcField.value;
    tempRow = [];
    for field in regarr:
        tempRow.append(row[field]);
    writer.writerow(tempRow);

def upper(row,regarr,fields,newFields,writer,logstream=None,rowid=None):
    for field in fields:
        dcField = dcvalue.DCValue(row[field]);
        dcCommand = DCCommand(dcField);        
        command = {
    					"fname": "toUpper",
    				};
        dcCommand.command(command,row,field=field,rowid=rowid,logStream=logstream);
        #dcField.toUpper();
        row[newFields[field]] = dcField.value;
    tempRow = [];
    for field in regarr:
        tempRow.append(row[field]);
    writer.writerow(tempRow);

def lower(row,regarr,fields,newFields,writer,logstream=None,rowid=None):
    for field in fields:
        dcField = dcvalue.DCValue(row[field]);
        dcCommand = DCCommand(dcField);        
        command = {
    					"fname": "toLower",
    				};
        dcCommand.command(command,row,field=field,rowid=rowid,logStream=logstream);
        #dcField.toLower();
        row[newFields[field]] = dcField.value;
    tempRow = [];
    for field in regarr:
        tempRow.append(row[field]);
    writer.writerow(tempRow);

"""
@begin MainDCFramework @desc main workflow for datacleaning framework
@in input_file @desc input file
@in init_config_file
@in open_refine_config_file
@param arguments
@out output_file @desc output file
@out database_file @desc cleaned_database_file
@out aggregated_file @desc aggregated_file from group operation
"""
def main(argv):
    str = "";
    """
    @begin initialize_arg_parser @desc Initialize parser for main program
    @param parserConfig as parserArr
    @out parsers
    """
    parser = argparse.ArgumentParser(description='Data Cleaning Framework v0.1')
    
    # rowid for log provenance
    parser.add_argument('-rid','--rowid',help='rowid / id if the csv input file contains rowid for log provenance');
    
    
    subparsers = parser.add_subparsers(help='sub-command help')

    commonIn = {
                'param': '-in',
                'longparam': '--infile',
                'help': 'input file to be cleaned',
                'required': True,
                'message': 'infile is not defined, you must define file input -in [file_name]'
            };
    commonOut = {
                'param': '-out',
                'longparam': '--outfile',
                'help': 'output cleaned file',
                'required': True,
                'message': 'you must define output file with -out [select_outfile]'
            };
    commonFields = {
                'param': '-f',
                'longparam': '--fields',
                'help': 'field to be cleaned',
                'required': True,
                'message': 'fields not defined, you must define fields to be grouped using -f [fields]'
            };
            
    parserArr = {
        'id': {
            'help': 'gives id for a particular csv file',
            'argument': [ 
                commonIn, 
                commonOut,
                {
                    'param': '-s',
                    'longparam': '--startid',
                    'help': 'starting id, if it''s not stated then will start at 1',
                    'required': False
                },
                {
                    'param': '-f',
                    'longparam': '--field',
                    'help': 'field name for id, if it''s not stated then the field name will be ID',
                    'required': False
                }
            ]
        },
        'init': {
            'help': 'init cleaning tasks using config file',
            'argument': [
                commonIn,
                commonOut,
                {
                    'param': '-c',
                    'longparam': '--config',
                    'help': 'config file for cleaning',
                    'required': True,
                    'message': 'config file not defined, you must define config file using -c [config file]'
                }            
            ]
        },
        'group': {
            'help': 'group and count predefined fields, file output will be determined by column name',
            'argument': [
                commonIn,
                commonFields,
                {
                    'param': '-out',
                    'longparam': '--outfile',
                    'help': 'outfile prefix will be followed by field name to be grouped',
                    'required': True,
                    'message': 'Must define prefix for output grouped files'
                }
            ]
        },
        'merge': {
            'help': 'merge distincted fields value in one file, file can be used as input for cluster and merging operation on openrefine',
            'argument': [
                commonIn,
                commonOut,
                commonFields
            ]
        },
        'select': {
            'help': 'select particular fields into new output file',
            'argument': [
                commonIn,
                commonOut,
                commonFields
            ]
        },
        'regexrep': {
            'help': 'replace pattern defined by regular expression to some value',
            'argument': [
                commonIn,
                commonOut,
                {
                    'param': '-f',
                    'longparam': '--field',
                    'help': 'field to be cleaned',
                    'required': True,
                    'message': 'you must define field to replace with -f [field name]'
                },
                {
                    'param': '-p',
                    'longparam': '--pattern',
                    'help': 'pattern to be searched by regex',
                    'required': True,
                    'message': 'you must define pattern to replace using -p [regex pattern]'
                },
                {
                    'param': '-r',
                    'longparam': '--replace',
                    'help': 'replaced value',
                    'required': True,
                    'message': 'you must define output replace value with -r [replace value]'
                },
                {
                    'param': '-nf',
                    'longparam': '--newfield',
                    'help': 'new field for replaced value, otherwise it will automatically create new field',
                    'required': False
                }
            ]
        },
        'trim': {
            'help': 'trim left and right particular fields, separate using comma',
            'argument': [
                commonIn,
                commonOut,
                commonFields
            ]
        },
        'colspace': {
            'help': 'colapse multiple white space for some fields',
            'argument': [
                commonIn,
                commonOut,
                commonFields
            ]
        },
        'upper': {
            'help':'upper case some fields',
            'argument': [
                commonIn,
                commonOut,
                commonFields
            ]
        },
        'lower': {
            'help': 'lower case some fields',
            'argument': [
                commonIn,
                commonOut,
                commonFields
            ]
        },
        'massedit': {
            'help': 'Reading mass edit from openrefine config for merging operation',
            'argument': [
                commonIn,
                commonOut,
                {
                    'param': '-c',
                    'longparam': '--config',
                    'help': 'open refine json config file',
                    'required': True,
                    'message': 'you must define config file from openrefine using -c [config]'
                },
                {
                    'param': '-nf',
                    'longparam': '--newfield',
                    'help': 'new field for replaced value, otherwise it will automatically create new field',
                    'required': False
                }
            ]
        },
        'loadtable': {
            'help': 'Load csv file into sqlite database',
            'argument': [
                commonIn,
                commonOut,
                {
                    'param': '-t',
                    'longparam': '--tname',
                    'help': 'table name for loaded file',
                    'required': True,
                    'message': 'you must define table destination name with -t [table name]'
                },
                {
                    'param': '-a',
                    'longparam': '--addition',
                    'help': 'if present, command will not create new table',
                    'required': False
                }
            ]
        },
        'runquery': {
            'help': 'Run Query to sqlite database defined -d --database',
            'argument': [
                {
                    'param': '-in',
                    'longparam': '--infile',
                    'help': 'file that contains sql query',
                    'required': True,
                    'message': 'sqlfile not defined, you must define fields to be cleaned using -in [sqlfile]'
                },
                {
                    'param': '-d',
                    'longparam': '--database',
                    'help': 'sqlite3 database file',
                    'required': True,
                    'message': 'you must define database file using -d [database file]'
                },
                {
                    'param': '-out',
                    'longparam': '--outfile',
                    'help': 'if present, result of query will be present in csv file',
                    'required': False
                }
            ]
        }
    }

    #init parser
    for myparskey,myparsval in parserArr.items():
        myparsval['parser'] = subparsers.add_parser(myparskey,help = myparsval['help']);
        for myargval in myparsval['argument']:
            myparsval['parser'].add_argument(myargval['param'],myargval['longparam'],help=myargval['help']);
    
    """
    @end initialize_arg_parser
    """

    """
    @begin parse_args
    @in parsers
    @in arguments
    @out parsed_args
    """
    args = parser.parse_args(argv[1:]); 
    argobj = vars(args)
    rowid = argobj['rowid'];
    """
    @end parse_args
    """
    
    
    """
    @begin ExecuteCommand @desc parse command and execute based on arguments
    @in parsed_args
    @out id
    @out init
    @out select
    @out merge
    @out group
    @out massedit
    @out loadtable
    """
    if len(argv)>1:
        args = parserArr[argv[1]]['parser'].parse_args(argv[2:]);
        argobj = vars(args);
        
        comm = argv[1];
        if(comm not in parserArr.keys()):
            print("%s command is not allowed" %(comm));
            return;
        
        
        #check required command
        myparser = parserArr[comm];
        for myarg in myparser['argument']:
            if(myarg['required'] and argobj[myarg['longparam'].replace('--','')] is None):
                print(myarg['message']);
                return;                
        """
        @end ExecuteCommand
        """
        
        """
        @begin AddId @desc Add Rowid from input file and output it to the new file
        @param id
        @in input_file @uri file:{input_file}
        @out output_file @uri file:{output_file}
        """
        if argv[1]=='id':             
            readfile = openReadFile(argobj['infile'],',');    
            idwriter = openWriteFile(argobj['outfile']);                                 
            start = 1;
            field = 'ID';
            if(argobj['startid'] is not None):
                start = argobj['startid'];
            if(argobj['field'] is not None):
                field = argobj['field'];
            header = readfile['header'];
            newheader = [field];
            newheader.extend(header);
            idwriter.writerow(newheader);
            for row in readfile['rows']:                
                #group column                
                row[field] = start;
                temparr = [];
                for tempfield in newheader:
                    temparr.append(row[tempfield]);
                idwriter.writerow(temparr);
                start = start + 1;                                        

        """
        @end AddId
        """
        """
        @begin InitClean @desc Perform initialization cleanup using config file
        @param init
        @in input_file
        @in init_config_file @uri file:{init_config_file}
        @out output_file @uri file:{output_file}
        """
        if argv[1]=='init':             
            runConfig(argobj['infile'],argobj['outfile'] ,argobj['config'] );
        """
        @end InitClean
        """
        
        """
        @begin AggregateFields @desc Aggregate and Count values in selected fields
        @param group
        @in input_file
        @out aggregated_file @uri file:{prefix}.{field_name}
        """
        
        if argv[1]=='group':             
            readfile = openReadFile(argobj['infile'],',');
            groupArr = argobj['fields'].split(',');
            groupResult = {};
            for row in readfile['rows']:                
                #group column
                groupColumn(groupArr,row,groupResult);
            saveGroupColumn(groupResult,argobj['outfile']);
        """
        @end AggregateFields
        """

        """
        @begin MergeField @desc merge selected distincted field for opern refine merging purpose
        @param merge
        @in input_file
        @out output_file
        """
        if argv[1]=='merge':             
            readfile = openReadFile(argobj['infile'],',');
            #initialization for group
            groupArr = argobj['fields'].split(',');
            groupResult = {};
            for row in readfile['rows']:                
                #group column
                groupColumn(groupArr,row,groupResult);
            mergeGroupColumn(groupResult,argobj['outfile']);
        """
        @end MergeField 
        """

        """
        @begin SelectField @desc select fields and output the field to new output file
        @param select
        @in input_file
        @out output_file
        """
        if argv[1]=='select':
            readfile = openReadFile(argobj['infile'],',');            
            #initialization for select
            selectArr = argobj['fields'].split(',');
            # make new output file
            selectwriter = openWriteFile(argobj['outfile']);
            #print header
            selectwriter.writerow(selectArr);
            for row in readfile['rows']:                
                #group column                        
                selectColumn(row,selectArr,selectwriter);
        """
        @end SelectField
        """

        #Common Transform                
        commontransform = ['regexrep','trim','colspace','upper','lower'];
        if argv[1] in commontransform:        
            #create logstream for provenance checking
            logstream = openWriteFile(argobj['outfile']+'.'+time.strftime("%y%m%d%H%M%S")+'.log');    
            logstream.writerow(['id','field','timestamp','function','oldval','newval']);
            
            readfile = openReadFile(argobj['infile'],',');
            #initialization for regular expression cleaning 
            regArr = readfile['header'].copy();
            #make output file                          
            commonWriter = openWriteFile(argobj['outfile']);
            #define new column
            if ('field' in argobj.keys()):
                newColumn = autoDefineColumn(regArr,argobj['field']);
                regArr.append(newColumn);
            elif ('fields' in argobj.keys()):                
                fields = argobj['fields'].split(',');                
                newField = {};
                for field in fields:
                    newField[field] = autoDefineColumn(regArr,field);
                    regArr.append(newField[field]);                                            
                
            commonWriter.writerow(regArr);
            i = 1;
            for row in readfile['rows']:
                if rowid is not None :
                    myid = row[rowid];
                else:
                    myid = i;
                    
                if argv[1]=='regexrep':
                        #readfile = openReadFile(argobj['infile'],',');            
                        #initialization for regular expression cleaning 
                        #regArr = readfile['header'].copy();
                        #make output file                          
                        #regWriter = openWriteFile(argobj['outfile']);
                        #define new column
                        #newColumn = autoDefineColumn(regArr,argobj['field']);
                        #regArr.append(newColumn);
                        #write new header file
                        #regWriter.writerow(regArr);
                        #copy row so it won't affect another operation
                        #for row in readfile['rows']:
                            #replace using horizontal cleaning                                   
                            regexReplace(row,argobj['pattern'],newColumn,argobj['field'],argobj['replace'],commonWriter,logstream,myid);
                
                #trim command        
                if argv[1]=='trim':
                        #readfile = openReadFile(argobj['infile'],',');            
                        #initialization for trim
                        #regArr = readfile['header'].copy();                                
                        #commonWriter = openWriteFile(argobj['outfile']);
                        #define new column
                        #fields = argobj['fields'].split(',');
                        #newField = {};
                        #for field in fields:
                        #    newField[field] = autoDefineColumn(regArr,field);
                        #    regArr.append(newField[field]);                            
                        #write new header file
                        #commonWriter.writerow(regArr);                                                
                        #for row in readfile['rows']:                
                            #trim fields 
                            trim(row,regArr,fields,newField,commonWriter,logstream,myid);
                
                #collapse white space
                if argv[1]=='colspace':
                        #readfile = openReadFile(argobj['infile'],',');            
                        #initialization for trim
                        #regArr = readfile['header'].copy();                                
                        #commonWriter = openWriteFile(argobj['outfile']);
                        #define new column
                        #fields = argobj['fields'].split(',');
                        #newField = {};
                        #for field in fields:
                        #    newField[field] = autoDefineColumn(regArr,field);
                        #    regArr.append(newField[field]);                            
                        #write new header file
                        #commonWriter.writerow(regArr);                                                
                        #for row in readfile['rows']:                
                            #collapse white space fields
                            dspace(row,regArr,fields,newField,commonWriter,logstream,myid);                        
                
                #uper
                if argv[1]=='upper':
                        #readfile = openReadFile(argobj['infile'],',');            
                        #initialization for trim
                        #regArr = readfile['header'].copy();                                
                        #commonWriter = openWriteFile(argobj['outfile']);
                        #define new column
                        #fields = argobj['fields'].split(',');
                        #newField = {};
                        #for field in fields:
                        #    newField[field] = autoDefineColumn(regArr,field);
                        #    regArr.append(newField[field]);                            
                        #write new header file
                        #commonWriter.writerow(regArr);                                                
                        #for row in readfile['rows']:                
                            #collapse white space fields
                            upper(row,regArr,fields,newField,commonWriter,logstream,myid);                                                                    
                
                #lower
                if argv[1]=='lower':
                        #readfile = openReadFile(argobj['infile'],',');            
                        #initialization for trim
                        #regArr = readfile['header'].copy();                                
                        #commonWriter = openWriteFile(argobj['outfile']);
                        #define new column
                        #fields = argobj['fields'].split(',');
                        #newField = {};
                        #for field in fields:
                        #    newField[field] = autoDefineColumn(regArr,field);
                        #    regArr.append(newField[field]);                            
                        #write new header file
                        #commonWriter.writerow(regArr);                                                
                        #for row in readfile['rows']:                
                            #collapse white space fields
                            lower(row,regArr,fields,newField,commonWriter,logstream,myid);
                i = i + 1;
    
        #massedit
        """
        @begin MassEdit
        @in input_file
        @param massedit
        @in open_refine_config_file @uri file:{open_refine_config_file}
        @out output_file @uri file:{output_file}
        @out log_file @uri file:{outfile_name}.{timestamp}.log
        """
        if argv[1]=='massedit':
            #create logstream for provenance checking
            logStream = openWriteFile(argobj['outfile']+'.'+time.strftime("%y%m%d%H%M%S")+'.log');    
            logStream.writerow(['id','field','timestamp','function','oldval','newval']);
            
            readfile = openReadFile(argobj['infile'],',');
            header = readfile['header'].copy();
            #initialization for trim
            masseditWriter = openWriteFile(argobj['outfile']);
            
            #read json config file
            with open(argobj['config']) as json_file:
                jsonConfig = json.load(json_file)
            
            if(argobj['newfield'] is not None):
                newfields = argobj['newfield'].split(',');
                if(len(newfields)!=len(jsonConfig)):
                    print("fields defined must be the same as config length, config: %s vs newfield: %s" %(len(jsonConfig),len(newfields)));
                    return;
                
            #field to replace
            tempValue = {};
            i = 0;
            for row in readfile['rows']:                
                j = 0;
                if rowid is not None :
                    myid = row[rowid];
                else:
                    myid = i+1;
                    
                for config in jsonConfig:
                    #mass edit only
                    if(config['op']=="core/mass-edit"):
                        field = config['columnName'];
                        if not field in tempValue.keys():                                                       
                            if(argobj['newfield'] is None):
                                newfield = autoDefineColumn(header,field);
                            else:
                                newfield = newfields[j];
                            #store newfield value
                            tempValue[field] = newfield;
                            header.append(newfield);
                            #row[newfield] = row[field];
                            value = row[field];
                        else:
                            newfield = tempValue[field];                                
                            #row[newfield] = row[field];
                            if newfield in row.keys():
                                value = row[newfield];
                            else:
                                value = row[field];
                            field = tempValue[field];
                            
                        #start fetching                   
                        #print('field: %s' %(field));
                        #dcField = dcvalue.DCValue(row[field]);
                        #fetch massedit config
                        #row[newfield]=row[field];                            
                        for edit in config['edits']:
                            #text must be the same
                            if value in edit['from']:
                                row[newfield]=edit['to'];
                                if(value!=edit['to']):
                                    tempRow = [myid,field,time.strftime("%d/%m/%y %H:%M:%S"),'massedit',value,edit['to']];
                                    logStream.writerow(tempRow);                                    
                                value = row[newfield];
                            else:
                                row[newfield]=value;                                
                    j = j + 1;
                # write header
                if i==0:
                    masseditWriter.writerow(header);
                # write row
                temprow = [];
                for tempField in header:
                    temprow.append(row[tempField]);
                masseditWriter.writerow(temprow);
                i = i + 1;
        """
        @end MassEdit
        """

        #loadtable
        """
        @begin LoadTable @desc load csv file into database
        @param loadtable
        @in input_file
        @out database_file @uri file:{database_file}.db
        """
        if argv[1]=='loadtable':
                    tablename = argobj['tname'];                           
                    readfile = openReadFile(argobj['infile'],',');
                    header = readfile['header'].copy();
                    #initialization for trim
                        #open connection
                    conn = sqlite3.connect(argobj['outfile']);
                    #define cursor
                    c = conn.cursor()
                    createquery = "CREATE TABLE '"+tablename+"' (";
                    insertquery = "INSERT INTO '"+tablename+"' VALUES (";
                    for pos,item in enumerate(header):
                        # Replace whitespaces with underscores in the column names. It is cumbersome to write SQL with whitespaces in column names.
                        columnName = '_'.join(item.split())
                        createquery = createquery + "'" + columnName + "'";
                        insertquery = insertquery + "?";
                        # add comma for before last item
                        if(pos<len(header)-1):
                            createquery = createquery + ",";
                            insertquery = insertquery + ",";
                    createquery = createquery + ")";
                    insertquery = insertquery + ")";
                    print(createquery);
                    print(insertquery);
                    if(argobj['addition'] is None):
                        c.execute(createquery);
                    #start fetching
                    for row in readfile['rows']:                                                        
                        temprow = [];
                        for tempField in header:
                            temprow.append(row[tempField]);
                        c.execute(insertquery,temprow);
                    conn.commit();  
        """
        @end LoadTable
        """

        #runquery
        if argv[1]=='runquery':
                    #initialization for trim
                    outfile = False;
                    if (argobj['outfile'] is not None) :
                        #open connection
                        outfile = True;
                        querywriter = openWriteFile(argobj['outfile']);

                    conn = sqlite3.connect(argobj['database']);
                    #define cursor
                    csql = conn.cursor();
                                                            
                    with open(argobj['infile']) as f:
                        endfile = False;
                        while not endfile:
                            query = "";
                            c = f.read(1);
                            if not c:
                                endfile = True;
                                break;                          
                            query = query + c;
                            endquery = False;
                            while not endquery and not endfile:
                                c = f.read(1);
                                if not c:
                                    endfile = True;
                                    endquery = True;
                                if c==';':
                                    endquery = True;
                                query = query + c;
                            if(outfile):
                                querywriter.writerow(["-------BEGIN EXECUTE QUERY-------"]);
                                querywriter.writerow([query]); 
                                querywriter.writerow(["-------RESULT EXECUTE QUERY-------"]);
                            else:
                                print("-------BEGIN EXECUTE QUERY-------");
                                print(query);
                                print("-------RESULT EXECUTE QUERY-------");
                            result = csql.execute(query);
                            if(csql.description is not None):
                                names = list(map(lambda x: x[0], csql.description))
                                if(outfile):
                                    querywriter.writerow(names);
                                else:
                                    print(",".join(names));
                                for row in result:
                                    if(outfile):
                                        querywriter.writerow(row);
                                    else:
                                        print(row);
                            if(outfile):
                                querywriter.writerow(["-------END EXECUTE QUERY-------"]);
                            else:
                                print("-------END EXECUTE QUERY-------");
        

"""
@end MainDCFramework
"""


if __name__ == "__main__":
    main(sys.argv)


    
