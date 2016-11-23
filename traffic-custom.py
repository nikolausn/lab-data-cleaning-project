#!/usr/local/bin/python
import dcvalue;
import re;

def testFunction(value,field,row):
    return value;

def vehicleSplit(value,field,row,rownum):
    #value: DCValue
    #initialize new column
    
    vehicle = value.value;
    position = re.search('-',vehicle);
    vehicles = []
    if(position is not None):
        start = position.start();
        vehicle1 = vehicle[0:start-1];
        vehicles.append(vehicle1);
        if(start<len(vehicle)-1):
            vehicle2 = vehicle[start+1:len(vehicle)-1];
            vehicles.append(vehicle2);
    else:
        vehicles.append(vehicle)        
    i=1;
    #print(len(row[field]));
    for vehicleSplit in vehicles:
        if not "%s %s" % (field,i) in row.keys():
            row["%s %s" % (field,i)] = [None] * len(row[field]);
        #row["%s %s" % (field,i)].append({'rownum': rownum, 'value': vehicleSplit});
        #print(row["%s %s" % (field,i)]);
        myvalue = dcvalue.DCValue(vehicleSplit);
        row["%s %s" % (field,i)][rownum] = myvalue.trim().value;
        i=i+1
    #print(row);
