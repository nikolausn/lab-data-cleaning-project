#!/Volumes/HD-500GB/Users/nikolausn/Applications/anaconda/bin/python3.5
def testFunction(value,field,row):
    return value;

def vehicleSplit(value,field,row,rownum):
    #initialize new column
    
    vehicle = value.value;
    vehicles = vehicle.split("-");
    i=1;
    #print(len(row[field]));
    for vehicleSplit in vehicles:
        if not "%s %s" % (field,i) in row.keys():
            row["%s %s" % (field,i)] = [None] * len(row[field]);
        #row["%s %s" % (field,i)].append({'rownum': rownum, 'value': vehicleSplit});
        #print(row["%s %s" % (field,i)]);
        row["%s %s" % (field,i)][rownum] = vehicleSplit;
        i=i+1
    #print(row);