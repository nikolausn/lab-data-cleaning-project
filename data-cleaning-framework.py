#!/Volumes/HD-500GB/Users/nikolausn/Applications/anaconda/bin/python3.5
import re;
import csv;
import json;

class DCValue:
    'Data Cleaning value class, as a class template for any data cleaning default transform'    
    def __init__(self,value,field="",row=None):
        self.value = value;        
    
    #leftTrim
    def leftTrim(self):
        self.value = self.value.lstrip();
    
    #righTrim
    def rightTrim(self):
        self.value = self.value.rstrip();
    
    #trim
    def trim(self):
        self.leftTrim();
        self.rightTrim();
    
    #regular expression cleanup / replace
    def regexReplace(self,pattern,replace):
        repattern = re.compile(pattern);
        tempvalue = self.value;
        self.value = re.sub(repattern,replace,self.value);
        print('%s - %s' %(tempvalue,self.value))
    
    #collapse multiple whitespace
    def collapseWhiteSpace(self):
        self.regexReplace('/\\s+/',' ');
    
    #toUpper
    def toUpper(self):
        self.value = self.value.upper();
    
    #toLower
    def toLower(self):
        self.value = self.value.lower();
    
    #custom Function
    def customFunction(self,custom,field,row):
        custom(self,field,row);

def customCommand(row):
    return 0;

class DCCommand:    
    def __init__(self,dcValue):
        self.dcValue = dcValue;
    
    def command(self,cmdObject,row=None):
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
                self.dcValue.customFunction(customFunction,field,row);
                                            
        return self.dcValue;       
                               
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
for i in range(1,jsonConfig['skipRow']):
    dcFile.next();

#i=0;
#with dcFile as csvRow:
#    row = csv.reader(csvRow, jsonConfig['fields'],delimiter=jsonConfig['delimiter']);
#    print("%i %s %s" %(i,row['Date Of Stop'],row['Time Of Stop']));    
#    i = i + 1;
reader = csv.DictReader(dcFile, jsonConfig['fields'],delimiter=jsonConfig['delimiter']);
i = 0;
attrarr = [];
for row in reader:
    #print("%i %s %s" %(i,row['Date Of Stop'],row['Time Of Stop']));
#    print(jsonConfig['hcleaning'][0]['VehicleType'][0]);
#    print("%s" %(cmd.command(jsonConfig['hcleaning'][0]['VehicleType'][0])).value);        
    
    #Do Horizontal cleaning
    hcleaning = jsonConfig['hcleaning'];
    for hcvalue in hcleaning:
        field = hcvalue['field'];
        #row[hcleaning[i]]['newField']] = row[field];
        dcField = DCValue(row[field]);
        dcCommand = DCCommand(dcField);
        for opvalue in hcvalue['operation']:
            dcCommand.command(opvalue,row);
        row[hcvalue['newField']] = dcField.value;
        print(dcField.value);
    rowarr = [];
    for rowattr,rowval in row.items():
        if(i==0):
            attrarr.append(rowattr)
        rowarr.append(rowval);
    if(i==0):
        writer.writerow(attrarr);    
    writer.writerow(rowarr);
    i = i + 1;    
    
print('rows: %s' %(i));