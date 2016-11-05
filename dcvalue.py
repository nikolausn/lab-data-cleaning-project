#!/usr/local/bin/python
import re;

class DCValue:
    'Data Cleaning value class, as a class template for any data cleaning default transform'    
    def __init__(self,value,field="",row=None):
        self.value = value;        
    
    #leftTrim
    def leftTrim(self):
        self.value = self.value.lstrip();
        return self;
    
    #righTrim
    def rightTrim(self):
        self.value = self.value.rstrip();
        return self;
    
    #trim
    def trim(self):
        self.leftTrim();
        self.rightTrim();
        return self;
    
    #regular expression cleanup / replace
    def regexReplace(self,pattern,replace):
        repattern = re.compile(pattern);
        tempvalue = self.value;
        self.value = re.sub(repattern,replace,self.value);
        #print('%s - %s' %(tempvalue,self.value))
        return self;
    
    #collapse multiple whitespace
    def collapseWhiteSpace(self):
        self.regexReplace('/\\s+/',' ');
        return self;
    
    #toUpper
    def toUpper(self):
        self.value = self.value.upper();
        return self;
    
    #toLower
    def toLower(self):
        self.value = self.value.lower();
        return self;
    
    #custom Function
    def customFunction(self,custom,field,row,rownum=None):
        custom(self,field,row,rownum);
        return self;
