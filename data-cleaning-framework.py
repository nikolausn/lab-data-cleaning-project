import re;

class DCValue:
    'Data Cleaning value class, as a class template for any data cleaning default transform'
    value = "";
    
    def __init__(value):
        self.value = value;        
    
    #leftTrim
    def leftTrim():
        self.value = self.value.lstrip();
    
    #righTrim
    def rightTrim():
        self.value = self.value.rstrip();
    
    #trim
    def trim():
        leftTrim();
        rightTrim();
    
    #regular expression cleanup / replace
    def regexReplace(pattern,replace):
        self.value = re.sub(pattern,self.value,replace);
    
    #collapse multiple whitespace
    def collapseWhiteSpace():
        regexReplace(r'/\\s+/',' ');
    
    #toUpper
    def toUpper():
        self.value = self.value.upper();
    
    #toLower
    def toLower():
        self.value = self.value.lower();