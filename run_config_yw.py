"""
@begin RunInitConfig @desc run init cleaning job using config file
@in infile
@param outfile_name
@in config_file
@out outfile
@out logfile
"""
def runConfig(infile,outfile,dcConfig):
    #make log stream for provenance
    
    """
    @begin OpenLogStream @desc open log stream for provenance
    @param outfile_name
    @out log_stream
    """
    logstream = openWriteFile(outfile+'.'+time.strftime("%y%m%d%H%M%S")+'.log');
    
    logstream.writerow(['id','field','timestamp','function','oldval','newval']);
    """
    @end OpenLogStream
    """              
    
    #read jsonconfig file
    jsonConfig = {};
    with open(dcConfig) as json_file:
        jsonConfig = json.load(json_file)
    
    
    """
    @begin OpenWriterStream @desc open writer stream
    @param outfile_name
    @out outfile_stream
    """
    #start data cleaning workflow
    #change fields array into tuple
    #fieldName = tuple(jsonConfig['fields']);
    #read csv file base on fields definition
    #open file
    writer = csv.writer(open(outfile, 'w'))
    """
    @end OpenWriterStream
    """
    
    """
    @begin OpenReadInputStream @desc read input_file into stream
    @in infile
    @out infile_stream
    """
    dcFile = open(infile, 'r');
    #skip header
    i = 0;
    #for i in range(1,jsonConfig['skipRow']):
    next(dcFile);
    reader = csv.DictReader(dcFile, jsonConfig['fields'],delimiter=jsonConfig['delimiter']);
    attrarr = jsonConfig['fields'].copy();
    """
    @end OpenReadInputStream
    """

    #Build vertical set for vertical cleaning
    vcleaning = jsonConfig['vcleaning'];
    
    vlist = {};
    for vset in vcleaning:
        vlist[vset['field']] = [];    
    
    """
    @begin StartHorizontalCleaning @desc doing horizontal cleaning in which cleaning process will be done for each row using stream
    @in log_stream
    @in infile_stream
    @in outfile_stream
    @in config_file
    @out horizontal_cleaned_file @uri file:{output_file}
    """
    i = 0;
    for row in reader:
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
        rowarr = [];
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
    """
    @end StartHorizontalCleaning
    """
    
    """
    @begin ParseVerticalConfig @desc parse vertical config value
    @in config_file
    @out vertical_config
    """
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
    """
    @end ParseVerticalConfig
    """
            
    dcFile.close();

    """
    @begin StartVerticalCleaning @desc doing vertical cleaning from the output of horizontal cleaning file
    @in horizontal_cleaned_file
    @in vertical_config
    @out outfile @uri file:{outfile_name}
    """
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
    """
    @end StartVerticalCleaning
    """
    
"""
@end RunInitConfig
"""
