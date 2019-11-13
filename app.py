import inputcsv
import re
import onelake
import config
import log
import utility

# enable logging for this instance. 
logger = log.enablelog()

# onelake transformation and fetch details 
def onelaketransformation(onelakefile):
    try:
        bucket, key = re.match(config.matchtext, onelakefile).groups()
        bucket = bucket.replace(config.replacetext,'')
        onelakefilecount = onelake.s3SelectFilecount(bucket,key)
        onelakecolumns = onelake.s3SelectColumns(bucket,key)
        onelakecolumcount = len(onelakecolumns)
        logger.info('Completed one lake file read for' +  onelakefile)
        return onelakefilecount,onelakecolumns,onelakecolumcount
    except:
        logger.error('Error in reading one lake file' + onelakefile)


# database transformation and fetch details
def databasetransformation(dbquery):
    try:     
        bucket, key = re.match(config.matchtext, dbquery).groups()
        bucket = bucket.replace(config.replacetext,'')
        onelakefilecount = onelake.s3SelectFilecount(bucket,key)
        onelakecolumns = onelake.s3SelectColumns(bucket,key)
        onelakecolumcount = len(onelakecolumns)
        logger.info('Completed fetching db query' + dbquery)
        return onelakefilecount,onelakecolumns,onelakecolumcount
    except:
        logger.error('Error in fetching db query' + dbquery)


# generate output elements
def generateoutputelements(output,index,onelakefilecount,onelakecolumns,onelakecolumncount,dbrowcount, dbcolumns,dbcolumncount):
    try:
        columns = config.outputcolumns
        values = [onelakefilecount,onelakecolumns,onelakecolumncount,dbrowcount, dbcolumns,dbcolumncount
                , (onelakefilecount==dbrowcount),set(onelakecolumns)==set(dbcolumns),(onelakecolumncount==dbcolumncount)]
        rowsandvalues = {k:v for k,v in zip(columns,values)}
        output[index] = rowsandvalues
        print('Successfully Transformed the row',index)
        logger.info('Completed output element transformation for ' + str(index) + ' record')
    except:
        print('Error in transforming the row',index)
        logger.error('Error in output element transformation for' + str(index) + ' record')

def main():
    # read input from csv file.
    input = inputcsv.readexcelfile()
    print('input file read and has',len(input), 'records')
    logger.info('input file read and has ' + str(len(input)) +  ' records')
    index = 1
    output = {}
    # loop through all the records file. 
    print('one lake and db transformation started. Please wait.......')
    for onelakefile, dbquery in input.items(): 
        # do the activity for onelake files 
        onelakefilecount,onelakecolumns,onelakecolumncount = onelaketransformation(onelakefile)
        dbrowcount, dbcolumns,dbcolumncount = databasetransformation(dbquery)
        generateoutputelements(output,index,onelakefilecount,onelakecolumns,onelakecolumncount,dbrowcount,dbcolumns,dbcolumncount)
        index = index +1
    
    # write output to file
    try:
        inputcsv.writeoutputtoexcel(output)
        logger.info('Completed output file export')
        print('one lake and db transformation completed successfully.')
    except:
        logger.error('Error in writing output file')
        print('one lake and db transformation has some errors. Please see log files')

if __name__ == "__main__": 
    utility.createoutputpath()
    main()