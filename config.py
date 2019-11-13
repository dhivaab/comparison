import boto3

# s3 url matching 
matchtext = r's3:\/\/(.+?)\/(.+)'
replacetext = '.s3.amazonaws.com'

# Establish s3 connectivity 
client = boto3.client('s3', 
                    aws_access_key_id='',
                    aws_secret_access_key='', 
                    region_name='')

# select Queries
s3selectcountquery = 'SELECT count(*) FROM s3object'
s3selectcolumnquery = 'SELECT * from s3object limit 1'


#input files and output files
inputfile = 'input/inputsheet.csv'
outputfile = 'output/transformoutput.xlsx'

#output columns
outputcolumns = ['onelakefilecount','onelakecolumns','onelakecolumncount','mongofilecount','mongocolumns','mongocolumncount',
              'filecountcomparision','columncomparison','columncountcomparison']


# log path
logpath = '/log/outputlog.txt'