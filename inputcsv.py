import pandas
import os
import config

# read excel file input 
def readexcelfile():
    inputcollections = {}
    result = pandas.read_csv(os.getcwd() + '/' + config.inputfile)
    for ind in result.index: 
        inputcollections[result['Onelakepath'][ind]] = result['Mongopath'][ind]
    return inputcollections

# write dictionary to excel file
def writeoutputtoexcel(output):
    df = pandas.DataFrame(output).T
    df.to_excel(config.outputfile)
