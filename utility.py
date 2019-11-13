import os

# create output path
def createoutputpath():
    path = os.getcwd() + '/'
    if not os.path.exists(path + 'output'):
        os.makedirs(path + 'output')
    