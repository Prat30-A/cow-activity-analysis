import os

#get the path of the data folder on any computer
data_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))

#Create an empty list to store full paths of all CSV files found
csvFiles = []
#Walk through all directories and subdirectories starting from 'data_root'
for path, subdirs, files in os.walk(data_root):
    #Loop through all files in the current directory
    for x in files:
        #Check if the file ends with ".csv"
        if x.endswith(".csv") == True:
            #Construct the full file path and add it to the list
            csvFiles.append(os.path.join(path, x))





