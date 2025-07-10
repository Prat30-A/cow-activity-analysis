import os
import pandas as pd

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


#List to store processed summary data for each cow/day
processedList = [] 

#Loop through each CSV file found in the data directory
for csv in csvFiles:
    try:
        #Read the CSV file using ';' as separator and drop any fully empty rows
        df_raw = pd.read_csv(csv, sep=';').dropna(how='all')
        #Skip this file if the resulting DataFrame is empty after cleanup
        if df_raw.empty:
            continue

        i = 0
        while i +288 <= len(df_raw.index):
            #add 288 to get the rows per day
            #segment df per day
            df_1 = df_raw.iloc[i:i+288]
            #increment i
            i += 288
            #get the cow id
            cow_num = df_1['animal_id'].iloc[0]
            #get the date
            dateTime = df_1['date (UTC)'].iloc[0].split()
            date = dateTime[0]
            #get ingestion count 
            ingestionMin = (df_1['ingestion_trough_pasture'] != 0).sum() *5
            #get rumination
            ruminationMin = (df_1['rumination'] != 0).sum() *5
            #get rest
            restMin = (df_1['rest'] != 0).sum() *5
            #get other_activity
            otherActMin = (df_1['other_activity'] != 0).sum() *5
            #get over_activity
            overActMin = (df_1['over_activity'] != 0).sum() *5
            #get standing_up 
            standingMin = (df_1['standing_up'] != 0).sum()*5 
            #create processed dict 
            processedDict = {'animal_id':cow_num, 'date (UTC)':date, 'ingestion_trough_pasture':ingestionMin, 'rumination':ruminationMin, 
                            'rest':restMin, 'other_activity':otherActMin, 'over_activity':overActMin, 'standing':standingMin}
            #append each proccessed day into a list
            processedList.append(processedDict)

    except pd.errors.EmptyDataError:
        #Skip the file if it's completely empty (no header or content)
        continue

#create a datafram of the list
processedDF = pd.DataFrame(processedList)
#print(processedDF)

num_unique_cows = processedDF['animal_id'].nunique()
print(num_unique_cows)
