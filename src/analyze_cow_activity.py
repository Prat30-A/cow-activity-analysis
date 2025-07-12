import os
import pandas as pd
import pdfplumber 
import datetime

#get the path of the data folder on any computer
data_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))

#Create an empty list to store full paths of all CSV files found
csvFiles = []
#create an empty list to store pdf files
pdfFiles = []
#Walk through all directories and subdirectories starting from 'data_root'
for path, subdirs, files in os.walk(data_root):
    #Loop through all files in the current directory
    for x in files:
        #Check if the file ends with ".csv"
        if x.endswith(".csv") == True:
            #Construct the full file path and add it to the list
            csvFiles.append(os.path.join(path, x))
        if x.endswith(".pdf") == True:
            pdfFiles.append(os.path.join(path, x))

pdfList = []

for pdf_path in pdfFiles:
    #get the last part of the file name
    file_name = os.path.basename(pdf_path)       
    #get the cow number and remove the .pdf       
    cow_id = os.path.splitext(file_name)[0]           
  
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            #get all the text in the page
            text = page.extract_text()
            #split the page into lines
            lines = text.split('\n')
            #Find where the table started
            tableStart = False
            for line in lines:
                if 'Dscrp' in line and 'Date' in line:
                    tableStart = True
                    continue
                #keep looping forward while tablestart is false
                if not tableStart:
                    continue
                
                #break each line into words
                words = line.strip().split()
                try:
                    format = '%m/%d/%Y'
                    pdfDict = {'animal_id':cow_id, 'date (UTC)':datetime.datetime.strptime(words[3],format).date(),'event':words[2]}
                    pdfList.append(pdfDict)
                #if date isnt formatted properly(error was encountered)
                except ValueError:
                    continue
#create the df from the list 
processedPdfDf = pd.DataFrame(pdfList)
    
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
processedCsvDf = pd.DataFrame(processedList)
#convert each df to one data type
processedCsvDf["animal_id"] = processedCsvDf["animal_id"].astype(str)
processedPdfDf["animal_id"] = processedPdfDf["animal_id"].astype(str)

#merge both df in an outer join to include all the combined data and fill the rest with Nan
merged_df = pd.merge(processedCsvDf,processedPdfDf, on=["animal_id", "date (UTC)"],how="outer")

