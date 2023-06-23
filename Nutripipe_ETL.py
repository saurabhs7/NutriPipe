#import libraries
import pymongo
import json
from pymongo import MongoClient
import pandas as pd
import numpy as np
import pymysql
import os

#Set nutrition level tags to read (for reference only)
nutriTags = (
"energy_100g",
"proteins_100g",
"carbohydrates_100g",
"sugars_100g",
"fat_100g",
"saturated-fat_100g",
"trans-fat_100g",
"cholesterol_100g",
"fiber_100g",
"sodium_100g",
"vitamin-a_100g",
"vitamin-d_100g",
"vitamin-c_100g"
)

#Set product level tags to read (for reference only)
prodTags = (
"product_name",
"code",
"_id",
"brands"
)

#connect to local database server
client = MongoClient()

#switch to test DB
db = client.food

# function to print only first n documents (to avoid perf/memory issues)
def printhead(cursor, n):
    for idx,document in enumerate(cursor):
        if idx <= n: 
            print(document)
        else:
            break

# function to check if value exists in dataframe for nutrition info
def readNutrition(df, key):
    try:   
        myValue = df["nutriments"]
        myValue = df["nutriments"][key]       
    except:
        myValue = "NA"
    return(myValue)

# function to check if value exists in object
def readValue(df, key):
    try:   
        myValue = df[key]
    except:
        myValue = "NA"
    return(myValue)

# function to read all files in folder for UPCs
def readFiles(path):
    files = []
    print("Reading UPC files from: ",path)
    for filePath in sorted(os.listdir(path)):
        print("Reading file: ",filePath)
        fullPath = os.path.join(path, filePath)
        files.append(fullPath)
    print("Read ",len(files)," files")
    return(files)

#Paths for load files
myPath = "D:/DataCopy/UPC/"
myPanelistSalesPath = "D:/DataCopy/Year12/PANEL/" #Not used
mySQLUploadPath = "C:\\\\ProgramData\\\\MySQL\\\\MySQL Server 8.0\\\\Uploads\\\\"

all_files = readFiles(myPath)

%%time

myprods = db.products

upcMatched = []
tCount=0
tAllCount=0

for filePath in all_files:
    print("Searching UPCs from: ",filePath)
    file = open(filePath, "r")
    all_upcs = file.read().splitlines()
    iCount = 0
    iAllCount = 0
    for upc in all_upcs:
        result = pd.DataFrame({"Category":os.path.splitext(os.path.basename(file.name))[0], "Product":list(myprods.find({"code" : "0"+ upc }).limit(1))})
        if not result.empty:
            upcMatched.append(result)
            #Update counters
            iCount = iCount + 1
            tCount = tCount + 1
        iAllCount = iAllCount + 1
        tAllCount = tAllCount + 1
    print("Matched ", str(iCount), " UPCs out of ",str(iAllCount)," in: ",filePath)

print("Total matched ", str(tCount), " UPCs out of ",str(tAllCount)," in all files.")

%%time

myNutriDataFrame = pd.DataFrame()

for i, product in enumerate(upcMatched):
    headerVal_Category = product["Category"].values
    headerVal_UPCCode = readValue(product["Product"][0],"code")
    headerVal_brand = readValue(product["Product"][0],"brands")
    headerVal_name = readValue(product["Product"][0],"product_name")
    #Read nutritional tags
    nutriVal_energy = readNutrition(product["Product"][0],"energy_100g")    
    nutriVal_protein = readNutrition(product["Product"][0],"proteins_100g")    
    nutriVal_carbs = readNutrition(product["Product"][0],"carbohydrates_100g")    
    nutriVal_sugars = readNutrition(product["Product"][0],"sugars_100g")    
    nutriVal_fat = readNutrition(product["Product"][0],"fat_100g")    
    nutriVal_saturatedfat = readNutrition(product["Product"][0],"saturated-fat_100g")    
    nutriVal_transfat = readNutrition(product["Product"][0],"trans-fat_100g")    
    nutriVal_cholestrol = readNutrition(product["Product"][0],"cholesterol_100g")    
    nutriVal_fiber = readNutrition(product["Product"][0],"fiber_100g")    
    nutriVal_sodium = readNutrition(product["Product"][0],"sodium_100g")    
    nutriVal_vita = readNutrition(product["Product"][0],"vitamin-a_100g")    
    nutriVal_vitd = readNutrition(product["Product"][0],"vitamin-d_100g")    
    nutriVal_vitc = readNutrition(product["Product"][0],"vitamin-c_100g")    
    myNutriDataFrame = myNutriDataFrame.append({
        "Category" :headerVal_Category, 
        "UPCCode" :headerVal_UPCCode, 
        "UPCKey" :"",
        "Brand" :headerVal_brand,
        "Name" :headerVal_name ,
        "energy_100g" :nutriVal_energy,
        "proteins_100g":nutriVal_protein,
        "carbohydrates_100g":nutriVal_carbs,
        "sugars_100g":nutriVal_sugars,
        "fat_100g":nutriVal_fat,
        "saturated-fat_100g":nutriVal_saturatedfat,
        "trans-fat_100g":nutriVal_transfat,
        "cholesterol_100g":nutriVal_cholestrol,
        "fiber_100g":nutriVal_fiber,
        "sodium_100g":nutriVal_sodium,
        "vitamin-a_100g":nutriVal_vita,
        "vitamin-d_100g":nutriVal_vitd,
        "vitamin-c_100g":nutriVal_vitc},
        ignore_index=True)
    

myNutriDataFrame.head(5)

myNutriDataFrame.drop_duplicates(subset="UPCCode", keep='first', inplace=True)
myNutriDataFrame.to_csv("D:\IRIDataCopy\Test\Output.csv", sep='|', index=False, encoding='utf-8')

all_files_sales = readFiles(mySQLUploadPath)

%%time

# Open database connection
db = pymysql.connect("localhost","root","root","nutripipe" )


for file in all_files_sales:
    if "_PANEL_" in os.path.basename(file): 
        sqlhead = "LOAD DATA INFILE '"
        sqltail = "' INTO TABLE nutripipe.panelist_sales FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' IGNORE 1 LINES (panid, week, minute, units, outlet, dollars, iri_key, colupc);"
        execsql = sqlhead + file + sqltail
        
        # prepare a cursor object using cursor() method
        cursor = db.cursor() 
        try:
            # Execute the SQL command
            cursor.execute(execsql)
            db.commit()
        except:
            print ("Error: unable to load data for ", file)        
    else:
        continue

db.close()

