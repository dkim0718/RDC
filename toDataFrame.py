# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import os
import json
import pandas as pd
from pandas import DataFrame
import csv

def jsonToDF(filename):
    """ This function takes a filename as input and outputs a dataframe """
    
    # Columns defines the columns of the resulting dataframe
    columns = ['num-connections', 'last-name', 'first-name', 'industry', 'location', 'public-profile-url']
    npeople = 0;
    noposition = 0
    nerrors = 0
    df = []
    errors = []
    db = json.load(open(filename))
    for person in db:
        npeople += 1
        # GET ALL POSITIONS
        try:
            ppos = DataFrame(person['positions'])        
        except:
            # If no position, then move on to next person
            errors.append(person)
            noposition += 1
            continue
        
        # GET MOST RECENT EDUCATION
        try:
            educ = DataFrame(person['educations'][0],index=[0])
            if 'start-date' in list(educ):
                educ = educ.drop('start-date')
            elif 'end-date' in list(educ):
                educ=educ.drop('end-date')  
            ppos = ppos.join(educ)
        except:
            nerrors += 1
            
        # GET ALL OTHER VARIABLES
        for col in columns: 
            try:
                ppos[col] = person.get(col,float('nan'))
            except:
                nerrors += 1
        try: 
            df.append(ppos)
        except: 
            continue 
    try:
        df = pd.concat(df, ignore_index=True)
    except:
        print(filename,"error")
    # Print for debugginf purposes
#    print(filename, 'Processed:', npeople, "No position:", noposition, "Errors:", nerrors)
    return df

def processFolder(pathname):
    # Set up main directory
    os.chdir(pathname)
    json_files = os.listdir(os.getcwd()) # List of files in a folder
    outputfilename = '/Users/dokim/Documents/outputfilename.csv'

    # Keep track of how many files we have processed
    nfiles = len(json_files)
    processed = 0
    output = DataFrame()
    
    for filename in json_files:
        if processed == 0:
            pass
        temp = jsonToDF(filename)
        # Having such big datasets may be bad for memory; switching to writing as we go instead.
        #output = pd.concat([output,temp], ignore_index=True)
        
        if processed == 0:
            temp.to_csv(outputfilename,encoding='utf-8',index=False)
        else:
            temp.to_csv(outputfilename,header=False,encoding='utf-8',mode='a',index=False)
        processed += 1
    if processed % 50 == 0:
        return str(processed)+' out of '+str(nfiles)+' processed.'

def main():
    os.chdir()

#os.chdir('C:/Users/doyoon/Downloads/1340404404/archive')
if __name__=='__main__':
    print main();
