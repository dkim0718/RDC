# -*- coding: utf-8 -*-
import os, sys, getopt
from pprint import pprint
import csv  
import datetime
import json 
import pandas as pd
from pandas import DataFrame

def jsonToDF(filename):
    """ Return a DataFrame constructed from filename """
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

def writeDF(data,output_dir,logitems):
    """ Write data to output_dir, update and write logitems to logfilename

    Keyword arguments:
    data -- a Pandas DataFrame object
    output_dir -- output directory
    logitems -- dictionary with keys 'nfiles','logfilename','curr_time', 'curr_ind'
    """
    logfilename = output_dir+'/'+logitems['logfilename']
    # Check if logfile exists. If it does, load previous log. 
    if (os.path.isfile(logfilename + '.log')):
        with open(logfilename + '.log','r') as f:
            existinglog = json.load(f,encoding='utf-8')

    # Write DATA to OUTPUT_DIR
    with open(logfilename+'.csv','a') as f:
        data.to_csv(f,encoding='utf-8',index=False,header=False,mode='a')

    # Update logitems
    logitems['curr_time'] = str(datetime.datetime.now())

    # Update logfile     
    with open(logfilename + '.log','w+') as f:
        json.dump(logitems,f,encoding='utf-8')

    
def main(argv,restart=False):
    """ Open specified folder and access all subdirectories 

    Keyword arguments:
    argv -- statements of the form '-i <inputdir> -o <outputdir> -r <numberrun>' Defaults to '/export/home/doctoral/dokim/Linkedin/Data' for -i and '/export/home/doctoral/dokim/Linkedin/Output' for -o.
    restart -- Boolean denoting whether to restart the whole process

    """
    # Process user arguments
    input_root_dir = ''
    output_dir = ''
    try:
        opts, args = getopt.getopt(argv,"hi:o:r:c:",["ifile=","ofile=","restart=","chunk="])
    except getopt.GetoptError:
        print 'toDataFrame.py -i <inputdir> -o <outputdir> -r <restart?> -c <chunk>'
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print 'toDataFrame.py -i <inputdir> -o <outputdir> -r <restart?> -c <chunk>'
            sys.exit()
        elif opt in ("-i", "--ifile"):
            input_root_dir = arg
        elif opt in ("-o", "--ofile"):
            output_dir = arg
        elif opt in ("-c"):
            try:
                iteration = int(arg)
            except:
                print 'argument after -c needs to be number'
        elif opt in ("-r")
            try:
                restart = arg == 'True'
            except:
                print 'argument after -r needs to be True or False'
    print 'Input folder is "', input_root_dir
    print 'Output folder is "', output_dir
    print 'Chunk to be processed is', iteration
    print 'Overwrite?', restart

    # Try to change directory to input_root_dir; otherwise deploy on grid.
    try:
        os.chdir(input_root_dir )
    except:
        #Non existing directory; Seems to be running on grid
        os.chdir('/export/home/doctoral/dokim/Linkedin/Data')
        input_root_dir = os.getcwd()

    # Notify whether output_dir exists
    if not(os.path.exists(output_dir)):
        print("Invalid output for -o; Saving to /export/home/doctoral/dokim/Linkedin/Output instead")
        output_dir = '/export/home/doctoral/dokim/Linkedin/Output'
    print('\n')

    # Get a list of folders to process (folders with json files in them) within input_root_dir
    folders_to_process = [item[0] for item in os.walk(input_root_dir) if item[2] != []] 
    print 'Trying to chdir to',folders_to_process[iteration]
    try:
        os.chdir(folders_to_process[iteration])
        data_path = os.getcwd()
        print 'Current directory is',os.getcwd()
    except:
        print('Index invalid! Enter a number less than ',len(folders_to_process)-1)
        sys.exit(2)
    print('\n')

    # Get a list of all the json files to be processed
    jsonfilelist = os.listdir(data_path)
    jsonfilelist = [f for f in jsonfilelist if f.split('.')[-1] == 'json']
    print('There are',len(jsonfilelist),'files in',data_path)

    # Initialize some parameters for picking up code where it left off
    logitems = {
        'nfiles':len(jsonfilelist),
        'logfilename':data_path.split('Data/')[-1].split('/')[0],
        'curr_time':datetime.datetime.now(),
        'curr_ind':0
        }

    # Check where we left off before
    logfilename = output_dir+'/'+logitems['logfilename']
    if (os.path.isfile(logfilename + '.log')):
        with open(logfilename + '.log','r') as f:
            existinglog = json.load(f,encoding='utf-8')
        print('Previously finished up to',existinglog['curr_ind'],existinglog['curr_time'])

    # Change where to start
    if restart:
        start_here = 0
    else:
        # there might or might not be a existinglog variable
        try:
            start_here = existinglog['curr_ind']
        except:
            start_here = 0

    # For each file in the folder, proess our file
    for fname in jsonfilelist[start_here:-1]:
        curr_ind = jsonfilelist.index(fname)
        logitems['curr_ind'] = curr_ind
        if (curr_ind % 100) == 0:
            print(fname,curr_ind+1,'of',logitems['nfiles'])

        # Sometimes the conversion to json doesn't work because the files are faulty
        try:
            df = jsonToDF(fname)
        except:
            print('Error while processing! Appending to errors.txt')
            # Print curr_ind, fname, data_path, save to text file
            with open('/export/home/doctoral/dokim/Linkedin/errors.txt','a') as f:
                e_log = logitems
                e_log['fname'] = fname
                e_log['data_path'] = data_path
                json.dump(logitems,f,encoding='utf-8')

        # Write df to output_dir, log results.
        writeDF(df,output_dir,logitems)
    return 

#os.chdir('C:/Users/doyoon/Downloads/1340404404/archive')
if __name__=='__main__':
    import time
    t0 = time.time()
    # # Uncomment below to restart; otherwise start where we last finished.
    # main(sys.argv[1:],True);
    main(sys.argv[1:]);
    t1 = time.time()
    print(t1-t0)
