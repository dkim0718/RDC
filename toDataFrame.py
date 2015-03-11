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
    columns = [u'num-connections', u'last-name', u'first-name', u'industry', u'location', u'public-profile-url']
    df = [] # List of people 
    db = json.load(open(filename))
    for person in db:
        # GET ALL POSITIONS
        try:
            ppos = DataFrame(person['positions'])
            
            # List of all columns required
            order = [u'company-name', u'title', u'start-date', u'end-date', u'summary', u'is-current']

            # If certain columns do not exist, append empty columns
            ppos = ppos.append(DataFrame(columns=order))
        except:
            # If no position, then move on to next person
            continue
        ################################################
        # Eduction is messed up; don't run this part
        ################################################
        # # GET MOST RECENT EDUCATION
        # try:
        #     # educ contains 'school-name', 'degree', 'field-of-study'
        #     educ = DataFrame(person['educations'][0],index=[0])
        #     if 'start-date' in list(educ):
        #         educ = educ.drop('start-date')
        #     elif 'end-date' in list(educ):
        #         educ=educ.drop('end-date')  
        #     ppos = ppos.join(educ)
        # except:
        #     # No education; do nothing and continue to get other variables
        #     pass
        # finally:
        #     # add stuff if missing 
        #     educ_order = [u'school-name', u'degree', u'field-of-study']
        #     ppos = ppos.append(DataFrame(columns=educ_order))
            
        # GET ALL OTHER VARIABLES
        for col in columns: 
            try:
                # Add each of 
                ppos[col] = person.get(col,float('nan'))
            except:
                # In case the statement above fails, do nothing, continue to add onto df
                pass

        # APPEND TO LIST OF PEOPLE TO TURN INTO DATA FRAME
        try: 
            df.append(ppos)
        except:
            # If everything is empty, continue to next person 
            continue 

    # TURN THE LIST OF PEOPLE INTO A SINGLE DATAFRAME
    try:
        df = pd.concat(df, ignore_index=True)
    except:
        print(filename,"error")

    #print(list(df))
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

    f = logfilename+'.csv'

    # Write DATA to OUTPUT_DIR
    all_cols = [u'company-name',u'end-date',u'first-name',u'industry',u'is-current',u'last-name',u'location',u'num-connections',u'public-profile-url',u'start-date',u'summary',u'title']
    data.to_csv(f,encoding='utf-8',index=False,columns=all_cols,header=False,mode='a')

    # Update logitems
    logitems['curr_time'] = str(datetime.datetime.now())

    # Update logfile     
    with open(logfilename + '.log','w+') as f:
        json.dump(logitems,f,encoding='utf-8')

def inputSequence(argv):
    """ Process user arguments and return input directory and output directory
    
    Keyword arguments:
    argv -- command line arguments passed by user
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
        elif opt in ("-r"):
            try:
                restart = arg == 'True'
            except:
                restart = False
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
        print("Invalid input for -i; Using /export/home/doctoral/dokim/Linkedin/Data instead")
        input_root_dir = os.getcwd()

    # Notify whether output_dir exists
    if not(os.path.exists(output_dir)):
        print("Invalid input for -o; Saving to /export/home/doctoral/dokim/Linkedin/Output instead")
        output_dir = '/export/home/doctoral/dokim/Linkedin/Output'
    print('')
    return (input_root_dir, output_dir, iteration, restart)

def getFileList(input_root_dir, output_dir, iteration, restart):
    # LOCAL VARIABLES
    # folders_to_process - all subdirectories of input_root_dir that contain json files
    # data_path - Subdirectory with all json files given the iteration number
    # jsonfilelist - List of all json files in data_path

    # Get subdirectory of json files specifed by iteration number
    folders_to_process = [item[0] for item in os.walk(input_root_dir) if item[2] != []] 
    print 'Trying to chdir to',folders_to_process[iteration]    
    try:
        os.chdir(folders_to_process[iteration])
        
        data_path = os.getcwd()
        print 'Current directory is',os.getcwd()
    except:
        print('Index invalid! Enter a number less than ',len(folders_to_process)-1)
        sys.exit(2)
    print('')

    # Create list of all json files 
    jsonfilelist = os.listdir(data_path)
    jsonfilelist = [f for f in jsonfilelist if f.split('.')[-1] == 'json']
    print('There are',len(jsonfilelist),'files in',data_path)

    # Initialize some parameters for picking up code where it left off
    logitems = {
        'nfiles':len(jsonfilelist),
        'logfilename':data_path.split('Data/')[-1].split('/')[0],
        'curr_time':str(datetime.datetime.now()),
        'curr_ind':0
        }

    # Check where we left off before
    logfilename = output_dir+'/'+logitems['logfilename']
    if (os.path.isfile(logfilename + '.log')):
        with open(logfilename + '.log','r') as f:
            existinglog = json.load(f,encoding='utf-8')
        print('Previously finished up to',existinglog['curr_ind'],existinglog['curr_time'])

    # TODO: GET SUBLIST OF JSONFILE TO PROCESS AND RETURN THAT
    # Change where to start
    if restart:
        start_here = 0
    else:
        # there might or might not be a existinglog variable
        try:
            start_here = existinglog['curr_ind']
        except:
            start_here = 0
    return jsonfilelist, start_here, logitems, logfilename, data_path
    
def main(argv):
    """ Open specified folder and access all subdirectories 

    Keyword arguments:
    argv -- statements of the form '-i <inputdir> -o <outputdir> -r <numberrun>' Defaults to '/export/home/doctoral/dokim/Linkedin/Data' for -i and '/export/home/doctoral/dokim/Linkedin/Output' for -o.
    restart -- Boolean denoting whether to restart the whole process

    """
    # Get input and output directories
    (input_root_dir, output_dir, iteration, restart) = inputSequence(argv)

    # Get a list of folders to process (folders with json files in them) within input_root_dir
    (jsonfilelist, start_here, logitems, logfilename, data_path) = getFileList(input_root_dir,output_dir,iteration,restart)

    # For each file in the folder, proess our file
    for i, fname in enumerate(jsonfilelist[start_here:]):
        logitems['curr_ind'] = i
        if (i % 100) == 0:
            print(fname,i+1,'of',logitems['nfiles'])

        # Delete file if first time
        if i == 0:
            print("Processing first file;")
            try: 
                f = logfilename+'.csv'
                os.remove(f)
                print('Existing file found! Removing.')
            except:
                pass

        # Sometimes the conversion to json doesn't work because the files are faulty
        try:
            df = jsonToDF(fname)
            
            # Write df to output_dir, log results.
            writeDF(df,output_dir,logitems)
        except:
            print('Error while processing! Appending to errors.txt')
            # Print curr_ind, fname, data_path, save to text file
            errorfile = '/export/home/doctoral/dokim/Linkedin/errors.txt'
            # This is for testing locally.
            # errorfile = output_dir + '/errors.txt'
            with open(errorfile,'a') as f:
                e_log = logitems
                e_log['fname'] = fname
                e_log['data_path'] = data_path
                json.dump(logitems,f,encoding='utf-8')
            continue
    print(fname, jsonfilelist[-1])
    return 

#os.chdir('C:/Users/doyoon/Downloads/1340404404/archive')
if __name__=='__main__':
    import time
    t0 = time.time()
    main(sys.argv[1:]);
    t1 = time.time()
    print(t1-t0)
