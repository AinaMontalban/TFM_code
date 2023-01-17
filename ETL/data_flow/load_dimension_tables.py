###############################################################################
# Python script to transform and populate data into the Dimension Tables      #
# Author: Aina Montalbán
# Function: functions to populate dimension tables
# Date: Desember 2022
###############################################################################

# import needed packages
from datetime import date
import re
import os
import pandas as pd
import numpy as np
from interop import py_interop_run_metrics, py_interop_run, py_interop_summary
from utils import *


#########################################
##-- Turn Dimension
#########################################
def etl_TurnDimension(input, engine ):
    ###- Input: algorithm that populates the Turn dimension (insert rows directly)
    df=pd.DataFrame(np.arange(1,input+1,1))
    df.columns=['turn_id']
    with engine.connect().execution_options(autocommit=True) as conn:
        df.to_sql('turn_dimension', con=conn, if_exists='append', index= False)
    print("Records created successfully in Turn Dimension Table.")  


#########################################
##-- Time Dimension
#########################################
##- Input: algorithm that populates the dimension (insert rows directly)
def etl_Time_Date_Dimension(input, engine):
    ## Date Dimension ##
        ## We only need day, month, and year.
    start_date=input[0]
    end_date=input[1]
    # create the range date
    days = pd.date_range(start=start_date, end=end_date)

    df = pd.DataFrame({'day_id': days})
    df['month_number']=df['day_id'].dt.month
    df['month_name'] = df['day_id'].dt.month_name()
    df['year']=df['day_id'].dt.year
    df = df.set_index('day_id')

    with engine.connect().execution_options(autocommit=True) as conn:
        df.to_sql('day_dimension', con=conn, if_exists='append', index= True)
    print("Records created successfully in Day Dimension Table.")  

        # SAMPLE INFO
    input_samples_date="/home/amontalban/Documents/TFM/data/sample_info"
    sample_info_files = [os.path.join(input_samples_date, fn) for fn in os.listdir(input_samples_date)]
    print("Processing sample info")
    samples_timestamp=[]
    for fl in sample_info_files:
        sample_info_file=open(fl, "r")
        for line in sample_info_file:
            if re.search("date", line):
                timestamp=line.split(',')[1].rstrip()
                time_pd=pd.to_datetime(timestamp, infer_datetime_format=False)  
                date_pd = pd.to_datetime(timestamp).date()
        samples_timestamp.append([time_pd ,date_pd])

    time_4_samples = pd.DataFrame(samples_timestamp, columns=["time_id","day_id"])

            # Run date
    input_run_date="/home/amontalban/Documents/TFM/data/sequencing_info/"
    run_folders = [fn for fn in os.listdir(input_run_date)]

    run_data = []
    for run in run_folders:
        try:
            run_path=os.path.join(input_run_date, run)#, #"run_qc/illumina/")
            run_info = py_interop_run.info()
            run_info.read(run_path)
            date_info=run_info.date()
            if len(date_info) == 6:
                my_date_edited=str(date_info[4:])+str(date_info[2:4])+str(date_info[:2])
                print(date_info)
                time_pd=pd.Timestamp(my_date_edited).strftime('%d-%m-%Y %X')
                date_pd = pd.to_datetime(time_pd).date()
                run_data.append([time_pd, date_pd])
            else:
                time_pd=pd.Timestamp(date_info).strftime('%d-%m-%Y %X')
                date_pd = pd.to_datetime(date_info).date()
                run_data.append([time_pd, date_pd])
        except:
            pass

            
    time_4_runs = pd.DataFrame(run_data, columns=["time_id","day_id"])
    time_dim=pd.concat([time_4_runs, time_4_samples])
    time_dim=time_dim.drop_duplicates()
    with engine.connect().execution_options(autocommit=True) as conn:
        time_dim.to_sql('time_dimension', con=conn, if_exists='append', index= False)
    print("Records created successfully in Time Dimension Table.")  

#########################################
##-- Panel Dimension
#########################################
def etl_PanelDimension(file, engine):
    #### Extraction ####
    df = pd.read_csv(file, delimiter=',', header=None)

    ### Transformation ####
    df.columns=['panel_id', 'panel_type', 'brand']

    ### Loading ####
    if not df.empty:
        with engine.connect().execution_options(autocommit=True) as conn:
            df.to_sql('panel_dimension', con=conn, if_exists='append', index= False)
        print("Records created successfully in Panel Dimension Table.")
    else:
        print("No more records to add in Panel Dimension Table.")


#########################################
##-- Instrument,Model, Reagent Dimensions
#########################################
def etl_instr_reagent(intr_input_file, reag_input_file, engine):
    ## Extraction ###
    df = extract_csv_file(intr_input_file, delim=',')

    df_model=df.loc[:, ['model_id','brand']]
    df_model = df_model.drop_duplicates(subset=['model_id'])
    df_model['model_id']=df_model['model_id'].astype(str)
 # process dataframe (data type coversion and removing duplicates if present)
    with engine.connect().execution_options(autocommit=True) as conn:
        if not df_model.empty:
            df_model.to_sql('instrument_model_dimension', con=conn, if_exists='append', index= False)
            print("Records created successfully in Instrument Model Dimension Table.")

    ## Instrument
    query1 = "SELECT model_id FROM instrument_model_dimension"
    model_dim=pd.read_sql(query1, con=engine) 
    model_dim['model_id']=model_dim['model_id'].astype(str)

    df_instrument=df.loc[:, ['instrument_id','instrument_name','model_id']]    
    df_instrument['instrument_id']=df_instrument['instrument_id'].astype(str)
    df_merged=df_instrument.merge(model_dim, how='inner', on='model_id')
    df_merged=df_merged.loc[:, ['instrument_id','instrument_name','model_id']]    
    df_merged.columns=['instrument_id','instrument_name','model_id']

     # process dataframe (data type coversion and removing duplicates if present)
    with engine.connect().execution_options(autocommit=True) as conn:
        if not df_merged.empty:
            df_merged.to_sql('instrument_dimension', con=conn, if_exists='append', index= False)
            print("Records created successfully in Instrument  Dimension Table.")
        else:
            print("No more records to add in Instrument  Dimension")
    
    # NextSeq: CartridgePartNumber
    ## Extraction ###
    df_r = extract_csv_file(reag_input_file, delim=',')

    ## Transformation
    df_r.columns = ['reagent_id', 'kit', 'num_cycles','model_id', 'brand']
    df_r = df_r.drop_duplicates(subset=['reagent_id'])
    df_r['reagent_id']=df_r['reagent_id'].astype(str)
    ## DATA TYPES CONVERSION
    
    df_merged_r=df_r.merge(model_dim, how='inner', on='model_id')
    # Select columns
    df_merged_r=df_merged_r.loc[:, ['reagent_id', 'kit', 'num_cycles','model_id']]
    df_merged_r.columns=['reagent_id', 'kit', 'num_cycles','model_id']
    
    # process dataframe (data type coversion and removing duplicates if present)
    if not df_merged_r.empty:
        with engine.connect().execution_options(autocommit=True) as conn:
            df_merged_r.to_sql('reagent_dimension', con=conn, if_exists='append', index= False)
        print("Records created successfully in Reagent Dimension Table.")



#########################################
##-- Sample Dimension
#########################################
def etl_SampleDimension(input, engine):

    ### Extraction ###
    #data_files = [os.path.join(input[0], fn, "run_samples_servolab.txt") for fn in os.listdir(input[0]) if (fn.startswith("R") and fn[1].isdigit())]
    data_files = [os.path.join(input[0], fn) for fn in os.listdir(input[0])]
    
    # Create data frame to get sample sex and diagnosis information
    samples_df=pd.DataFrame(data=None, columns=['sample_id','sex','diagnosis_code','diagnosis_name'])
    for f in data_files:
        try:
            # Extraction of servolab files to get sample basic information
            df = extract_csv_file(f, delim='\t')
            # add column names to dataframe
            df.columns = ['sample_id','sex', 'diagnosis_code','diagnosis_name']

            # if sex if unknown, we will have a GRAV value (i.e., the default)

            # concat all servolab to get one single dataframe
            samples_df=pd.concat([samples_df, df])
        except OSError:
            pass    

    ### Transformation ###
    
    # Conversion datatype to string or integer
    samples_df['sample_id']=samples_df['sample_id'].astype(str)
    samples_df['diagnosis_name']=samples_df['diagnosis_name'].astype(str)
    samples_df['diagnosis_code']=samples_df['diagnosis_code'].astype(int)

    # Remove duplicates
    samples_df=samples_df.drop_duplicates(subset=['sample_id'])
    #samples_df=samples_df.drop_duplicates()

    # Enrich data by adding department based on the diagnosis
    service_information = pd.read_csv(input[1], header=None, delimiter="\t") 
    service_information.columns = ['diagnosis_name', 'cdb_service_id']
    service_information['diagnosis_name']=service_information['diagnosis_name'].astype(str)
    service_information['cdb_service_id']=service_information['cdb_service_id'].astype(str)
    df_service=samples_df.merge(service_information, how='left', on='diagnosis_name')
                # if diagnosis is NA add Unknown
    df_service.diagnosis_code = df_service.diagnosis_code.fillna('Unknown')
    df_service.diagnosis_name = df_service.diagnosis_name.fillna('Unknown')
    df_service.cdb_service_id = df_service.cdb_service_id.fillna('Unknown')

    if not df_service.empty:
        with engine.connect().execution_options(autocommit=True) as conn:
            df_service.to_sql('sample_dimension', con=conn, if_exists='append', index= False)
        print("Records created successfully in Sample Dimension Table.")

  
#########################################
##-- Run Dimension
#########################################
def etl_RunDimension(input, engine):
        # Crossing multiple sources
    input1 = input[0]
    input2 = input[1]

    # Read Excel file runs 
    runs_lab = pd.read_excel(input2, skiprows=1, engine='openpyxl')
    runs_lab=runs_lab.loc[:, ['RUN ID','TÈCNIC']]
    # change column names
    runs_lab.columns=['run_id','technician']
    # remove duplicates
    runs_lab=runs_lab.drop_duplicates()
    # Get technician
    runs_lab['technician'] = runs_lab['technician'].str.upper()


    # Read from another source to get protcol correct
    runs_df=pd.DataFrame(data=None, columns=['run_id','protocol_id'])
    run_folders = [os.path.join(input1, fn) for fn in os.listdir(input1)]
    
    # iterate
    run_rows=[]
    for run in run_folders:
        run_info_file=open(run, "r")
        for line in run_info_file:
            if re.search("Protocol", line):
                protocol=line.split(',')[1].rstrip() # TRANSFORM
        run_rows.append([run.split('_')[3].split('.')[0], protocol])
        
    cluster_runs_df = pd.DataFrame(run_rows, columns=["run_id", "protocol_id"])

    # Read from another source to get protcol correct
    # JOIN

    df=cluster_runs_df.merge(runs_lab, how='left', on='run_id')
    # Remove duplicates
    df=df.drop_duplicates(subset=['run_id'])
    df.technician = df.technician.fillna('Unknown')
    
    runs_df=df.loc[:, ['run_id','protocol_id','technician']]
    runs_df=runs_df.drop_duplicates(subset=['run_id'])    
   
    if not runs_df.empty:
        with engine.connect().execution_options(autocommit=True) as conn:
            runs_df.to_sql('run_dimension', con=conn, if_exists='append', index= False)
            print("Records created successfully in Run Dimension Table.")


