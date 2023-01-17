import os
import pandas as pd
import re
import numpy as np
from interop import py_interop_run_metrics, py_interop_run, py_interop_summary
from utils import trim_sample_id
from datetime import datetime
from datetime import timedelta
import xml.etree.ElementTree as et 


#########################################
##-- Extraction QC
#########################################
def etl_extraction_qc(input, engine):

    ## Extraction ##
    samples_prep_df = pd.read_csv(input, decimal=',')

    ## Transformation ##
    samples_prep_df.columns = ['Sample','Concentrationx5', 'Ratio_260_280','Ratio_260_230','Date']

    # data conversion
    samples_prep_df['concentration']=samples_prep_df['Concentrationx5'].astype(float)
    samples_prep_df['a260_a280_ratio']=samples_prep_df['Ratio_260_280'].astype(float)
    samples_prep_df['a260_a230_ratio']=samples_prep_df['Ratio_260_230'].astype(float)

    samples_prep_df['sample_id']=samples_prep_df['Sample'].astype(float).astype('Int64').astype('str')
    samples_prep_df['sample_id']=samples_prep_df.apply(trim_sample_id, axis=1)

    # turn id 
    samples_prep_df['turn_id']=samples_prep_df.groupby(['sample_id']).cumcount()+1
    samples_prep_df = samples_prep_df.loc[:,['sample_id', 'Date',  'turn_id','Concentrationx5', 'Ratio_260_280', 'Ratio_260_230']]
    samples_prep_df.columns=['sample_id', 'day_id',  'turn_id','concentration', 'a260_a280_ratio', 'a260_a230_ratio']

    # derive status
    #samples_prep_df['status'] = samples_prep_df.apply(derive_status_extraction, axis=1)

    # look up table to sample and date
    sql_samples = "select sample_id from sample_dimension"
    df_samplesdim=pd.read_sql(sql_samples, con=engine)
    df_merged=samples_prep_df.merge(df_samplesdim, how='inner', on='sample_id')

    with engine.connect().execution_options(autocommit=True) as conn:
        df_merged.to_sql('extraction_qc', con=conn, if_exists='append', index= False)
    print("Records created successfully in Extraction QC Fact Table.")   


#########################################
##-- Library QC
#########################################
def etl_library_qc(input, engine):
    input1=input[0] # MiSeq Library Data
    input2=input[1] # NextSeq Library Data

    ## Extraction ##
    lib_miseq_data = pd.read_csv(input1, decimal=',')
    lib_nextseq_data = pd.read_csv(input2)

    ###############################
    ## Transformation for MiSeq ##
    ###############################
    # Select columns
    lib_miseq_data=lib_miseq_data.loc[:, ['DNA','Qbit llibreria', 'Nom RUN' ,'Data Bioanalyzer', 'Bioanalyzer (pb)']]
    lib_miseq_data.columns=['sample_id','concentration','run_id', "day_id", "fragment_size"]
    lib_miseq_data=lib_miseq_data.dropna(subset=['sample_id']) # remove values with mising data 

    # Get only digit values from sample_id column
    lib_miseq_data=lib_miseq_data[lib_miseq_data['sample_id'].astype(str).str.isdigit()]

    # Data types converssion
    lib_miseq_data['sample_id']=lib_miseq_data['sample_id'].apply(str)
    lib_miseq_data['run_id']=lib_miseq_data['run_id'].apply(str)

    # Trimming sample_ids
    lib_miseq_data['sample_id']=lib_miseq_data.apply(trim_sample_id, axis=1)

    # transform metrics of interest to float values:
    ## 1, first replace commas for dots
    lib_miseq_data['concentration'] = lib_miseq_data['concentration'].replace(',','.',regex=True)
#    lib_miseq_data['pool_concentration'] = lib_miseq_data['pool_concentration'].replace(',','.',regex=True)
    lib_miseq_data['fragment_size'] = lib_miseq_data['fragment_size'].replace(',','.',regex=True)
    # 2. convert to float
    lib_miseq_data['concentration']=pd.to_numeric(lib_miseq_data.concentration, errors='coerce', downcast='float')
 #   lib_miseq_data['pool_concentration']=pd.to_numeric(lib_miseq_data.pool_concentration, errors='coerce', downcast='float')
    lib_miseq_data['fragment_size']=pd.to_numeric(lib_miseq_data.fragment_size, errors='coerce', downcast='float')

    ###############################
    ## Transform for NextSeq Spreadsheet
    ###############################
    lib_nextseq_data=lib_nextseq_data.loc[:, ['DNA','Qbit llibreria', 'Nom RUN','Data Integritat', 'Bioanalyzer (pb)']]
    lib_nextseq_data.columns=['sample_id','concentration','run_id',"day_id", "fragment_size"]
    lib_nextseq_data=lib_miseq_data.dropna(subset=['sample_id']) 

    # Get only digit values
    lib_nextseq_data=lib_miseq_data[lib_miseq_data['sample_id'].astype(str).str.isdigit()]
    lib_nextseq_data['sample_id']=lib_miseq_data['sample_id'].apply(str)
    lib_nextseq_data['run_id']=lib_miseq_data['run_id'].apply(str)

    lib_nextseq_data['concentration'] = lib_nextseq_data['concentration'].replace(',','.',regex=True)
   # lib_nextseq_data['pool_concentration'] = lib_nextseq_data['pool_concentration'].replace(',','.',regex=True)
    lib_nextseq_data['fragment_size'] = lib_nextseq_data['fragment_size'].replace(',','.',regex=True)

    lib_nextseq_data['concentration']=pd.to_numeric(lib_nextseq_data.concentration, errors='coerce', downcast='float')
    #lib_nextseq_data['pool_concentration']=pd.to_numeric(lib_nextseq_data.pool_concentration, errors='coerce', downcast='float')
    lib_nextseq_data['fragment_size']=pd.to_numeric(lib_nextseq_data.fragment_size, errors='coerce', downcast='float')

    # Concatanate the two sources
    df = pd.concat([lib_miseq_data, lib_nextseq_data])

    # trim sample id
    df['sample_id']=df.apply(trim_sample_id, axis=1)
    # select columns of interest
    df.columns=['sample_id','concentration', 'run_id',"day_id", "fragment_size"]

    # convert datetime
    df['day_id'] = pd.to_datetime(df['day_id'], errors='coerce')
    df['day_id'] = df['day_id'].dt.strftime('%Y-%m-%d')

    # select period of time
    df=df[(df['day_id'] > '2017-01-01') & (df['day_id'] < '2022-12-31')]

    # clean run id column
    df['run_id']=df['run_id'].str.split('_', 1, expand=True)[0]

    df['turn_id']=df.groupby(['sample_id']).cumcount()+1


    # select only samples present in the sample dimension
    sql_samples = "select sample_id from sample_dimension"
    df_samplesdim=pd.read_sql(sql_samples, con=engine)
    df_samplesdim['sample_id']=df_samplesdim['sample_id'].apply(str)

    # select only runs in the run dimension
    sql_runs = "select run_id from run_dimension"
    df_rundim=pd.read_sql(sql_runs, con=engine)
    df_rundim['run_id']=df_rundim['run_id'].apply(str)

    # merge
    df_merged_samples=df.merge(df_samplesdim, how='inner', on='sample_id')
    df_merged=df_merged_samples.merge(df_rundim, how='inner', on='run_id')
    df_merged.columns=['sample_id','concentration','run_id',"day_id", "fragment_size", 'turn_id']

    df_new=df_merged.drop_duplicates() 

    with engine.connect().execution_options(autocommit=True) as conn:
        df_new.to_sql('library_qc', con=conn, if_exists='append', index= False)
    print("Records created successfully in Library QC Fact Table.")


#########################################
##-- Sequencing QC
#########################################
def etl_sequencing_qc(input, engine):
    #    run_folders = [fn for fn in os.listdir(input) if (fn.startswith("R") and fn[1].isdigit())]
    run_folders = [fn for fn in os.listdir(input)]

    run_data = []
    for run in run_folders:
        run_path=os.path.join(input, run)#, #"run_qc/illumina/")
        if os.path.exists(os.path.join(run_path, "InterOp")) and os.path.exists(os.path.join(run_path, "RunInfo.xml")) and os.path.exists(os.path.join(run_path, "RunParameters.xml")):
            ### Run Info metrics
            run_info = py_interop_run.info()
            run_info.read(run_path)
            num_cycles=run_info.total_cycles() # number of cycles
            instrument=run_info.instrument_name() # instrument name
            date_info=run_info.date() # date
            if len(date_info) == 6:
                my_date_edited=str(date_info[4:])+str(date_info[2:4])+str(date_info[:2])
                time_pd=pd.Timestamp(my_date_edited).strftime('%d-%m-%Y %X')
            else:
                time_pd=pd.Timestamp(date_info).strftime('%d-%m-%Y %X')
            # reagent -> read RunInfo.xml
            try:
                xtree=et.parse(os.path.join(run_path, "RunParameters.xml"))
                xroot = xtree.getroot() 
                for node in xroot:
                    if node.tag=="CartridgePartNumber" or node.tag=="ReagentKitPartNumberEntered":
                        reagent_id=node.text
                    else:
                        pass
            except:
                reagent_id=""
                print("Reagent Not Found")
            ## Run Metrics
            run_metrics = py_interop_run_metrics.run_metrics()
            run_folder_metrics = run_metrics.read(run_path)
            valid_to_load = py_interop_run.uchar_vector(py_interop_run.MetricCount, 0)
            py_interop_run_metrics.list_summary_metrics_to_load(valid_to_load)
            summary = py_interop_summary.run_summary()
            py_interop_summary.summarize_run_metrics(run_metrics, summary)

            # Get metrics
            pct_gt_30=getattr(summary.total_summary(), 'percent_gt_q30')()
            yieldg=getattr(summary.total_summary(), 'yield_g')()
            pct_aligned=getattr(summary.total_summary(), 'percent_aligned')()
            cluster_density_mean=(summary.at(0).at(0).density().mean())/1000
            cluster_pf=summary.at(0).at(0).percent_pf().mean()
            run_data.append([run, time_pd, instrument, reagent_id,num_cycles, pct_gt_30, yieldg, pct_aligned, cluster_density_mean, cluster_pf])
        else:
                pass    

    df = pd.DataFrame(run_data, columns=["run_id", "time_id","instrument_id","reagent_id","num_cycles", "pct_gt_30", "yieldg", 
            "pct_aligned", "cluster_density", "cluster_pf"])

    # look up table to sample and date
    sql_runs = "select run_id from run_dimension"
    df_runsdim=pd.read_sql(sql_runs, con=engine)

    df_merged=df.merge(df_runsdim, how='inner', on='run_id')
    print(df_merged.time_id.dtype)
    with engine.connect().execution_options(autocommit=True) as conn:
        df_merged.to_sql('sequencing_qc', con=conn, if_exists='append', index= False)



#########################################
##-- Bioinformatics Analysis QC
#########################################
def etl_bioinfo_analysis_qc(input, engine): # path="/corebm/seq/results/by_run/"
    # sample panel info
    input1=input[0]
    # fastqc info
    input2=input[1]
    # coverage info
    input3=input[2]

    # SAMPLE INFO
    sample_info_files = [os.path.join(input1, fn) for fn in os.listdir(input1)]
    print("Processing sample info")
    sample_rows=[]
    for fl in sample_info_files:
        sample_info_file=open(fl, "r")
        for line in sample_info_file:
            if re.search("sample", line):
                s=line.split(',')[1].rstrip()
            elif re.search("RunName", line):
                run_id=line.split(',')[1].rstrip()
            elif re.search("library", line):
                library=line.split(',')[1].rstrip() 
            elif re.search("date", line):
                date=line.split(',')[1].rstrip()
                #date_pd=pd.to_datetime(date, infer_datetime_format=False) 
                date_pd=pd.Timestamp(date).strftime('%d-%m-%Y %X')  
        sample_rows.append([s, run_id, library, date_pd])

    df_4_info = pd.DataFrame(sample_rows, columns=["sample_id","run_id", "panel_id","time_id"])
    df_4_info['sample_id']=df_4_info['sample_id'].astype(str)
        
    # Read MultiQC data
    fastqc_files = [os.path.join(input2, fn) for fn in os.listdir(input2)]
    print("Processing sample multiqc")
    df_fastqc=pd.DataFrame(data=None, columns=['sample_id', 'run_id', 'pct_duplicates', 'pct_gc', 'read_length', 'pct_failed', 'total_sequences'])
    for fl in fastqc_files:
        fastqc_data=pd.read_csv(fl, delimiter='\t')
        fastqc_data.columns=['Sample_File', 'pct_duplicates', 'pct_gc', 'read_length', 'pct_failed', 'total_sequences']
        fastqc_data['run_id']=fl.split('_')[4].split('.')[0]
        fastqc_data['sample_id']=fastqc_data.Sample_File.str.split("_", 1, expand=True)[0]                
        fastqc_data['read_id']=fastqc_data.Sample_File.str.split("_", expand=True)[2]
        fastqc_data = fastqc_data.loc[:, ['sample_id', 'read_id' , 'run_id', 'pct_duplicates', 'pct_gc', 'read_length',  'pct_failed', 'total_sequences']]
        df_run=fastqc_data.groupby(by=['sample_id', 'run_id'], as_index=False).mean()
        df_fastqc=pd.concat([df_fastqc, df_run])

    # Coverage Data
    coverage_files = [os.path.join(input3, fn) for fn in os.listdir(input3)]
    print("Processing sample coverage")
    df_cov=pd.DataFrame(data=None, columns=['sample', 'run_id', 'X38x' , 'mean'])
    for fl in coverage_files:
        cov_data=pd.read_csv(fl, delimiter='\t')
        cov_data['run_id']=fl.split('_')[4].split('.')[0]
        cov_data = cov_data.loc[:, ['sample', 'run_id', 'X38x' , 'mean']]
        df_cov=pd.concat([df_cov, cov_data])

    df_cov.columns = ['sample_id', 'run_id', 'cov38x' , 'mean_cov']
    df_4_info['sample_id']=df_4_info['sample_id'].astype(str)
    df_fastqc['sample_id']=df_fastqc['sample_id'].astype(str)
    df_cov['sample_id']=df_cov['sample_id'].astype(str)
    df_4_info['run_id']=df_4_info['run_id'].astype(str)
    df_fastqc['run_id']=df_fastqc['run_id'].astype(str)
    df_cov['run_id']=df_cov['run_id'].astype(str)

    df_merged_1=df_4_info.merge(df_fastqc, how='inner', on=['sample_id', 'run_id'])
    df_merged_2=df_merged_1.merge(df_cov, how='inner', on=['sample_id', 'run_id'])

    print(df_merged_2)
    # lookup table
    sql_samples = "select sample_id from sample_dimension"
    df_samplesdim=pd.read_sql(sql_samples, con=engine)
    df_samplesdim['sample_id']=df_samplesdim['sample_id'].astype(str)

    df=df_merged_2.merge(df_samplesdim, how='inner', on='sample_id')

    # lookup table
    sql_runs = "select run_id from run_dimension"
    df_runsdim=pd.read_sql(sql_runs, con=engine)
    df_runsdim['run_id']=df_runsdim['run_id'].astype(str)

    df_def=df.merge(df_runsdim, how='inner', on='run_id')
    # change samples dim name
    with engine.connect().execution_options(autocommit=True) as conn:
        df_def.to_sql('bioinfo_analysis_qc', con=conn, if_exists='append', index= False)
        print("Records created successfully in Sample Analysis Fact Table.")
    

