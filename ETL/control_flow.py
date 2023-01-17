
import yaml
from multiprocessing import Process
from ETL.data_flow.load_dimension_tables import *
from ETL.data_flow.load_fact_tables import *

def process_data(sources, engine):

    # Read YML config file
    with open(sources, 'r') as file:
        params = yaml.safe_load(file)

    '''
    def runInParallel(*dims):
        proc = []
        for fn in dims:
            p = Process(target=fn)
            p.start()
            proc.append(p)
        for p in proc:
            p.join()

    runInParallel(etl_PanelDimension(params['panel_dimension'], engine),
                     etl_SampleDimension(params['sample_dimension'], engine),
                      etl_instr_reagent(params['instrument_dimension'], params['reagent_dimension'],engine), 
                      etl_RunDimension(params['run_dimension'], engine),
                       etl_Time_Date_Dimension(params['time_dimension'], engine), 
                       etl_TurnDimension(params['turn_dimension'], engine))
    etl_extraction_qc(params['extraction_qc'], engine)
    etl_library_qc(params['library_qc'], engine)
    etl_sequencing_qc(params['sequencing_qc'], engine)
    '''
    etl_bioinfo_analysis_qc(params['bioinfo_analysis_qc'], engine)
