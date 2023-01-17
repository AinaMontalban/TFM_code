from connection import engine

# CREATE SAMPLE DIMENSION
sample_dim="""
CREATE TABLE sample_dimension (
sample_id VARCHAR(9) NOT NULL PRIMARY KEY,                        
sex VARCHAR(5)  NOT NULL, 
diagnosis_code VARCHAR(20)  NOT NULL,
diagnosis_name VARCHAR(20)  NOT NULL,
cdb_service_id VARCHAR(20)  NOT NULL 
) ;
"""
engine.execute(sample_dim)
print("Sample Dimension created successfully")


# CREATE PANEL DIMENSION
panel_dim="""
CREATE TABLE panel_dimension (
panel_id VARCHAR(100) NOT NULL PRIMARY KEY,
panel_type VARCHAR(100),                         
brand VARCHAR(100)                     
);
"""
engine.execute(panel_dim)
print("Panel Dimension created successfully")


# CREATE TIME DIMENSION
time_dim="""
--- Temporal Dimension
create table day_dimension(
   day_id DATE not null primary key,
   month_name VARCHAR(10) not null,
   month_number INTEGER not null,
   year INTEGER not null
) ;
-- create table for datedimension
create table time_dimension(
   time_id timestamp not null primary key,
   day_id DATE not null REFERENCES day_dimension(day_id)
) ;
"""
engine.execute(time_dim)
print("Time Dimension created successfully")

# CREATE Instr DIMENSION
model_dim="""
-- Model dimension
CREATE TABLE model_dimension (
model_id VARCHAR(50) NOT NULL PRIMARY KEY,    
type  VARCHAR(50),                                        
brand VARCHAR(50)
);
"""
engine.execute(model_dim)
print("Instrument Model Dimension created successfully")

# CREATE TIME DIMENSION
turn_dim="""
CREATE TABLE instrument_model_dimension (
model_id VARCHAR(50) NOT NULL PRIMARY KEY,    
brand VARCHAR(50)
);
"""
engine.execute(turn_dim)
print("Turn Dimension created successfully")


# CREATE Run DIMENSION
run_dim="""
CREATE TABLE run_dimension (     
run_id VARCHAR(30) NOT NULL PRIMARY KEY,     
protocol_id VARCHAR(10),    
technician VARCHAR(20)
) ;

"""
engine.execute(run_dim)
print("Run Dimension created successfully")




instr_dim="""
CREATE TABLE instrument_dimension (
instrument_id VARCHAR(50) NOT NULL PRIMARY KEY,    
instrument_name  VARCHAR(50) NOT NULL,                                         
model_id VARCHAR(50) NOT NULL REFERENCES instrument_model_dimension(model_id)
);
"""
engine.execute(instr_dim)
print("Instrument Dimension created successfully")



reagent_dim="""
CREATE TABLE reagent_dimension (
reagent_id VARCHAR(50) NOT NULL PRIMARY KEY,
model_id VARCHAR(50) NOT NULL REFERENCES instrument_model_dimension(model_id),
kit VARCHAR(50) NOT NULL,                                   
num_cycles VARCHAR(50) NOT NULL                                   
 ) ;
"""
engine.execute(reagent_dim)
print("Reagent Dimension created successfully")

extraction_qc="""
CREATE TABLE extraction_qc (
sample_id VARCHAR(9) NOT NULL REFERENCES sample_dimension(sample_id), 
day_id date NOT NULL REFERENCES day_dimension(day_id),
turn_id INTEGER NOT NULL REFERENCES turn_dimension(turn_id),
concentration FLOAT NOT NULL,
a260_a280_ratio FLOAT,
a260_a230_ratio FLOAT) ;
"""
engine.execute(extraction_qc)
print("extraction_qc Fact created successfully")

library_qc="""
CREATE TABLE library_qc (
sample_id VARCHAR(9) NOT NULL REFERENCES sample_dimension(sample_id), 
day_id date NOT NULL REFERENCES day_dimension(day_id),
run_id VARCHAR(10) NOT NULL REFERENCES run_dimension(run_id),
turn_id INTEGER NOT NULL REFERENCES turn_dimension(turn_id),
concentration FLOAT,
fragment_size FLOAT);
"""
engine.execute(library_qc)
print("library_qc Fact created successfully")


sequencing_qc="""
CREATE TABLE sequencing_qc (
run_id VARCHAR(10) REFERENCES run_dimension(run_id), 
time_id timestamp NOT NULL REFERENCES time_dimension(time_id),
instrument_id VARCHAR(50) NOT NULL REFERENCES instrument_dimension(instrument_id), 
reagent_id VARCHAR(50) NOT NULL REFERENCES reagent_dimension(reagent_id),                                            
num_cycles FLOAT,
cluster_density FLOAT,
cluster_pf FLOAT,
pct_gt_30 FLOAT,
yieldg FLOAT,
pct_aligned FLOAT
) ;
"""
engine.execute(sequencing_qc)
print("sequencing_qc Fact created successfully")


bioinfo_analysis_qc="""
CREATE TABLE bioinfo_analysis_qc (
sample_id VARCHAR(9) NOT NULL REFERENCES sample_dimension(sample_id), 
run_id VARCHAR(10) NOT NULL REFERENCES run_dimension(run_id),
time_id timestamp NOT NULL REFERENCES time_dimension(time_id),
panel_id VARCHAR(100) NOT NULL REFERENCES panel_dimension(panel_id),
total_sequences FLOAT,
read_length FLOAT,
pct_gc FLOAT,
pct_duplicates FLOAT,
pct_failed FLOAT,
cov38x FLOAT,
mean_cov FLOAT
); 
"""
engine.execute(bioinfo_analysis_qc)
print("bioinfo_analysis_qc Fact created successfully")







