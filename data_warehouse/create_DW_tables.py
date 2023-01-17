from connection import engine

# CREATE SAMPLE DIMENSION
sample_dim="""
CREATE TABLE sample_dimension (
id SERIAL NOT NULL PRIMARY KEY,   
sample_id VARCHAR(9) NOT NULL ,                            
sex VARCHAR(5), 
diagnosis VARCHAR(20),
service VARCHAR(20) 
) ;
"""
engine.execute(sample_dim)
print("Sample Dimension created successfully")


# CREATE PANEL DIMENSION
panel_dim="""
CREATE TABLE panel_dimension (
panel_id VARCHAR(100) NOT NULL PRIMARY KEY,
library VARCHAR(100),       
category VARCHAR(100),                         
brand VARCHAR(100)                     
);
"""
engine.execute(panel_dim)
print("Panel Dimension created successfully")


# CREATE TIME DIMENSION
time_dim="""
create table time_dimension(
   date_id date not null primary key,
   day_number int not null,
   day_name VARCHAR(10) not null,
   month_name VARCHAR(10) not null,
   month_number int not null,
   year int not null
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
CREATE TABLE turn_dimension (
turn_id INTEGER NOT NULL PRIMARY KEY                                    
) ;
"""
engine.execute(turn_dim)
print("Turn Dimension created successfully")


# CREATE Run DIMENSION
run_dim="""
CREATE TABLE run_dimension (     
id SERIAL NOT NULL PRIMARY KEY,   
run_id VARCHAR(30) NOT NULL,  
protocol VARCHAR(10),    
technician VARCHAR(20)
) ;
"""
engine.execute(run_dim)
print("Run Dimension created successfully")




instr_dim="""
CREATE TABLE instrument_dimension (
serial_number VARCHAR(50) NOT NULL PRIMARY KEY,                                            
model_id VARCHAR(50) NOT NULL REFERENCES model_dimension(model_id)
);
"""
engine.execute(instr_dim)
print("Instrument Dimension created successfully")



reagent_dim="""
CREATE TABLE reagent_dimension (
part_number VARCHAR(50) NOT NULL PRIMARY KEY,
instrument_id VARCHAR(50) NOT NULL REFERENCES model_dimension(model_id),
kit VARCHAR(50),                                   
num_cycles VARCHAR(50),                                   
brand VARCHAR(50)
 ) ;
"""
engine.execute(reagent_dim)
print("Reagent Dimension created successfully")




