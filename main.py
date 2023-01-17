import argparse
import logging
from ETL.control_flow import process_data
from DW.connection import engine
from datetime import datetime
from graphtik import compose, operation


### ARGUMENTS ###
parser =  argparse.ArgumentParser(
description="..."
)

## Data sources for Dimensions Tables YAML file
parser.add_argument(
'-d','--data_sources',
required=True,
type=str, 
help="YAML file specifying the data sources",
metavar='PATH'
)


# argparse.FileType('r')

args =  parser.parse_args()


# Log file for control flow
logging.basicConfig(filename="logs/main.log", level=logging.INFO)

# Main function
def main():
    """ Execute ETL process
    """
    # Control FLow
    logging.info('ETL process started at'+str(datetime.now()))
    process_data(args.data_sources, engine)
    logging.info('ETL process finished at'+str(datetime.now()))


if __name__ == '__main__':
    main()