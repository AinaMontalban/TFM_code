import pandas as pd
import numpy as np
import re
import os
import pandas as pd
import numpy as np
import random
from re import sub
import pandas as pd 

def extract_csv_file(file, delim):
    return pd.read_csv(file, delimiter=delim)

def extract_excel_file(file, num):
    return pd.read_excel(file, skiprows=num)

def data_type_conversion(df, column_field, conversion_type):
    if conversion_type == "string":
        df[column_field] = df[column_field].astype(str)
    elif conversion_type == "numeric":
        df[column_field] = df[column_field].astype(int)
    elif conversion_type == "date":
        df[column_field] = df[column_field].astype(date)
    else:
        print("Wrong conversion type. It should be string, numeric or date.")

def trim_sample_id(row):
    if len(row['Sample']) == 14:
        sampleid=row['Sample'][3:len(row['Sample'])-2]
    elif len(row['Sample']) == 13:
        sampleid=row['Sample'][2:len(row['Sample'])-2]
    else:
        sampleid=row['Sample']
    return sampleid