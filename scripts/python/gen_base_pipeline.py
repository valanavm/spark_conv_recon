import os
import csv
import sys
import logging
import traceback
import time
import pyspark
from datetime import datetime

from utility_functions import utility_functions as putl
from spark_utility_functions import spark_utility_functions as sutl


#Commn variables setup
common_vars_dict = putl.gen_common_vars_init()

print("-----Assigned environment below variable-----")
print(common_vars_dict)

#python logger initialization
log_frmt = "%(levelname)s: %(asctime)s -- %(funcName)s - (message)s"
logging.basicConfig(filename=common_vars_dict['grp_log_file'], format=log_frmt)
logging.getLogger().setLevel(logging.INFO)
with open(common_vars_dict['drvr_file'], 'r') as file:
    dict_reader = csv.DictReader(file, delimiter='|')
    for dict_row in dict_reader:
        logging.info("FULL RUN for %s initiated", dict_row['TABLE_NM'])
        common_vars_dict['table_nm'] = dict_row['TABLE_NM']
        common_vars_dict['sttm_file'] = (
            common_vars_dict['code_base_dir']
            +"/map_files/"
            +dict_row['TABLE_NM']
            +".csv"
        )
        print(common_vars_dict)
        try:
            sutl.gen_base_pipeline(common_vars_dict)
        except Exception as e:
            logging.error(traceback.format_exc())
            failed_interface_list.append(common_vars_dict['target_nm'])
