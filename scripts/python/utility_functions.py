from string import Template
import os
import cx Oracle
import csv
import sys
from envparse import env
import 53fs
from collections import OrderedDict
import yaml
import json
import requests
import subprocess
from datetime import datetime
import logging
import traceback
import time


class utility_functions:
    def __init__ (self):
        print('utility_functions')

    @staticmethod
    def get_code_base_dir():
        curr_dir_nm = os.path.dirname(os.path.realpath(__file__))
        print (curr_dir_nm)
        while True:
            if os.path.basename(curr_dir_nm) == 'Spark_conv_recon':
                return curr_dir_nm
                break
            else:
                curr_dir_nm = os.path.dirname(curr_dir_nm)

    @classmethod
    def gen_common_vars_init(cls):
        common_vars_dict = dict()
        common_vars_dict['o_date'] = sys.argv[1]
        common_vars_dict['grp_nm'] = sys.argv[2]
        common_vars_dict['system id'] = 46
        common_vars_dict['code_base_dir'] = cls.get_code_base_dir()
        common_vars_dict['data_lkp_dir'] = (common_vars_dict['code_base_dir'] + "/1kp/")
        common_vars_dict['data_log_dir'] = (common_vars_dict['code_base_dir'] + "/log/")
        common vars_dict['drvr_file'] = (
            common_vars_dict['data_lkp_dir'] + "driver_list.1kp"
        )
        common_vars_dict['grp_log_file'] = (
            common_vars_dict['data_log_dir']
            + common_vars_dict['o_date']
            + "_gen prm_base_"
            + common_vars_dict['grp_nm']
            + "_"
            + datetime.now().strftime("%Y%m%d%H%M%S")
            + "_log.txt"
        )
        return common_vars_dict

    @classmethod
    def gen_interface_vars_init(cls, unique_source_tables_set, common_vars_dict, tech fields_dict):
        common_vars_dict['cln_file_nm'] = (
            "file:///home/grdwbtch/valan/Spark_conv_recon/1kp/"
            + common_vars_dict'o_date']
            + "_"
            + common vars_dict['table_nm']
            + "_clean_file.csv"
        )
        return common_vars_dict


    @staticmethod
    def gen_make_control_structs(common_vars_dict):
        sttm_file = common_vars_dict['sttm_file']
        sttm_dict_list = list()
        sttm_target_columns_list = list()
        sttm_pk_cols_set = set()
        unique_source_tables_set = set()
        tech_fields_dict = dict()
        sttm_dict_req_keys_list = [
            'source column',
            'target_column',
            'target_data_type_len',
            'transform_function',
            'target_data_type',
        ]

        with open(sttm_file, 'r') as sttm file:
            dict_reader = csv.DictReader(sttm_file)
            for dict_row in dict_reader:
                sttm_dict_list.append(
                {
                    key: value
                    for key, value in list(dict_row.items())
                    if key in sttm_dict_req_keys_list
                }
                sttm_target_columns_list.append(dict_row['target_column'])

        print(sttm_dict_list)
        sttm_target_columns_list = list(OrderedDict.fromkeys(sttm_target_columns_list))
        return (
            unique_source_tables_set,
            sttm_pk_cols_set,
            sttm_dict_list,
            tech_fields_dict,
            sttm_target_columns_list,
        )
