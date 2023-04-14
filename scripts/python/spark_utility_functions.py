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

#spark modules
from pyspark import SparkConf, SparkContext
From pyspark.sql import SQLContext, Row
From pyspark.sql.types import *
from pyspark.sql.functions import *

#Import the transformation functions
from common transform_functions import common_transform functions as xfm
from utility_functions import utility functions as utl

class spark_utility_functions(utl):
        def __init__(self):
                super(spark_utility)functions, self).__init__()
        @staticmethod
        def gen_spark_init(table_nm code_base_dir, log4j log level):
                #set spark and SQL context objects
        py_files_list= [
                code_base_dir + "/scripts/python/utility functions.py",
                code_base_dir + "/scripts/python/spark_utility_functions.py",
                code_base_dir + "/scripts/python/common_transform functions.py",
        ]
        conf=SparkConf().setMaster("local").setAppName(table_nm)
        sc =  SparkContext(conf=conf, pyFiles-py_files_list)
        sqlContext = SQLContext(sc)

        # set up log4j logger and log level
        log4j = sc. jvm.org.apache.log4j
        logger = log4j.LogManager.getLogger(_name_)
        sc.setLogLevel (log4j_log_level.upper())
        return sc, sqlContext

@classmethod
def gen_apply_sttm_xfms (
        cls,
        src_df,
        unique_Source_tables_list,
        sttm_dict_list,
        sttm_target_columns_list,
        common_var_dict,
        sqlContext,
):
        publn_id = xfm.common_dt_func('publn_id','')
        load_dt = common_var_dict['o_odate']
        system_id = common_var_dict['system_id']
        target_tbl_nm = common_var_dict['table_nm']

        print(target_tbl_nm)
        print(sttm_dict_list)
        for row_dict in sttm_dict_list:
                if row_dict['transform_function'][:4] == "gen_":
                        Logging.info(
                                "'%s' as been transformed to '%s' using '%s' function",
                row_dict['source_column'],
                row_dict['target_column'],
                row_dict['transform_function'],
             )

            print(row_dict)
            src_df eval(
                "xfm."
                +row_dict['transform_function'],
                + "(src_df,row_dict)"
            )
        # if block for processing all the etl_system_generated fields as mentioned in STTM
        elif row_dict['transform_function'] == "etl_system_generated":
            logging.info(
                "%s' field has been transformed to '%s' using '%s' function",
                row_dict['source_column'],
                row_dict['target_column'],
                row_dict['transform_function'],
            )
            src_df = src_df.withColumn (
                row_dict['target_column'], lit(eval(row_dict['target_column'].strip().lower()))
            )
        # if block for processing all the hard_coded fields as mentioned in STTM
        elif row dict['transform_function'] "hard_coded":
            logging.info( "'%s' field has been transformed to '%s' using '%s' function",
                row_dict['source_column'],
                row_dict['target_column'],
                row_dict['transform_function'],
            )
            src_df = src_df.withColumn(
                row_dict['target_column'], lit(row_dict['source_column'].strip())
            )
        # if block for processing all the custom fields as mentioned in STTM
        elif row_dict['transofrm_function'] == "custom":
            logging.info(
                "'%s' field has been transformed to '%s' using '%s' function",
                row_dict['source_column'],
                row_dict['target_column'],
                row_dict['transform_function'],
            )
            import importlib

            module_nm = importlib.import_module(target_tbl_nm + "_custom_xfm")
            class_nm = getattr(module_nm, target_tbl_nm)
            func_nm = getattr(class_nm, row_dict['target_column'].lower())
            src_df = func_nm(src_df, common_vars_dict, row_dict, sqlContext)
        # if block for processing all the fields which have unhandled transformation functions mentioned in SSTM
        else:
            logging.error(
                "'%s' field has been transformed to '%s' using '%s' function -- but no such function available",
                row_dict['source_column'],
                row_dict['target_column'],
                row_dict['tramsform_function'],
            )
            src_df = src_df.withColumn(
               row_dict['target_column'], trim(col(row_dict['source_column']))
           )

    src_df = src_df.select(sttm_target_columns_list)
    return src_df


    @classmethod
    def gen_clean(
         cls,
         sttm_dict_list,
         sttm_target_columns_list,
         unique_source_tables_set,
         sttm_pk_cols_set,
         common_vars_dict,
         sqlContext,
    ):

        logging.info("cleaning stage started")

        read_config = {"sep": ',', "quote": "\"", "escape": '"'}
        common_vars_dict['source_file_nm'] = "file:///.../lkp/"+common_var_dict['o_odate']+"_"+common_var_dict['table_nm']+"_source_file_nm.csv"
        src_df = xfm.readFileToDataFrame(
            sqlContext, common_vars_dict[base_tbl_nm + "_ora_unld_file_nm"], read_config, 'csv'
        )
        xfm_df = cls.gen_apply_sttm_xfms(
            src_df,
            unique_source_tables_set,
            sttm_dict_list,
            sttm_target_columns_list,
            common_vars_dict,
            sqlContext,
        )

        cln_df = cln_df.fillna('')


        write_config = {"mode": "overwrite", "sep":'\001',"quote": "\002", "escape": ""}
        xfm.writeFileFromDataFrame(cln_df, common_vars_dict['cln_file_nm'], write_config, 'csv')

        return cln_df


    @classmethod
    def gen_prm_base_pipeline(cls, common_vars_dict):
        logging.info(
            "<<<<<<<<<<<<<<< Processing of '%s' table has started ..... >>>>>>>>>>>>>>>>>",
            common_vars_dict['table_nm'].upper(),
        )
        # generate the control data structures which will drive the pipeline for a given table
        unique_source_tables_set, sttm_pk_cols_set, sttm_dict_list, tech_fields_dict, sttm_target_columns_list = utl.gen_make_control_structs(
            common_vars_dict
        )
        # Intiializing the variables for the given table
        utl.gem_interface_vars_init(unique_source_tables_set, common_vars_dict, tech_fields_dict)

        #Initializing the variables for the given table
        sc, sqlContext = cls.gen_spark_init(
               common_vars_dict['target_tbl_nm'], common_vars_dict['code_base_dir'],"WARN"
        )

        #cleansing stage
        cln_df = cls.gen_clean(
            sttm_dict_list,
            sttm_target_columns_list,
            unique_source_tables_set,
            sttm_pk_cols_set,
            common_vars_dict,
            sqlContext,
        )

        logging.info(
            "<<<<<<<<<<<<<<< Processing of '%s' table has ended ..... >>>>>>>>>>>>>>>>>",
            common_vars_dict['table_nm'].upper(),
        )
