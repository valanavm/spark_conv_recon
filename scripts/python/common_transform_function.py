from string import Template
import os
import csv
import sys
from collections import OrderedDict
import json
from datetime import datetime
import logging
import traceback
import time
import pyspark

#spark modules
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
from pyspark.sql.functions import *

#Common transformations class
class common_transform_functions:
    ''' This class is used to define all the FIELD-LEVEL-TRANSFORMATION methods which are used in the generic pipe line '''

    @staticmethod
    def persist_func(data_frame, mode):
        return data_frame.persist(pyspark.StorageLevel.MEMORY_AND_DISK)

    @staticmethod
    def unpersist_func(data_frame):
        return data_frame.unpersist()

    @staticmethod
    def readFileToDataFrame(sqlContext, readFilePath, readConfig, readFormat):
        rf = readFormat.strip().lower()
        rc = readConfig
        seprator = rc.get("sep", None) if rc.get("sep", None) != None else rc.get("delimiter", None)
        _data_frame = sqlContext.read.csv(
            readFilePath,
            sep=seprator,
            quote=rc.get("quote", None),
            escape=rc.get("escape", None),
            header=rc.get("header", "true"),
            ignoreTrailingWhiteSpace="true",
            ignoreLeadingWhiteSpace="true",
            nullValue="",
            nanValue="",
            mode="FAILFAST",
            encoding=rc.get("encoding", "UTF-8"),
        )
        return _data_frame


    @staticmethod
    def writeFileFromDataFrame(writeDataFrame, writeFilePath, writeConfig, writeFormat):
        wf = writeFormat.strip().lower()
        if all([wf != 'csv',wf != 'csv_2']):
            raise ValueError('___Error: WriteFormat Error - Acceptable write format only CSV ___')
        wc = writeConfig
        seprator = wc.get("sep", None) if wc.get("sep", None) != None else wc.get("delimiter", None)
        if writeFormat.strip() == 'csv':
            writeDataFrame.repartition(1).write.csv(
                writeFilePath,
                mode="overwrite",
                sep=seprator,
                quote=wc.get("quote", None),
                escape=wc.get("escape", None),
                header="true",
                escapeQuotes=wc.get("escapeQuotes", None),
                nullValue="",
            )
        else:
            print('____write File From Data Frame Fail_____')


    @staticmethod
    def common_dt_func(field_nm, field_value):
        """
        publn_id and snap_dt  conversion

        """
        if field_nm == "publn_id":
            return datetime.now().strftime('%Y%m%d%H%M%S')
        elif (field_nm == 'snap_dt') | (field_nm == 'data_dt'):
            return datetime.strptime(field_value, '%Y%m%d').strftime('%Y-%m-%d')
        else:
            return ''


    @staticmethod
    def gen_str_prefix(in_df, row_dict):
        in_df.show()
        out_df= in_df.withColumn(
                row_dict['target_column'],
                trim(col(row_dict['source_column']).substr(1, int(row_dict['target_data_type_len']))),
        )
        return out_df


    @staticmethod
    def gen_trim(in_df, row_dict):
        out_df = in_df.withColumn(row_dict['target_column'], trim(col(row_dict['source column'])))
        return out_df

    @staticmethod
    def gen_convert_flag_func(in_df, row_dict):
        out_df = in_df.withColumn(
                row_dict['target_column'], when(trim(upper(col(row_dict['source_column']))) == "YES", "Y").otherwise("N")
        )
        return out_df


    @staticmethod
    def gen_code_lookup(in_df, row_dict):
        out_df = in_df.withColumn(
                row_dict['target_column'], when(trim(upper(col(row_dict['source_column']))) < lit(10000), "100").otherwise("200")
        )
        return out_df

    @staticmethod
    def gen_normalize(in_df, row_dict):
        out_df = in_df.withColumn(
                    row_dict['target_column'], explode(split(row_dict['source_column'], '\|'))
        )
        return out_df

    @staticmethod
    def gen_timestamp_date(in_df, row_dict):
        out_df = in_df.withColumn(
                row_dict['target_column'], trim(col(row_dict['source_column'])).cast(DateType()).cast(StringType())
        )
        return out_df
