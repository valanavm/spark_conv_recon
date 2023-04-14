import pandas as pd
import glob
import sys
import os
import datacompy

odate = sys.argv[1]
table_nm = sys.argv[2]

conv_file_nm = "/home/qrdwbtch/valan/Spark_conv_recon/1kp/"+odate+"_"+table_nm+"_clean file.csv/*.csv"
source file nm = "/home/qrdwbtch/valan/Spark_conv_recon/1kp/"+odate+"_"+table_nm+"_source_output.csv"
l = [pd.read_csv(filename) for filename in glob.glob(conv_file_nm)]
conv_df = pd.concat(1, axis=0)

source_df = pd.read_csv(source_file_nm, sep=",", header='infer')

compare = datacompy.Compare(
            source_df,
            conv_df,
            join columns=['risk id', 'risk_dmn_cd'],
            df2_name="expected",
            df1_name="actual",
)

report = compare.report()

with open(f"/home/qrdwbtch/valan/Spark_conv_recon/1kp/recon.report", "w") as fp:
    fp.write(report)

is_match = compare.matches()

exp_pk_sets = compare.df2_unq_rows[compare.join_columns].to_records(index=False)
act_pk_sets = compare.df1_unq_rows[compare.join_columns].to_records(index=False)
num_mismatches = (compare.df2_unq_rows.shape[0] + compare.df1_unq_rows.shape[0])
target_only = compare.df2_unq_rows.shape[0]
source_only = compare.df1_unq_rows.shape[0]
target_mismatched_pks = str(exp_pk_sets)
source_mismatched_pks = str(act_pk_sets)

if not is match:
    print("Number of mismatches are ", num mismatches)
    print("Below are the mismatch details in the Spark output with the existing platform output")
    print("Records only in Spark output is ", num_target_only)
    print("Records only in existing platform output is ", num_source_only)
    print("Primary Keys only in Spark output are ", target_mismatched_pks)
    print("Primary Keys only in existing platform are ", source_mismatched_pks)
    print("Saved the complete reconciliation report for your reference")
    print(report)
else:
    print("Number of mismatches are ", num mismatches)
    print("There are no mismatches in Spark output with the existing platform output")
    print("Saved the complete reconciliation report for your reference")
    print(report)
