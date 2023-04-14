import pandas as pd
import glob
import sys
import os
import datacompy

odate = sys.argv[1]
table_nm = sys.argv[2]

conv_file_nm = "/home/ec2-user/spark_conv_recon/lkp/"+odate+"_"+table_nm+"_clean_file.csv/*.csv"
source_file_nm = "/home/ec2-user/spark_conv_recon/lkp/"+odate+"_"+table_nm+"_source_output.csv"
l = [pd.read_csv(filename,sep="\001") for filename in glob.glob(conv_file_nm)]
conv_df = pd.concat(l, axis=0)

source_df = pd.read_csv(source_file_nm, sep=",", header='infer')

print("-----------comparing two files-------------------")
print(source_df)
print(conv_df)

print("--------ignoring not neeeded column--------------")
source_df=source_df.drop(['publn_id'], axis=1)
conv_df=conv_df.drop(['publn_id'], axis=1)

compare = datacompy.Compare(
            source_df,
            conv_df,
            join_columns=['risk_id','risk_dmn_cd'],
            df2_name="expected",
            df1_name="actual",
)

report = compare.report()

with open(f"/home/ec2-user/spark_conv_recon/lkp/recon.report", "w") as fp:
    fp.write(report)

is_match = compare.matches()

exp_pk_sets = compare.df2_unq_rows[compare.join_columns].to_records(index=False)
act_pk_sets = compare.df1_unq_rows[compare.join_columns].to_records(index=False)
num_mismatches = (compare.df2_unq_rows.shape[0] + compare.df1_unq_rows.shape[0])
target_only = compare.df2_unq_rows.shape[0]
source_only = compare.df1_unq_rows.shape[0]
target_mismatched_pks = str(exp_pk_sets)
source_mismatched_pks = str(act_pk_sets)

if not is_match:
    print("Number of mismatches are ", num_mismatches)
    print("Below are the mismatch details in the Spark output with the existing platform output")
    print("Records only in Spark output is ", target_only)
    print("Records only in existing platform output is ", source_only)
    print("Primary Keys only in Spark output are ", target_mismatched_pks)
    print("Primary Keys only in existing platform are ", source_mismatched_pks)
    print("Saved the complete reconciliation report for your reference")
else:
    print("Number of mismatches are ", num_mismatches)
    print("There are no mismatches in Spark output with the existing platform output")
    print("Saved the complete reconciliation report for your reference")

print("--------printing recon summary report---------")
print(report)
