#/bin/ksh

JOB_START_TIME=$(date "+%Y%m%d%H%M%S")
ODATE=$1
TABLE_NM=$2
cwd=`realpath $0`
cwd=`dirname $cwd`

export log_file="/home/ec2-user/spark_conv_recon/log/${ODATE}_${TABLE_NM}_execution_${JOB_START_TIME}.log"
#Load the python virtual environment
py_path=$code_base_dir/scripts/python
export PYTHONPATH=$PYTHONPATH:$py_path

export PY_PATH=/usr/bin/
export PY_DRIVER=/usr/bin/python3


echo "--------converting Mapping sheet to Spark Code for $TABLE_NM ----------" >> $log_file
spark-submit --master local --deploy-mode client --driver-memory 8g \
--conf spark.pyspark.python=${PY_DRIVER} \
--conf spark.pyspark.driver.python=${PY_DRIVER} \
${cwd}/../python/gen_base_pipeline.py $ODATE $TABLE_NM >> $log_file

rc=$?

if [ $rc != 0 ]; then
        exit $rc

else

        echo "----successfully executed the spark code for $TABLE_NM ----" >> $log_file
        echo "----proceeding for the reconciliation for $TABLE_NM -------" >> $log_file
        /usr/bin/python3 ${cwd}/../python/reconciliation.py $ODATE $TABLE_NM >> $log_file
        rc=$?
        if [ $rc != 0 ]; then
                exit $rc
        else
                echo "----reconciliation process complete successfully for $TABLE_NM ------" >> $log_file
        fi
fi
