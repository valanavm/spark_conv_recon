#/bin/ksh

ODATE = $1
TABLE_NM = $2
cwd=`realpath $0`
cwd=`dirname $cwd`

#Load the python virtual environment

print "--------converting Mapping sheet to Spark Code----------"
spark-submit --master yarn --deploy-mode client --driver-memory 8g \
--conf "spark.app.id=`uuidgen`" \
--conf spark.pyspark.python=$(PY DRIVER} \
--conf spark.pyspark.driver.python=${PY DRIVER} \
$(cwd)/../python/gen_base_pipeline.py $1 $2

rc=$?

if [$rc != 8 ]; then
        exit $rc

else

        print "----successfully executed the spark code----"
        print "----proceeding for the reconciliation-----"
        python3 ${cwd)/../python/reconciliation.py 20230410 test_risk
fi
