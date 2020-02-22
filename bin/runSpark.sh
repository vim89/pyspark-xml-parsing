#!/bin/bash

export PYTHONPATH="<Use Python 3.x>:$PYTHONPATH"
export PYTHONPATH="/usr/hdp/current/spark2-client/python/lib/py4j-0.10.7-src.zip:$PYTHONPATH"
export PYTHONPATH="../lib/pyfiles.zip:$PYTHONPATH"
export PYSPARK_PYTHON="<Use Python 3.x>"
export HADOOP_CONF_DIR="/usr/hdp/current/hadoop-yarn-client/etc/hadoop"
export HADOOP_HOME="/usr/hdp/current/hadoop-client"
export HADOOP_MAPRED_HOME="$HADOOP_HOME/../hadoop-mapreduce-client"
export HADOOP_HDFS_HOME="$HADOOP_HOME/../hadoop-hdfs-client"
export YARN_HOME="$HADOOP_HOME/../hadoop-yarn-client"
export HADOOP_COMMON_LIB_NATIVE_DIR="$HADOOP_HOME/lib/native"
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_CONF_DIR="$HADOOP_HOME/conf"
export SPARK_HOME="/usr/hdp/current/spark2-client"
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin

START_TS_EPOCH=$(date +%s%N | cut -b1-13)

high_demand_flag=$1

LOG_STAMP=$(date +%F-%H.%M.%S)
GLOBAL_LOG_DIR="../logs/"
mkdir -p ${GLOBAL_LOG_DIR}
LOG_FILE=${GLOBAL_LOG_DIR}"xml-spark-yarn-${LOG_STAMP}.log"

exec &> >(while read -r line; do printf '%s %s\n' "$(date --rfc-3339=seconds): INFO - $line"; done | tee -a ${LOG_FILE} ) #2>&1

# Performance by file size
if [[ "${high_demand_flag}" == "true" ]]; then
  driver_cores=2
  driver_memory="15G"
  num_executor=4
  executor_cores=11
  executor_memory="35G"
else
  driver_cores=1
  driver_memory="10G"
  num_executor=12
  executor_cores=3
  executor_memory="30G"
fi

JARS="../lib/jars/spark-xml_2.11-0.8.0.jar"
JARS_CP=$(echo $JARS | sed 's/\,/\:/g')
FILES="/usr/hdp/current/spark2-client/conf/hive-site.xml,../src/resources/SparkDriver.properties"

cd ../src

spark-submit \
--conf "spark.executor.extraLibraryPath=${JARS_CP}" \
--jars "${JARS}" \
--driver-class-path "${JARS_CP}" \
--master yarn \
--deploy-mode cluster \
--driver-cores ${driver_cores} \
--driver-memory ${driver_memory} \
--num-executors ${num_executor} \
--executor-cores ${executor_cores} \
--executor-memory  ${executor_memory} \
--files "${FILES}" \
--properties-file "../src/resources/SparkExecutor.properties" \
--py-files "../lib/pyfiles.zip" \
Main.py

sparkSuccess=$?

echo "Collecting logs..."
yarnid=$(grep "YarnClientImpl: Submitted" "${LOG_FILE}" | cut -d' ' -f11 | head -1)
yarn logs -applicationId ${yarnid} -log_files stdout >> "${LOG_FILE}"
yarn logs -applicationId ${yarnid} -log_files stderr >> "${LOG_FILE}"

if [[ ${sparkSuccess} -ne 0 ]]; then
  echo "Execution failed check stderr & stdout logs in Yarn Application ${yarnid}"
  exit 1
fi

# Performance
END_TS_EPOCH=$(date +%s%N | cut -b1-13)
perfTime=$((${END_TS_EPOCH} - ${START_TS_EPOCH}))
echo "Successful Total Runtime = ${perfTime}"

#Purge log files older than 7 days
timetolive=7
cd ../logs/
echo "Purging log files older than ${timetolive} days"
find ./ -maxdepth 1 -type f -name '*.log' -mtime +${timetolive} | xargs rm -f
cd ../

# Clean exit
exit 0
