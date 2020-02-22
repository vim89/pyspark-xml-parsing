from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def readFromSource(spark: SparkSession, opt={}) -> DataFrame:
    try:
        if str(opt['filetype']).lower().__eq__('text'):
            return spark.read.text(paths=opt['location'], wholetext=opt['wholetext']).toDF('line')
        elif str(opt['filetype']).lower().__eq__('csv'):
            return spark.read.csv(path=opt['location'], header=opt['header'], inferSchema=opt['inferSchema'])
    except Exception as ex:
        print(f"Error reading file {ex}")
