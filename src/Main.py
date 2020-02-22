from configparser import ConfigParser

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import spark_partition_id
from pyspark.sql.session import SparkSession

from com.vitthalmirji.pyspark.xml.import_module import readFromSource
from com.vitthalmirji.pyspark.xml.mapper_module import mapXmlAsHadoopFile, buildQueriesFromXpath


def sanityChecks(config_file: ConfigParser):
    exit_test = False
    for each_section in config_file.sections():
        print(f'[{each_section}]')
        for (each_key, each_val) in config_file.items(each_section):
            if each_val is not None and not str(each_val).strip().__eq__(''):
                print(f'{each_key} = {each_val}')
            else:
                print(
                    f'ERROR: SparkDriver.properties file - Section {each_section} has {each_key} left blank, please fill in')


def getSparkSessionObject(appconfig: ConfigParser):
    if str(appconfig['Spark']['EnableHiveSupport']).__eq__('true'):
        return SparkSession.builder.enableHiveSupport().getOrCreate()

    return SparkSession.builder.getOrCreate()


def main():
    # Read SparkDriver.properties as ConfigParser() object
    appconfig = ConfigParser()
    appconfig.read(filenames='SparkDriver.properties')
    print(f'Properties file sections: {appconfig.sections()}')
    sanityChecks(config_file=appconfig)

    # Create Spark Session object
    spark = getSparkSessionObject(appconfig=appconfig)

    # Read XPATH Mappings given in Csv
    xpaths_mapping_df: DataFrame = readFromSource(spark=spark, opt={
        'location': f'{str(appconfig["Xml"]["XpathMappingsCsvFilePath"]).strip()}',
        'filetype': 'csv', 'header': True, 'inferSchema': True})

    # Using mapper module build spark sql queries from Xpath Mappings Csv
    spark_sql_query = buildQueriesFromXpath(df=xpaths_mapping_df)

    # You can also use other way by creating External Table building DDL using function buildDdlFromXpath
    # spark_sql_ddl = buildDdlFromXpath(appconfig=appconfig, df=xpaths_mapping_df)

    # Read actual huge multi-line XML file as XmlInputFormat determine row tag, eliminate new lines so that every
    # start and end tag comes in one line in a Dataframe
    xml_df: DataFrame = mapXmlAsHadoopFile(location=str(appconfig['Xml']['FileLocation']))

    # Determine revised partitions for Dataframe as XML data is skew
    total_records = xml_df.count()
    total_paritions = xml_df.rdd.getNumPartitions()
    total_records_per_partition = xml_df.groupBy(spark_partition_id()).count().select('count').collect()
    total_executors = int(spark.conf.get("spark.executor.instances", default="12").strip())
    total_cores = int(spark.conf.get("spark.executor.cores", default="3").strip())
    total_paritions_revised = total_cores * total_executors

    print(f"total_records = {total_records}")
    print(f"total_paritions = {total_paritions}")
    print(f"total_records_per_partition = {total_records_per_partition}")
    print(f"total_executors = {total_executors}")
    print(f"total_cores = {total_cores}")
    print(f"total_paritions_revised = {total_paritions_revised}")

    xml_df = xml_df.repartition(total_paritions_revised)

    # Execute the query in spark sql
    writedf: DataFrame = spark.sql(spark_sql_query['query'])

    # You can also use ddl and execute as spark SQL
    # spark.sql(spark_sql_ddl['ddl'])
    # spark.sql('LOAD DATA INPATH f"{str(appconfig['Xml']['FileLocation'])}" INTO xmltable')

    # Write data
    writedf.write.mode('overwrite').parquet(path=str(appconfig['Xml']['TargetWritePath']))


if __name__ == '__main__':
    main()
