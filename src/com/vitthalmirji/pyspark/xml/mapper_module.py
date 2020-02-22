from configparser import ConfigParser

import lxml
from pyspark.broadcast import Broadcast
from pyspark.rdd import RDD
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import udf, col, split, size
from pyspark.sql.session import SparkSession
from pyspark.sql.types import Row, StringType


def validateXpath(df: DataFrame) -> DataFrame:
    def isXpathValid(xpath) -> str:
        try:
            lxml.etree.XPath(str(xpath).strip())
            return 'true'
        except Exception as ex:
            return 'false'

    validate_xpath_udf = udf(isXpathValid, StringType())
    df = df.withColumn('xpathvalidation', validate_xpath_udf('XPATH'))
    df = df.withColumn('xpathleafnode',split(col('xpath'), '/')[size(split(col('xpath'), '/')) - 1])
    return df


def mapXmlAsHadoopFile(spark: SparkSession, appconfig:ConfigParser, location) -> DataFrame:
    xml: RDD = spark.sparkContext.newAPIHadoopFile(
        f'{location}',
        'com.databricks.spark.xml.XmlInputFormat',
        'org.apache.hadoop.io.LongWritable',
        'org.apache.hadoop.io.Text',
        conf={
            'xmlinput.start': f'{str(appconfig["Xml"]["XmlRecordStart"]).strip()}',
            'xmlinput.end': f'{str(appconfig["Xml"]["XmlRecordEnd"]).strip()}',
            'xmlinput.encoding': 'utf-8'
        })
    df: DataFrame

    if str(appconfig['Xml']['EliminateNewLines']).strip().__eq__('true'):
        df = spark.createDataFrame(
            xml.map(lambda x: str(x[1]).strip().replace('\n', '')), StringType()
        ).withColumnRenamed('value', 'line')
    else:
        df = spark.createDataFrame(
            xml.map(lambda x: str(x[1]).strip()), StringType()
        ).withColumnRenamed('value', 'line')

    return df


def buildQueriesFromXpath(spark: SparkSession, appconfig: ConfigParser, df: DataFrame) -> {}:
    def getMapColumnQuery(row: Row) -> str:
        row_dict = row.asDict()
        if _xpath_return_type.__eq__('string'):
            if str(row_dict["xpath"]) is None or str(row_dict["xpath"]).strip().__eq__('') or str(
                    row_dict["xpath"]).strip().__eq__('None') or str(row_dict["xpath"]).strip().__contains__('?'):
                return f'CAST(NULL AS STRING) AS {str(row_dict["column_alias"]).strip().replace(" ", "_")}'
            else:
                return f'CAST(TRIM(CONCAT_WS("{_sep.value}", XPATH(line, "{row_dict["xpath"]}"))) AS STRING) AS {str(row_dict["column_alias"]).strip().replace(" ", "_")}'
        else:
            if str(row_dict["xpath"]) is None or str(row_dict["xpath"]).strip().__eq__('') or str(
                    row_dict["xpath"]).strip().__eq__('None') or str(row_dict["xpath"]).strip().__contains__('?'):
                return f'CAST(NULL AS STRING) AS {str(row_dict["column_alias"]).strip().replace(" ", "_")}'
            else:
                return f'XPATH(line, "{row_dict["xpath"]}") AS {str(row_dict["column_alias"]).strip().replace(" ", "_")}'

    df_rdd: RDD = df.rdd
    _sep = spark.sparkContext.broadcast(str(appconfig['Xml']['XpathMultipleValuesSeparator']).strip())
    _xpath_return_type = spark.sparkContext.broadcast(
        str(appconfig['Xml']['XpathMultipleValuesSeparator']).strip().lower())
    queryhead = f'SELECT '
    querybody = df_rdd.map(lambda _row: getMapColumnQuery(row=_row)).collect()
    querytail = f'FROM xmltable'
    query = f'{queryhead} {",".join(querybody)} {querytail}'
    print(query)
    return {'query': query}


def buildDdlFromXpath(appconfig: ConfigParser, df: DataFrame) -> {}:
    def getColumnsAndSerdeXpath(row: Row) -> {}:
        ddl_tail_serde_property = ""
        ddl_body_column_and_datatype = ""
        row_dict = row.asDict()
        if str(row_dict["column_alias"]) is None or str(row_dict["column_alias"]).strip().__eq__(''):
            print('Skipping empty/null column name')
        elif str(row_dict["xpath"]) is None or str(row_dict["xpath"]).strip().__eq__(''):
            ddl_tail_serde_property = f'"column.xpath.{str(row_dict["column_alias"]).strip().replace(" ", "_")}"="{row_dict["xpath"]}"'
            ddl_body_column_and_datatype = f'{str(row_dict["column_alias"]).strip().replace(" ", "_")} ARRAY<STRING>'
        else:
            ddl_tail_serde_property = f'"column.xpath.{str(row_dict["column_alias"]).strip().replace(" ", "_")}"="/PleaseCorrectTheXpath/@InvalidColumn"'
            ddl_body_column_and_datatype = f'{str(row_dict["column_alias"]).strip().replace(" ", "_")} ARRAY<STRING>'

        return {'column_datatype': ddl_body_column_and_datatype, 'serde_property': ddl_tail_serde_property}

    df_rdd: RDD = df.rdd
    queryhead = f'CREATE EXTERNAL TABLE IF NOT EXISTS xmltable('
    ddl_columns_serde_properties = df_rdd.map(lambda _row: getColumnsAndSerdeXpath(row=_row)).collect()
    querybody = []
    query_tail_properties = []
    for dict_item in ddl_columns_serde_properties:
        querybody.append(dict_item['column_datatype'])
        query_tail_properties.append(dict_item['serde_property'])

    table_location = str(appconfig['Xml']['ExternalTableLocation']).strip()
    xml_record_start_tag = str(appconfig['Xml']['XmlRecordStartTag']).strip()
    xml_record_end_tag = str(appconfig['Xml']['XmlRecordEndTag']).strip()
    querytail = f') ' \
                f'ROW FORMAT SERDE "com.ibm.spss.hive.serde2.xml.XmlSerDe" ' \
                f'WITH SERDEPROPERTIES ({",".join(query_tail_properties)}) ' \
                f'STORED AS INPUTFORMAT "com.ibm.spss.hive.serde2.xml.XmlInputFormat" ' \
                f'OUTPUTFORMAT "org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat" ' \
                f'LOCATION "{table_location}" ' \
                f'TBLPROPERTIES ("xmlinput.start"="{xml_record_start_tag}","xmlinput.end"="{xml_record_end_tag}")'
    ddl = f'{queryhead} {",".join(querybody)} {querytail}'
    print(ddl)
    return {'xml_xpath_ddl': ddl}
