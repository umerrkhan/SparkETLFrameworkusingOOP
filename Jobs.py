import os, sys, json
from pyspark.sql import SparkSession
from typing import Optional
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,LongType,ArrayType,DoubleType,DateType,IntegerType


project_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(1, project_dir)
from Frameworks import etlFramework

def main(project_dir) -> None:

    sparkConfigfile = f"{project_dir}/Config/sparkSettings.json"

    sc = getSparkSession(sparkConfigfile)
    salesSchema = StructType([\
        StructField("ORDERNUMBER", LongType(), True),\
        StructField("PRODUCTCODE", StringType(), True),\
        StructField("attributes", ArrayType(\
            StructType([\
                StructField("MSRP", LongType(),True),\
                StructField("ORDERDATE", StringType(), True),\
                StructField("PRICEEACH", DoubleType(), True),\
                StructField("PRODUCTLINE", StringType(), True),\
                StructField("QUANTITYORDERED", LongType(), True),\
                StructField("SALES", DoubleType(), True),\
                StructField("STATUS", StringType(), True)]\
            ), True)\
                    , True)\
    ])


    salesFileList = listofloadingfiles("/SPARK_SAN/SOURCE_DATA/001", "json")
    df = createDataFrame(sc, salesFileList, "json", "True", salesSchema)
    showSampleDFValues(df)

    peopleFileList = listofloadingfiles("/SPARK_SAN/SOURCE_DATA/002", "json")
    df = createDataFrame(sc, peopleFileList, "json")
    showSampleDFValues(df)


    empFileList = listofloadingfiles("/SPARK_SAN/SOURCE_DATA/000/emp.csv", "csv")
    df = createDataFrame(sc, empFileList, "csv")
    showSampleDFValues(df)

    empFileList = listofloadingfiles("/SPARK_SAN/SOURCE_DATA/000/dept.csv", "csv")
    df = createDataFrame(sc, empFileList, "csv")
    showSampleDFValues(df)
















def listofloadingfiles(path: str, pattern:Optional[str] = "csv" ) -> str:
    return etlFramework.ETL_Framework(config={}).listofloadingfiles(path, pattern)


def getSparkSession(configfile: str):
    return etlFramework.ETL_Framework(config={}).getSparkSession(configfile, False)


def createDataFrame(sc: SparkSession, files: list, filetype: Optional[str] = "csv", Multiline: Optional[str] = None, FileStruct: Optional[StructType] = None):
    print("File Type -->" ,filetype )
    return etlFramework.ETL_Framework(config={}).createDataFrame(sc, files, filetype, Multiline, FileStruct)


def showSampleDFValues(df: DataFrame):
    etlFramework.ETL_Framework(config={}).showSampleDFValues(df)



if __name__ == '__main__':
    main(project_dir)

