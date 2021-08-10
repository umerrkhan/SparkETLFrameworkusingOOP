import os, sys, json
from pyspark.sql import SparkSession
from typing import Optional
from pyspark.sql.dataframe import DataFrame

project_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(1, project_dir)
from Frameworks import etlFramework


def main(project_dir) -> None:

    sparkConfigfile = f"{project_dir}/Config/sparkSettings.json"
    sc = getSparkSession(sparkConfigfile)

    sales_landing_dir = "/SPARK_SAN/SOURCE_DATA/001"
    salesFileList = listofloadingfiles(sales_landing_dir, "json")
    print(salesFileList)
    df = createDataFrame(sc, salesFileList, "json")
    showSampleDFValues(df)




def listofloadingfiles(path: str, pattern:Optional[str] = "csv" ) -> str:
    return etlFramework.ETL_Framework(config={}).listofloadingfiles(path, pattern)


def getSparkSession(configfile: str):
    return etlFramework.ETL_Framework(config={}).getSparkSession(configfile, False)


def createDataFrame(sc: SparkSession, files: list, filetype: Optional[str] = "csv"):
    print("File Type -->" ,filetype )
    return etlFramework.ETL_Framework(config={}).createDataFrame(sc, files, filetype)


def showSampleDFValues(df: DataFrame):
    etlFramework.ETL_Framework(config={}).showSampleDFValues(df)



if __name__ == '__main__':
    main(project_dir)

