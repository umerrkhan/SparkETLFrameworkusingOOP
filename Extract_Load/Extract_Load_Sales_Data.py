import os, sys, json
from pyspark.sql import SparkSession
from typing import Optional
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,LongType,ArrayType,DoubleType,DateType,IntegerType
import pyspark.sql.functions as F


project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
print("project_dir -------> ",project_dir)
sys.path.insert(1, project_dir)
from Frameworks import etlFramework

def main(project_dir) -> None:

    sc = getSparkSession(f"{project_dir}/Config/sparkSettings.json")
    salesSchema = StructType([
    StructField("ORDERNUMBER", LongType(),True),
    StructField("PRODUCTCODE", StringType(),True),
    StructField("attributes", ArrayType(
        StructType([
            StructField("MSRP",LongType(),True),
            StructField("ORDERDATE",StringType(),True),
            StructField("PRICEEACH",DoubleType(),True),
            StructField("PRODUCTLINE",StringType(),True),
            StructField("QUANTITYORDERED",LongType(),True),
            StructField("SALES",DoubleType(),True),
            StructField("STATUS",StringType(),True)]
        ), True)
                , True)
])
    salesFileList = listofloadingfiles("/SPARK_SAN/SOURCE_DATA/001", "json")
    print("Total Files : ", len(salesFileList))

    salesDF_T1 = createDataFrame(sc, salesFileList, "json", "True", salesSchema)

    salesDF_T2 = salesDF_T1.select(
        salesDF_T1.ORDERNUMBER,
        salesDF_T1.PRODUCTCODE,
        salesDF_T1.attributes.MSRP,
        salesDF_T1.attributes.ORDERDATE,
        salesDF_T1.attributes.PRICEEACH,
        salesDF_T1.attributes.PRODUCTLINE,
        salesDF_T1.attributes.QUANTITYORDERED,
        salesDF_T1.attributes.SALES,
        salesDF_T1.attributes.STATUS
    )

    salesDF_T3 = (salesDF_T2
                  .withColumnRenamed("attributes.MSRP", "MSRP")
                  .withColumnRenamed("attributes.ORDERDATE", "ORDERDATE")
                  .withColumnRenamed("attributes.PRICEEACH", "PRICEEACH")
                  .withColumnRenamed("attributes.PRODUCTLINE", "PRODUCTLINE")
                  .withColumnRenamed("attributes.QUANTITYORDERED", "QUANTITYORDERED")
                  .withColumnRenamed("attributes.SALES", "SALES")
                  .withColumnRenamed("attributes.STATUS", "STATUS"))

    salesDF_T4 = (salesDF_T3
                  .withColumn("MSRP", F.concat_ws(",", "MSRP"))
                  .withColumn("ORDERDATE", F.concat_ws(",", "ORDERDATE"))
                  .withColumn("PRICEEACH", F.concat_ws(",", "PRICEEACH"))
                  .withColumn("PRODUCTLINE", F.concat_ws(",", "PRODUCTLINE"))
                  .withColumn("QUANTITYORDERED", F.concat_ws(",", "QUANTITYORDERED"))
                  .withColumn("SALES", F.concat_ws(",", "SALES"))
                  .withColumn("STATUS", F.concat_ws(",", "STATUS"))
                  )

    sc.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    salesDF = (
    salesDF_T4
        .withColumn("ORDERNUMBER", salesDF_T4["ORDERNUMBER"].cast(LongType()))
        .withColumn("MSRP", salesDF_T4["MSRP"].cast(IntegerType()))
        .withColumn("SALES_ORDERDATE", F.to_date("ORDERDATE",'dd/MM/yyyy HH:mm'))
        .withColumn("ORDERDATE", (F.year(F.to_date("ORDERDATE", 'dd/MM/yyyy HH:mm')) * 10000) + (F.month(F.to_date("ORDERDATE", 'dd/MM/yyyy HH:mm')) * 100) + (F.dayofmonth(F.to_date("ORDERDATE",'dd/MM/yyyy HH:mm')) ))
        .withColumn("PRICEEACH", salesDF_T4["PRICEEACH"].cast(DoubleType()))
        .withColumn("QUANTITYORDERED", salesDF_T4["QUANTITYORDERED"].cast(IntegerType()))
        .withColumn("SALES", salesDF_T4["SALES"].cast(DoubleType())))

    showSampleDFValues(salesDF)






















def listofloadingfiles(path: str, pattern:Optional[str] = "csv" ) -> str:
    return etlFramework.ETL_Framework(config={}).listofloadingfiles(path, pattern)


def getSparkSession(configfile: str):
    return etlFramework.ETL_Framework(config={}).getSparkSession(configfile, False)


def createDataFrame(sc: SparkSession, files: list, filetype: Optional[str] = "csv", Multiline: Optional[str] = None, DFSchema: Optional[StructType] = None):
    print("File Type -->" ,filetype )
    return etlFramework.ETL_Framework(config={}).createDataFrame(sc, files, filetype, Multiline, DFSchema)


def showSampleDFValues(df: DataFrame, ShowValues: Optional[str] = True ):
    etlFramework.ETL_Framework(config={}).showSampleDFValues(df, ShowValues)



if __name__ == '__main__':
    main(project_dir)

