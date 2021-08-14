from pyspark.sql.types import StructType, StructField, IntegerType, StringType,LongType,ArrayType,DoubleType,DateType,IntegerType

class TableStructs:

    salesSchema = StructType([
    StructField("ORDERNUMBER", LongType(), True),
    StructField("PRODUCTCODE", StringType(), True),
    StructField("attributes", ArrayType(
        StructType([
            StructField("MSRP", LongType(),True),
            StructField("ORDERDATE", StringType(), True),
            StructField("PRICEEACH", DoubleType(), True),
            StructField("PRODUCTLINE", StringType(), True),
            StructField("QUANTITYORDERED", LongType(), True),
            StructField("SALES", DoubleType(), True),
            StructField("STATUS", StringType(), True)]
        ), True)
                , True)
])