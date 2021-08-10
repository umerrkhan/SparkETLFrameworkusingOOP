import json, os, re, sys
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


class ETL_Framework:
    def __init__(self, config):
        self.config = config

    def listofloadingfiles(self, location: str, pattern: Optional[str] = None) -> list:
        def FileOrDirectoy(location: str) -> str:
            if os.path.exists(location):
                if os.path.isfile(location):
                    return "File"
                else:
                    return "Dir"
            else:
                return "Invalid Path"

        def ListFiles(location: str) -> list:
            filelist = []
            for dirpath, dirname, filenames in os.walk(location):
                for filename in filenames:
                    filelist.append(f"{dirpath}/{filename}")
            return filelist

        def SearchSpecificfiles(filelist: list, pattern: Optional[str] = None) -> list:
            files = []
            if pattern == None:
                files = filelist
            else:
                for filename in filelist:
                    if re.search(rf"{pattern}", filename):
                        files.append(filename)
            return files

        if FileOrDirectoy(location) == "Dir":
            allfileslist = ListFiles(location)
            return SearchSpecificfiles(allfileslist, pattern)
        else:
            return SearchSpecificfiles(location)

    def getSparkSession(self, filepath: str, appDebug: Optional[str] = False) -> SparkSession:
        with open(filepath, "r") as f:
            SessionParams = json.load(f)

        Master = SessionParams["sparkconf"]["master"]
        AppName = SessionParams["sparkconf"]["appname"]
        LogLevel = SessionParams["log"]["level"]
        LogLevel = SessionParams.get('sparkconf', {}).get('log', "level")  # another method
        builder = SparkSession.builder.appName(AppName).master(Master)
        return builder.getOrCreate()

        if appDebug:
            print("Settings from Json File")
            print("Master    : ", Master)
            print("App Name  : ", AppName)
            print("Log Level :", LogLevel)
            print("Type :", type(SparkSession))
            print(SparkSession)
        return SparkSession

    def createDataFrame(self, sc: SparkSession, files: list, filetype: str) -> DataFrame:

        def createCSVDataFrame(sc: SparkSession, files: list) -> DataFrame:
            df = sc.read.format("csv") \
                .option("header", "true") \
                .option("mode", "DROPMALFORMED") \
                .load(files)
            return df

        def createJSONDataFrameJSON(sc: SparkSession, files: list) -> DataFrame:
            print("json Function is called")
            df = sc.read.format("json") \
                .option("mode", "PERMISSIVE") \
                .option("primitivesAsString", "true") \
                .option("multiline", "true") \
                .load(files)

            return df

        if filetype == "json":
            df = createJSONDataFrameJSON(sc, files)
        elif filetype == "csv":
            df = createCSVDataFrame(sc, files)

        return df

    def showSampleDFValues(self, df: DataFrame):
        print("Printing Data Frame Schema")
        print(df.printSchema())
        print("Printing Top 10 Values of Data Frame")
        print(df.show(10))
        print("Total rows including mal format ", df.count())
