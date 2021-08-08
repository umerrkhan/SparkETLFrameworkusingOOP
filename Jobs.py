import os, sys,json
from typing import Optional

project_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(1, project_dir)
from Frameworks import etlFramework


def main(project_dir) -> None:

    sparkAppConfig = f"{project_dir}/Config/sparkSettings.json"
    landing_dir = "/SPARK_SAN/SOURCE/"

    #print(listofloadingfiles("/SPARK_SAN/SOURCE/"))

    #print(listofloadingfiles("/SPARK_SAN/SOURCE/","info"))

    #print(listofloadingfiles("/SPARK_SAN/SOURCE/Twitter/twitter.info", "info"))

    sparkStart(sparkAppConfig)




def listofloadingfiles(path: str, pattern:Optional[str] = None ) -> str:
    return etlFramework.ETL_Framework(config={}).listofloadingfiles(path, pattern)

def sparkStart(filepath:str):
    etlFramework.ETL_Framework(config={}).sparkStart(filepath, True)


if __name__ == '__main__':
    main(project_dir)

