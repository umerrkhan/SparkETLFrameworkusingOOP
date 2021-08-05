import os, sys
from typing import Optional

project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(1, project_dir)
from SampleProject import Framework


def main(project_dir) -> None:
    landing_dir = "/SPARK_SAN/SOURCE/"

    #print(listofloadingfiles("/SPARK_SAN/SOURCE/"))

    #print(listofloadingfiles("/SPARK_SAN/SOURCE/","info"))

    print(listofloadingfiles("/SPARK_SAN/SOURCE/Twitter/twitter.info","info"))



def listofloadingfiles(path: str, pattern:Optional[str] = None ) -> str:
    return Framework.ETL_Framework(config={}).listofloadingfiles(path, pattern)

if __name__ == '__main__':
    main(project_dir)

