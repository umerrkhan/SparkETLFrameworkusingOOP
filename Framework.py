import json, os, re, sys
from typing import Optional

class ETL_Framework:
    def __init__(self, config):
        self.config = config


    def listofloadingfiles(self,location: str, pattern:Optional[str] = None) -> list:
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

        def SearchSpecificfiles(filelist:list, pattern:Optional[str] = None) -> list:
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


