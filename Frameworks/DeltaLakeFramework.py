from typing import Optional

def GenerateDBLocation(storageaccount,layername,organisation,organisation_domain,domain_service,database_name,database_type):
    return f"abfss://{layername}@{storageaccount}.dfs.core.windows.net/{organisation}/{organisation_domain}/{domain_service}/{database_name}/{database_type}"
    

def GenerateDeltaTableLocation(database_location,source_catalog,table_name,file_format):
    return f"{database_location}/{source_catalog}/{table_name}/{file_format}"
    

def GeneratecreateDatabseStatement(database_name,database_location):
    return f"CREATE DATABASE IF NOT EXISTS {database_name} LOCATION \'{database_location}\'  "

def GeneratecreateTableStatement(sourceDbName,sourceTablename,sourceTableAttributes,metadataAttributes,partitionColumn:Optional[str] = "",tableLocation:Optional[str] = ""):  

    def generate_addcolumnStatement(columnNameswithDatatypes, MetadataColumns:Optional[bool] = True):
        totalcolumns =  len(columnNameswithDatatypes)
        statment = ""
        columnStatement = ""
        for key, value in columnNameswithDatatypes.items():
            if MetadataColumns == True:
                statment = f".addColumn(\"{key}\", \"{value}\" , nullable = False )"
            else:
                statment = f".addColumn(\"{key}\", \"{value}\")"

            if totalcolumns == 0 :     
                columnStatement = statment
            else:
                columnStatement = columnStatement + statment
        return columnStatement
    
    create_table_statement = f"DeltaTable.createIfNotExists(spark).tableName(\"{sourceDbName}.{sourceTablename}\")"
    create_table_statement = create_table_statement + generate_addcolumnStatement(sourceTableAttributes) 
    create_table_statement = create_table_statement + generate_addcolumnStatement(metadataAttributes,True)
        
    if partitionColumn != "":
        create_table_statement = create_table_statement + f".partitionedBy(\"{partitionColumn}\")"    
    if tableLocation != "":
        create_table_statement = create_table_statement + f".location(\"{tableLocation}\")"    
    return create_table_statement

def GenerateSelectStatement(source_database_name,source_table_name,columnNameswithDatatypes):
        counter = 0
        totalcolumns =  len(columnNameswithDatatypes)        
        SelectStatement = ""
        for key, value in columnNameswithDatatypes.items():
                if counter == 0:
                     SelectStatement = "Select " + f"\"{key}\""
                     counter+=1
                else:
                     SelectStatement = SelectStatement + f", \"{key}\""
        SelectStatement = SelectStatement + f" FROM {source_database_name}.{source_table_name}"
        return SelectStatement

def GenerateUpdatedict(update_database_name,update_table_name,columnNameswithDatatypes):
        counter = 0
        totalcolumns =  len(columnNameswithDatatypes)        
        UpdateStatement = ""
        for key, value in columnNameswithDatatypes.items():
                if counter == 0:
                     UpdateStatement = f"\"{key}\" : source.\"{key}\""
                     counter+=1
                else:
                     UpdateStatement = UpdateStatement+ f",\"{key}\" : source.\"{key}\""
        
        return UpdateStatement

def GenerateMergeStatement(delta_update_target,source_df,merge_on_column,source_database_name,source_table_name):
     statement = f"{delta_update_target}.alias('target').merge({source_df}.alias('source')"
     statement = statement + f" on ( source.{merge_on_column} = target.{merge_on_column}" + ")"
     statement = statement + f".whenNotMatchedInsert(values = " + GenerateUpdatedict(source_database_name,source_table_name,table_attributes) + ")" 
     statement = statement + f".execute()"
     return statement
