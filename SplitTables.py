# Intake the json file that came from `aws glue get-table` and split that 
# into individual tables that can be ingested into `aws glue create-table`

import sys
import json
import os

def main():
    argv = sys.argv

    if len(argv) < 2:
        print('Error: JSON argument needed.')
        exit()

    filename = argv[1]
    
    SplitTables(filename)

def SplitTables(filename):
    contents = None

    with open(filename, 'r') as f:
        content = f.read()
    
    content = json.loads(content)
    tableList = content['TableList']

    for table in tableList:
        createTable(table)

def createTable(table):
    databaseName = table['DatabaseName']

    # We will create a new local directory with the database name
    # within which all the individual table names will go.
    if not os.path.isdir(databaseName):
        os.mkdir(databaseName)

    # Change into the directory as all our files are going to made in that.
    os.chdir(databaseName)
    
    # We only need a subset of the values returned by the get-table output
    acceptedParams = ['Name', 'Description', 'Owner', 'LastAccessTime',  'LastAnalyzedTime', 'Retention', 'StorageDescriptor', 'PartitionKeys', 'ViewOriginalText', 'ViewExpandedText', 'TableType', 'Parameters', 'TargetTable']
    toRemove = []
    for key in table.keys():
        if not key in acceptedParams:
            toRemove.append(key)

    for key in toRemove:
        del table[key]
    
    tableName = table['Name']
    with open(f"{tableName}.json", 'w') as f:
        f.write(json.dumps(table, indent=1))

    os.chdir('../')

if __name__ == '__main__':
    main()
