# Intake the json file that came from `aws glue get-table` and split that 
# into individual tables that can be ingested into `aws glue create-table`

import sys
import json
import os

def main():
    argv = sys.argv

    if len(argv) < 3:
        print('Error: 2 arguments needed. Json file of combined schema and s3 prefix for the new location buckets.')
        exit()

    filename = argv[1]
    prefix = argv[2]
    
    SplitTables(filename, prefix)

def SplitTables(filename, prefix):
    contents = None

    with open(filename, 'r') as f:
        content = f.read()
    
    content = json.loads(content)
    tableList = content['TableList']

    for table in tableList:
        createTable(table, prefix)

def createTable(table, prefix):
    databaseName = table['DatabaseName']

    print(f"Processing {table['Name']}")

    # We will create a new local directory with the database name
    # within which all the individual table names will go.
    if not os.path.isdir(databaseName):
        os.mkdir(databaseName)

    # Change into the directory as all our files are going to made in that.
    os.chdir(databaseName)
    
    # We only need a subset of the values returned by the get-table output
    acceptedParams = ['Name', 'Description', 'Owner', 'LastAccessTime',  'LastAnalyzedTime', 'Retention', 'StorageDescriptor', 'PartitionKeys', 'ViewOriginalText', 'ViewExpandedText', 'TableType', 'Parameters', 'TargetTable']
    toRemove = []
    tableName = table['Name']
    for key in table.keys():
        if not key in acceptedParams:
            toRemove.append(key)

    for key in toRemove:
        del table[key]

    if not ('Location' in table['StorageDescriptor']):
        print(f"Skipping {tableName} as StorageDescriptor does not contain field named Location.")
        return

    # Add the prefix to the location.
    location = table['StorageDescriptor']['Location']
    if len(location) > 0:
        protocol = location[:5]
        path = location[len(protocol):].split('/')
        bucketName = path[0]
        bucketName = ''.join([prefix, '-', bucketName])
        path[0] = bucketName
        location = ''.join([protocol, '/'.join(path)])
        table['StorageDescriptor']['Location'] = location

    with open(f"{tableName}.json", 'w') as f:
        f.write(json.dumps(table, indent=1))

    os.chdir('../')

if __name__ == '__main__':
    main()
