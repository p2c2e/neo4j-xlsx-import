#!/usr/bin/env python

import pandas as pd
import os
import stat

# HOW TO RUN
# - One time setup 
# conda install pandas xlrd
# - To run this script 
# python <thisfilename.py>

# On successful completion 
# 1. You will have a bunch of <entity>.csv and <relationships>.csv files
# 2. A "importcmd.sh" file that can be executed to load the data into Neo4J
# 3. A pre-existing runneo4j.sh script illustrates how to launch Neo4J via docker 
# WARNING: In the Neo4J community edition, bulk import cannot be done into an existing
#       database. So, this script will wipe / delete the contents and load the 
#       data. This means the password will be reset to the default 'neo4j' 
# 
#################################################################################
# INPUTS SECTION
#################################################################################

# File with data to load
input_filename="sampledata.xlsx"

# Location of CSV files and location of the Neo4J Data folder
import_root="$HOME/workspace/neo4j-pdr/testing/"
import_root="/tmp/testing"
data_root=import_root+"/data"

# Main Entity in the file - Each row will be unique for this entity with a PK column
main_entity_name='PERSON'
main_entity_column="EMP_ID"
main_entity_attributes=['FIRST_NAME','LAST_NAME','STARTDATE','GENDER']

# IF there is no self-ref column, set this self_ref_column to empty "" 
self_ref_column="SUPERVISOR_ID"
self_ref_reln="IS_SUPERVISED_BY"
self_ref_reln_filename="person_supervisor.csv"

# column mappings 
# Store nodes and relationships : Format ....
# ( COL_NAME, 'entity-filename', 'RELATIONSHIP', 'maping-filename', (addl props for entity), (addl props for edge),
mappings = {
    ('TITLE_NAME', 'titles.csv', 'IS_IN_TITLE', 'person_title.csv',(), ()),
    ("GROUP","group.csv","IS_IN_GROUP","person_group.csv",(), ()),
    ("CLIENT_NAME","client.csv","WORKS_FOR","person_client.csv",(), ()),
    ("PROJECT_NAME","project.csv","IS_PART_OF_PROJECT","person_project.csv",(), ("STARTDATE", "TEAM_NAME"))
} # will contain 'node' and 'relationship'

#################################################################################
# CORE LOGIC : Load and process the file ....
#################################################################################
if not os.path.exists(import_root):
    print("Creating the import folder... : "+import_root)
    os.makedirs(import_root)

df = pd.read_excel(input_filename, sheet_name=0, header=0, nrows=19000)

magic_no=999999 

df = df.fillna(magic_no)
if self_ref_column:
  df[self_ref_column] = df[self_ref_column].astype(int)

main_entity = df.loc[:,[main_entity_column]+main_entity_attributes].copy()

main_entity[':LABEL'] = main_entity_name
main_entity = main_entity.rename(columns={main_entity_column : main_entity_column + ':ID'})

main_entity.to_csv(import_root+"/"+main_entity_name.lower()+".csv", index = False)

def getreln(df, colname, entityfile, relation_name, mappingfile, props=(), relprops=()):
    entity = df.loc[:, [colname] + list(props)].copy()
    
    entity = entity.drop_duplicates(subset=[colname] + list(props))
    entity = entity[entity[colname] != magic_no] #Remove bad rows...
    entity = entity.dropna()
    entity[':LABEL'] = colname
    entity = entity.rename(columns={colname: colname+':ID'})
    #print(entity)
    entity.to_csv(import_root+"/"+entityfile, index=False)
    allcols = [main_entity_column, colname] + list(relprops)
    print(allcols)
    relation = df.loc[:, allcols].copy()
    relation[':TYPE'] = relation_name
    relation = relation.rename(columns={main_entity_column: ':START_ID', colname : ':END_ID'})
    relation.to_csv(import_root+"/"+mappingfile, index=False)

def get_self_refs(df, mappingfile):
    relation = df.loc[:, [main_entity_column, self_ref_column]].copy()
    relation[':TYPE'] = self_ref_reln
    relation = relation.rename(columns={main_entity_column : ':START_ID', self_ref_column : ':END_ID'})
    relation.to_csv(import_root+"/"+mappingfile, index=False)
    
for colname, entityfile, relation_name, mappingfile, props, relprops in mappings:
    print("Processing "+colname)
    getreln(df, colname, entityfile, relation_name, mappingfile, props, relprops)

# Since self-referential - special treatment 
if self_ref_column:
    get_self_refs(df,  self_ref_reln_filename)


cmd = "rm -rf data && mkdir data \n"
cmd += "docker run --rm --publish=7474:7474 --publish=7687:7687 --volume="+data_root+":/data --volume="+import_root+":/var/lib/neo4j/import neo4j"
cmd += " neo4j-admin import --report-file=import/import.report "
cmd += " --nodes=import/"+main_entity_name.lower()+".csv"
if self_ref_column:
  cmd += " --relationships=import/"+ self_ref_reln_filename
for colname, entityfile, relation_name, mappingfile, props, relprops in mappings:
  cmd += " --nodes=import/" + entityfile + " --relationships=import/" + mappingfile
cmd += " --high-io=true --skip-bad-relationships --skip-duplicate-nodes=true"

print(cmd) # For debugging 

# Generate a script to IMPORT DATA into neo4j using a docker image
scriptfile=import_root+'/importcmd.sh'
with open(scriptfile, 'w') as ofile:
    ofile.write(cmd)
    os.chmod(scriptfile, os.stat(scriptfile).st_mode | stat.S_IEXEC)

# Generate a script to LAUNCH NEO4J with the NEW DATA
cmd = "docker run --name neo4j-server --rm --publish=7474:7474 --publish=7687:7687 --volume="+data_root+":/data --volume="+import_root+":/var/lib/neo4j/import neo4j"

scriptfile=import_root+'/runneo4j.sh'
with open(scriptfile, 'w') as ofile:
    ofile.write(cmd)
    os.chmod(scriptfile, os.stat(scriptfile).st_mode | stat.S_IEXEC)
