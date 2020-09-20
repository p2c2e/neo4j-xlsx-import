## Utility to load data from xlsx file into Neo4J

# HOW TO RUN
- One time setup 
```
conda install pandas xlrd
```
- To run this script 
```
python <thisfilename.py>
```

# On successful completion 
1. You will have a bunch of entity.csv and relationship.csv files
1. A "importcmd.sh" file that can be executed to load the data into Neo4J
1. A pre-existing runneo4j.sh script illustrates how to launch Neo4J via docker 
# WARNING: 
In the Neo4J community edition, bulk import cannot be done into an existing
database. So, this script will wipe / delete the contents and load the 
data. This means the password will be reset to the default 'neo4j' 



![Sample Loaded data in Neo4J](https://raw.githubusercontent.com/p2c2e/neo4j-xlsx-import/master/SampleImport.png?raw=true)
