# Data Warehouse

## Summary
This project demonstrates how to copy data from json files, hosted on an S3 datastore, to staging tables on a Redshift cluster, and then ETL the data into a star schema SQL database on the said Redshift cluster.

**Extract** 
Extract the data from staging tables used to temporarily hold our unorganized data.

**Transform** 
Transform the data using basic SQL queries.

**Load** 
Load the data to a star schema SQL database on the Redshift cluster .


## How to run the python scripts
Using a terminal you will need to run
1. create_tables.py (Used to setup the star schema data model)
2. etl.py (Used to wrangle and load the data)

## Explanation of files
### create_tables.py
create_tables.py is used to drop the tables used to store the data, if any are present, then create those tables.

### etl.py
This script copies data from json files located in an S3 data store to staging tables on a Redshift Cluster. When the raw data has been copied and mapped to an SQL database staging table, the data is ready to be parsed through to extract only the data we need. To create all of our dimension tables, the program uses SQL queries to clean and transform the dataset into something more useful. The data is then loaded into the star schema tables in a way to maximize query efficiency. AWS offers a handful of techniques to allow the data to be efficiently queried while running on multiple nodes. Smaller dimension tables are distributed on ALL nodes to limit the amount of shuffling. i.e. limiting the amount of network calls to physical CPUs or Nodes. Larger dimension tables can be linked to a larger fact table by associating a distribution key between the tables to allow an EVEN distribution of rows.

### sql_queries.py
This file includes sql queries used to create our datamodel, drop our tables, and query the staging tables for specific data needed to be loaded into our final tables. Distribution and sorting strategies are implemented on the create tables queries in an effort to maximize the query effeciency of our data model. 