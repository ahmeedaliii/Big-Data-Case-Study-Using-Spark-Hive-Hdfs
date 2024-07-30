#!/bin/bash

DATABASE=retail

# Get the list of tables
TABLES=$(hive -e "SHOW TABLES IN $DATABASE;")

# Drop each table
for TABLE in $TABLES
do
    hive -e "DROP TABLE IF EXISTS $DATABASE.$TABLE;"
done
