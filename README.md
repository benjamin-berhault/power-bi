# Query crawler for Power BI Reports Server

This script retrieve all the queries used by reports published under a Power BI Report Server and store this information in a given SQL Server table.

To automatically export requirements.txt
```
pip freeze > requirements.txt
# If using Anaconda
conda list -e > requirements.txt
```