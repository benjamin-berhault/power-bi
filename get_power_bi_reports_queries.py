"""Script to retrieve queries from Power BI Report Server
and store this information in a SQL Server database such as

```sql
USE [DESTINATION_DATABASE]
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'MY_SCHEMA_NAME.PowerBIReportQueries') AND type in (N'U'))
BEGIN
       DROP TABLE MY_SCHEMA_NAME.PowerBIReportQueries
END

CREATE TABLE MY_SCHEMA_NAME.PowerBIReportQueries(
	[Item Id] varchar(36) NOT NULL,
	[Report name] [nvarchar](512) NOT NULL,
	[Path] [nvarchar](512) NOT NULL,
	[Queries] [varchar](max) NOT NULL,
	[Loading date] [datetime2](0) NOT NULL CONSTRAINT C_Dim_PowerBIRapportRequetes_LoadingDate_GETDATE DEFAULT GETDATE(),
	[Loading user] [varchar](128) NOT NULL CONSTRAINT C_Dim_PowerBIRapportRequetes_LoadingUser_SYSTEM_USER DEFAULT SYSTEM_USER
) ON [MY_SCHEMA_NAME]

GO
```

Standalone execution
--------------------
	# Load your Python environment
	# Add to the PYTHONPATH variable the project root directory
	export PYTHONPATH=$PYTHONPATH:$(pwd)
	 
	# Call the __main__ function to launch a standalone gradient boosting model training
	python get_power_bi_reports_queries.py
"""

# Authors: Benjamin Berhault

import os
import requests
import subprocess
from requests_negotiate_sspi import HttpNegotiateAuth
import pyodbc
from time import time, strftime

# Local configuration
REPORTS_SUBFOLDER_NAME = 'power_bi_reports'
POWER_BI_API = 'http://sqlinfobi/ReportsPowerBi/api/v2.0/'

PB_REPORT_SERVER_DB_CONNECTION_STRING = r'Driver={ODBC Driver 17 for SQL Server};Server=SQLINFOBI;Database=ReportServePowerbi;Trusted_Connection=yes;'
POWER_BI_CATALOG_TABLE = '[ReportServePowerbi].[dbo].[Catalog]'

RESULTS_DESTINATION_CONNECTION_STRING = r'Driver={ODBC Driver 17 for SQL Server};Server=SQLINFOSERVICE;Database=Staging;Trusted_Connection=yes;'
DESTINATION_TABLE = 'Staging.MDW.Dim_PowerBIRapportRequetes'

SEVEN_ZIP_PATH =  'C:/Program Files (x86)/7-Zip/7z.exe'

def time_me(elapsed_time):
    microseconds_time = elapsed_time * 1000000

    microseconds = int((microseconds_time) % 1000) ;
    milliseconds = int((microseconds_time / 1000) % 1000) ;
    seconds = int((microseconds_time / 1000000) % 60) ;
    minutes = int(((microseconds_time / (1000000*60)) % 60));
    hours   = int(((microseconds_time / (1000000*60*60)) % 24));

    if(hours != 0):
        return ("%d hours %d min %d sec" % (hours, minutes, seconds)) 
    elif(minutes != 0):
        return ("%d min %d sec" % (minutes, seconds))    
    elif(seconds != 0):
        return ("%d sec %d ms" % (seconds, milliseconds))
    else :
        return ("%d ms %d µs" % (milliseconds, microseconds))

def download_pbix(ItemID, Path):
    """Download locally the given Power BI report
	Parameters
	----------
	ItemID : string
		Power BI report identifier
        
	Path : string
		Local folder where to extract the Power BI report
	Returns
	-------
	Local folder where to extract the Power BI report, Zipped Power BI report
	"""
    url = POWER_BI_API + 'catalogitems('+ItemID+')/Content/$value'
    # Download query
    r = requests.get(url, allow_redirects=True, auth=HttpNegotiateAuth())
    # create the report destination folder
    os.makedirs(REPORTS_SUBFOLDER_NAME+Path.rsplit('/', 1)[0], exist_ok=True)
    # Delete any previously imported report
    print(('del "./'+ REPORTS_SUBFOLDER_NAME + Path +'/*"').replace("/", "\\") + " /F /Q")
    subprocess.check_output(('if exist ./' + REPORTS_SUBFOLDER_NAME + Path + ' del "./'+ REPORTS_SUBFOLDER_NAME + Path +'/*" 2>nul').replace("/", "\\") + " /F /Q", shell=True,encoding="437")
    
    open(REPORTS_SUBFOLDER_NAME + Path + '.zip', 'wb').write(r.content)
    
    return str(REPORTS_SUBFOLDER_NAME + Path), str(REPORTS_SUBFOLDER_NAME + Path + '.zip') 
    

if __name__ == '__main__':

    if not os.path.exists(REPORTS_SUBFOLDER_NAME):
        os.makedirs(REPORTS_SUBFOLDER_NAME)

    con1_results_destination = pyodbc.connect(RESULTS_DESTINATION_CONNECTION_STRING)

    cur1_results_destination = con1_results_destination.cursor()
    cur1_results_destination.execute('TRUNCATE TABLE ' + DESTINATION_TABLE + ';')
    con1_results_destination.close()
    
    print(strftime('%H:%M:%S'), '- Retrieving information relating to Power BI reports (identifier, path, name of the report)')
    start_time = time()
    con_sqlinfobi = pyodbc.connect(PB_REPORT_SERVER_DB_CONNECTION_STRING)
    cur_sqlinfobi = con_sqlinfobi.cursor()
    cur_sqlinfobi.execute("SELECT ItemID, Path, Name \
      FROM " + POWER_BI_CATALOG_TABLE + " t  \
      WHERE Type = 13")
    print('Information retrieved', time_me(time() - start_time), "\n")

    """ Power BI report server objects
    CASE Type
           WHEN 1 THEN 'Folder'
           WHEN 2 THEN 'Report Builder'
           WHEN 5 THEN 'Data Source'
           WHEN 7 THEN 'Report Part'
           WHEN 8 THEN 'Shared Dataset'
           WHEN 13 THEN 'Power BI Report'
           ELSE 'Other'
         END
    """

    # Query extraction from reports one after the other
    while 1:
        row = cur_sqlinfobi.fetchone()
        if not row:
            break
            
        print(strftime('%H:%M:%S'), '- Download a report')
        start_time = time()
        folder, file_zip = download_pbix(row.ItemID, row.Path)
        print('Report downloaded', time_me(time() - start_time), "\n")

        print(strftime('%H:%M:%S'), '- Extracting the DataMashup file')
        start_time = time()
        print(('Command: "' + SEVEN_ZIP_PATH + '" -aoa e "'+file_zip+'" "-o./'+folder+'" DataMashup -y').replace("/", "\\"))
        subprocess.check_output(('"' + SEVEN_ZIP_PATH + '" -aoa e "'+file_zip+'" "-o./'+folder+'" DataMashup -y').replace("/", "\\"), shell=True,encoding="437")
        print('DataMashup extracted in', time_me(time() - start_time), "\n")

        print(strftime('%H:%M:%S'), '- Extracting Section1.m')
        start_time = time()
        print(('Command: "' + SEVEN_ZIP_PATH + '" -aoa e "'+folder+'/DataMashup" "-o./'+folder+'" Section1.m -r -y').replace("/", "\\"))
        p = subprocess.Popen(('"' + SEVEN_ZIP_PATH + '" -aoa e "'+folder+'/DataMashup" "-o./'+folder+'" Section1.m -r -y').replace("/", "\\"), shell = True)
        # Waits for subprocess.Popen() to finish before continuing
        p.wait()
        print('Section1.m extracted in', time_me(time() - start_time), "\n")
              
        print(strftime('%H:%M:%S'), '- Reading Section1.m')
        start_time = time()
        queries_file = str(os.getcwd().replace("\\","/")+('/'+folder+'/Section1.m'))
        print("Full path: ",queries_file)
        
        f = open(queries_file, 'r', encoding='utf-8')
        file_contents = f.read()
        
        print(u'[Item Id] : '+row.ItemID)
        print(u'[Report name] : '+row.Name)
        print(u'[Path] : '+folder)
        # print(u'[Queries] : '+file_contents)

        con2_results_destination = pyodbc.connect(RESULTS_DESTINATION_CONNECTION_STRING)
        cur2_results_destination = con2_results_destination.cursor()   
                
        query = 'INSERT INTO ' + DESTINATION_TABLE + '([Item Id], [Report name] ,[Path], [Queries])  \
        values(\''+row.ItemID+'\',\''+row.Name+'\',\''+folder+'\',\''+file_contents.replace("'"," ")+'\')'
                
        cur2_results_destination.execute(query)
        con2_results_destination.commit()
        con2_results_destination.close()
        
    con_sqlinfobi.close()