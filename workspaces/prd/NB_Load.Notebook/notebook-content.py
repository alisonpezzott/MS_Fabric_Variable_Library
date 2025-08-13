# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# #### Importing libraries  

# CELL ********************

import notebookutils
import pandas as pd
import pyarrow as pa

from deltalake import write_deltalake, DeltaTable
from sempy.fabric import get_notebook_workspace_id, list_workspaces  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Obtaining parameters from current workspace and defining paths   

# CELL ********************

workspace_id = get_notebook_workspace_id()  
workspace_name = list_workspaces().query("Id == @workspace_id")['Name'].iloc[0]
lakehouse_name = 'LH_Storage'
lakehouse_path = f'abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{lakehouse_name}.Lakehouse'
tables_path = f'{lakehouse_path}/Tables'
files_path = f'{lakehouse_path}/Files/Raw'
 
print(f'Workspace: {workspace_name} | ID: {workspace_id}')
print(f'Tables path: {tables_path}') 
print(f'Files path: {files_path}') 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Read each csv file from Raw folder and load to delta  

# MARKDOWN ********************

# ##### FactIncomeStatement

# CELL ********************

tb = 'FactIncomeStatement'
df = pd.read_csv(f'{files_path}/{tb}.csv', )

df['CompanyCode'] = df['CompanyCode'].astype('int64') 
df['AccountCode'] = df['AccountCode'].astype('int64')
df['Date']        = pd.to_datetime(df['Date'])
df['Amount']      = df['Amount'].astype('float64')  

arrow_schema = pa.schema([
    pa.field('CompanyCode', pa.int64()),
    pa.field('AccountCode', pa.int64()),
    pa.field('Date',        pa.date32()),   
    pa.field('Amount',      pa.float64()),
])

arr_company = pa.array(df['CompanyCode'])
arr_account = pa.array(df['AccountCode'])
arr_date    = pa.array(df['Date'].dt.date, type=pa.date32())
arr_amount  = pa.array(df['Amount'])

arrow_table = pa.Table.from_arrays(
    [arr_company, arr_account, arr_date, arr_amount],
    schema=arrow_schema
)

write_deltalake(
    f'{tables_path}/{tb}', 
    arrow_table, 
    mode='overwrite'
) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ##### DimCompany  

# CELL ********************

tb = 'DimCompany'
df = pd.read_csv(f'{files_path}/{tb}.csv', )

write_deltalake(
    f'{tables_path}/{tb}', 
    df, 
    mode='overwrite'
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ##### DimAccount

# CELL ********************

tb = 'DimAccount'
df = pd.read_csv(f'{files_path}/{tb}.csv', )

write_deltalake(
    f'{tables_path}/{tb}', 
    df, 
    mode='overwrite'
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
