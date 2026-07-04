

import os
import pandas as pd
base_database_path = '/content/drive/MyDrive/datasets'

def generate_dynamic_sql_ddl_silver(base_path) :
  if not os.path.exists(base_path):
    print(f"the path is not right : {base_path}")
    return
  else :
    for sub_folder in os.listdir(base_path):
      sub_folder_path = os.path.join(base_path , sub_folder)
      # making sure that folder is dir :
      if os.path.isdir(sub_folder_path):
        schema_name = sub_folder
        print (f"\n\n  -- ################################")
        print (f"-- starting schema : {schema_name}")
        print (f"      -- ################################")



        for file_name in os.listdir(sub_folder_path) :
          if file_name.lower().endswith('.csv'):
            file_path = os.path.join(sub_folder_path , file_name)
            table_name = os.path.splitext(file_name)[0]


            clean_schema_prefix = schema_name.replace('source_', '')
            
            full_table_name = f"bronze.{clean_schema_prefix}_{table_name}"
              #------------------------------------------------------------------------
            try :
              #reading the first 5 rows to know the type :
              df = pd.read_csv(file_path , nrows = 5)

              print(f"--=============================")

              print(f"ddl for table : {full_table_name}")

              print(f"--=============================")

              print(f"if object_id('{full_table_name}','U') is not null")

              print(f"    drop table {full_table_name}; ")

              print("go")

              print(f"create table {full_table_name} ( ")

              columns_defs = []

              for col in df.columns:
                clean_column_name = col.strip()
                if ' ' in clean_column_name :
                  columns_defs.append(f"     [{clean_column_name}] nvarchar(500)")

                else :columns_defs.append(f"     {clean_column_name} nvarchar(500)")

              print(",\n".join(columns_defs))
              print(");")
              print(f"go\n")




            except Exception as e :
              print(f"an error happended while reading {file_name} file : {str(e)}")


generate_dynamic_sql_ddl_silver(base_database_path)


