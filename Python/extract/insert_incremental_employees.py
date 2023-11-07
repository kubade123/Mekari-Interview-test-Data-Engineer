import pandas as pd
from sqlalchemy import create_engine
from db_config import DB_PARAMS_2

# Membuat koneksi ke DB
engine = create_engine(DB_PARAMS_2)

# Membaca employees csv
csv_file = 'data/employees.csv'
new_df = pd.read_csv(csv_file)
new_df.rename(columns= {'employe_id': 'employee_id'}, inplace=True)

query = "SELECT * FROM staging.employees"
current_df = pd.read_sql_query(query, engine)

# Mengambil record incremental
new_records = new_df[~new_df['employee_id'].isin(current_df['employee_id'])]
print(new_records.to_string())

# Write record incremental
new_records.to_sql('employees', engine, schema='staging',if_exists='append', index=False)



