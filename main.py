import pandas as pd
import psycopg2
from sqlalchemy import create_engine, inspect, Table, MetaData

# Step 1: Load and clean the data
csv_file = 'flipkart_com-ecommerce_sample.csv'
df = pd.read_csv(csv_file)

print(len(df))

# Example cleaning - customize as needed
df.columns = df.columns.str.strip()
df.drop(columns=['product_rating', 'overall_rating', 'product_category_tree'], inplace=True)
df.dropna(inplace=True)

print(len(df))

# Step 2: Database connection details
db_config = {
    'username': 'postgres',
    'password': 'your-password',
    'host': 'your-db-name',
    'port': '5432',
    'database': 'your-db-name',
    'table_name': 'your-table-name'
}

# Create SQLAlchemy engine
engine = create_engine(
    f"postgresql+psycopg2://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
)

print("Engine initiated")

# Step 3: Check if table exists
inspector = inspect(engine)
table_exists = inspector.has_table(db_config['table_name'])

print("Table existence checked")

# Step 4: Insert data accordingly
if table_exists:
    print(f"Table '{db_config['table_name']}' exists. Appending data...")
    df.to_sql(db_config['table_name'], engine, if_exists='append', index=False)
else:
    print(f"Table '{db_config['table_name']}' does not exist. Creating table and inserting data...")
    df.to_sql(db_config['table_name'], engine, if_exists='replace', index=False)

print("Data upload complete.")
