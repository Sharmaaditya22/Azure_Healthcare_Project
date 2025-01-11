import pandas as pd

def csv_to_sql_queries(file_path, table_name):
    # Load the CSV file into a pandas DataFrame
    data = pd.read_csv(file_path)
    
    # Extract column names
    columns = data.columns.tolist()
    
    # Generate SQL INSERT queries
    queries = []
    for _, row in data.iterrows():
        values = ', '.join(
            [f"'{value}'" if isinstance(value, str) else str(value) for value in row]
        )
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({values});"
        queries.append(query)
    
    return queries

# File path and table name
csv_file = r'C:\Users\Admin\Desktop\Code\Trendytech-Azure-Project\datasets\EMR\trendytech-hospital-b\transactions.csv'
table_name = 'dbo.transactions'

# Generate SQL queries
sql_queries = csv_to_sql_queries(csv_file, table_name)

# Save the queries to a file
output_file = r'C:\Users\Admin\Desktop\Code\Trendytech-Azure-Project\datasets\EMR\trendytech-hospital-b\insert_transactions.sql'
with open(output_file, 'w') as file:
    file.write('\n'.join(sql_queries))

print(f"SQL queries have been generated and saved to '{output_file}'.")
