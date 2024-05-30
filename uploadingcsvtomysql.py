import mysql.connector
import pandas as pd

host = "127.0.0.1"
user = "root"
password = "admin"
database = "task2"
table_name = "diamonds"
csv_file_path = "cubic_zirconiaaaa.csv"

# Connect to the database
conn = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database
)

cursor = conn.cursor()

try:
    # Read the CSV file
    df = pd.read_csv(csv_file_path)

    # Remove any whitespace from column names
    df.columns = df.columns.str.strip()

    # Ensure all NaN values are converted to None
    df = df.where(pd.notna(df), None)

    # Check column names and missing values
    print("CSV Columns:", df.columns)
    print("Any missing values in CSV columns:", df.isnull().any())

    # Print the number of rows in the dataframe
    print(f"Number of rows in the CSV: {len(df)}")

    # Iterate over each row and insert into MySQL table
    inserted_rows = 0
    for index, row in df.iterrows():
        try:
            # Print row data for debugging
            print(f"Inserting row {index + 1}: {row.to_dict()}")

            # Ensure all columns are present
            cursor.execute(
                f"INSERT INTO {table_name} (carat, cut, color, clarity, depth, tableee, x, y, z, price) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    row['carat'], row['cut'], row['color'], row['clarity'], row['depth'],
                    row['tableee'], row['x'], row['y'], row['z'], row['price']
                )
            )
            inserted_rows += 1
        except Exception as e:
            print(f"Error inserting row {index + 1}: {e}")
            print("Row data:", row)

    # Commit the transaction
    conn.commit()
    print(f"CSV data uploaded successfully to MySQL. Total rows inserted: {inserted_rows}")

except Exception as e:
    print("Error:", e)
    conn.rollback()

finally:
    cursor.close()
    conn.close()
