import mysql.connector
import pandas as pd

host = "127.0.0.1"
user = "root"
password = "admin"
database = "dataset"
table_name = "dataweather"
csv_file_path = "weatherclm.csv"

conn = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database
)

cursor = conn.cursor()

try:
    df = pd.read_csv(csv_file_path)

    # Remove any whitespace from column names
    df.columns = df.columns.str.strip()

    # Ensure all NaN values are converted to None
    df = df.where(pd.notna(df), None)

    # Convert 'datetime' column to datetime format
    df['datetime'] = pd.to_datetime(df['datetime'])

    # Iterate over each row and insert into MySQL table
    for index, row in df.iterrows():
        try:
            cursor.execute(
                f"INSERT INTO {table_name} (datetime, TempC, DewPointTempC, RelHum, WindSpeed, Visibility, PresskPa, Weather) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    row['datetime'], row['TempC'], row['DewPointTempC'], row['RelHum'], row['WindSpeed'],
                    row['Visibility'], row['PresskPa'], row['Weather']
                )
            )
        except Exception as e:
            print(f"Error inserting row {index + 1}: {e}")
            print("Row data:", row)

    conn.commit()
    print("CSV data uploaded successfully to MySQL.")

except Exception as e:
    print("Error:", e)
    conn.rollback()

finally:
    cursor.close()
    conn.close()
