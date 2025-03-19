import csv
import sqlite3

from path import csv_file_path


def csv_to_sqlite(csv_file_path, sqlite_db_path, lowest_rating, highest_rating):
    # Connect to SQLite database (or create it)
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()
    count = 0
    # Read the CSV file
    with open(csv_file_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        headers = next(reader)  # Get the headers from the first row

        # Define column types
        column_types = {header: 'TEXT' for header in headers}
        for col in ['Rating', 'RatingDeviation', 'Popularity']:
            if col in column_types:
                column_types[col] = 'INTEGER'

        # Create a table with the same name as the CSV file (without extension)
        table_name = csv_file_path.split('/')[-1].split('.')[0]
        columns = ', '.join([f'"{header}" {column_types[header]}' for header in headers])
        cursor.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns})')
        max_rating = 0
        # Insert the CSV data into the table
        for row in reader:
            if not lowest_rating <= int(row[3]) <= highest_rating:
                continue
            max_rating = max(max_rating, int(row[3]))
            count += 1
            placeholders = ', '.join(['?' for _ in headers])
            cursor.execute(f'INSERT INTO "{table_name}" VALUES ({placeholders})', row)
    # Commit the transaction and close the connection
    conn.commit()
    conn.close()
    return count



low = 0
total_count = 0
for r in [(0, 1000), (1001, 1500), (1501, 2000), (2001, 2500), (2501, 3900)]:
    sqlite_db_path = f'puzzle-{r[0]}-{r[1]}.db'
    current_count = csv_to_sqlite(csv_file_path, sqlite_db_path, r[0], r[1])
    print(f"Total puzzles in range {r[0]} to {r[1]}: {current_count}")
    total_count += current_count

print(total_count)
