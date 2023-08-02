import csv
import sqlite3

# Function to create a database table and insert data
def create_table_and_insert_data(connection):
    create_table_query = '''
        CREATE TABLE IF NOT EXISTS data_table (
            column1 TEXT,
            column2 TEXT,
            column3 INTEGER
        )
    '''
    connection.execute(create_table_query)

# Function to insert data into the database
def insert_data(connection, data):
    insert_query = '''
        INSERT INTO data_table (column1, column2, column3) VALUES (?, ?, ?)
    '''
    connection.executemany(insert_query, data)
    connection.commit()

# Main function
def main():
    # Replace 'your_database_name.db' with the name of your SQLite database file
    database_file = 'your_database_name.db'
    
    # Replace 'your_csv_file.csv' with the name of your CSV file
    csv_file = 'your_csv_file.csv'

    # Create a connection to the database
    connection = sqlite3.connect(database_file)

    # Create a table and insert data into the database
    create_table_and_insert_data(connection)

    # Read data from the CSV file in streaming fashion and insert into the database
    with open(csv_file, 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        next(csvreader)  # Skip the header row if present
        
        batch_size = 1000  # Adjust the batch size based on your dataset size
        data_batch = []

        for row in csvreader:
            # Process each row and add it to the data batch
            # Adjust the column indexes (0, 1, 2) based on your CSV structure
            column1, column2, column3 = row[0], row[1], int(row[2])
            data_batch.append((column1, column2, column3))

            # Insert the batch into the database when it reaches the batch size
            if len(data_batch) >= batch_size:
                insert_data(connection, data_batch)
                data_batch = []

        # Insert any remaining data in the last batch
        if data_batch:
            insert_data(connection, data_batch)

    # Close the database connection
    connection.close()

if __name__ == "__main__":
    main()

