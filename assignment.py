import csv
import sqlite3
import os

def create_database_and_tables():
    """
    Creates the SQLite database and the necessary tables.
    The schema is designed based on the actual columns in the provided CSV files.
    """
    conn = sqlite3.connect('teleparty.db')
    cursor = conn.cursor()

    # Drop tables if they already exist to ensure a fresh start
    cursor.execute('DROP TABLE IF EXISTS shows')
    cursor.execute('DROP TABLE IF EXISTS episodes')

    # --- Create the 'shows' table ---
    # Stores data from 'all-series-ep-average.csv'
    cursor.execute('''
        CREATE TABLE shows (
            Code TEXT PRIMARY KEY,
            Title TEXT,
            Rating REAL,
            RatingCount INTEGER,
            Rank INTEGER
        )
    ''')

    # --- Create the 'episodes' table ---
    # Stores data from 'all-episode-ratings.csv'
    # We use a composite PRIMARY KEY (parent_code, Season, Episode) because the
    # id_code in the source file is not unique. This is a more robust key.
    cursor.execute('''
        CREATE TABLE episodes (
            id_code TEXT,
            parent_code TEXT,
            Season INTEGER,
            Episode INTEGER,
            Rating REAL,
            FOREIGN KEY (parent_code) REFERENCES shows (Code),
            PRIMARY KEY (parent_code, Season, Episode)
        )
    ''')

    conn.commit()
    conn.close()
    print("Database 'teleparty.db' and tables created successfully.")

def ingest_data():
    """
    Reads data from the CSV files and inserts it into the database,
    using the correct column names and cleaning the data as needed.
    """
    conn = sqlite3.connect('teleparty.db')
    cursor = conn.cursor()

    # --- Ingest data into the 'shows' table ---
    with open('all-series-ep-average.csv', 'r', encoding='utf-8-sig') as file:
        reader = csv.DictReader(file)
        to_db = []
        for row in reader:
            # Clean the 'Rating Count' by removing commas before converting to integer
            rating_count_cleaned = int(row['Rating Count'].replace(',', '')) if row['Rating Count'] else None

            to_db.append((
                row['Code'],
                row['Title'],
                float(row['Rating']) if row['Rating'] else None,
                rating_count_cleaned,
                int(row['Rank']) if row['Rank'] else None
            ))
        cursor.executemany('''
            INSERT INTO shows (Code, Title, Rating, RatingCount, Rank)
            VALUES (?, ?, ?, ?, ?)
        ''', to_db)
    print(f"Ingested {len(to_db)} records into 'shows' table.")

    # --- Ingest data into the 'episodes' table ---
    # Use INSERT OR IGNORE to skip any rows that would violate the composite primary key.
    # This handles cases where the CSV contains multiple entries for the same episode.
    with open('all-episode-ratings.csv', 'r', encoding='utf-8-sig') as file:
        reader = csv.DictReader(file)
        to_db = []
        for row in reader:
            to_db.append((
                row['id_code'],
                row['Code'], # This is the foreign key to the 'shows' table
                int(row['Season']) if row['Season'].isdigit() else None,
                int(row['Episode']) if row['Episode'].isdigit() else None,
                float(row['Rating']) if row['Rating'] else None
            ))
        cursor.executemany('''
            INSERT OR IGNORE INTO episodes (id_code, parent_code, Season, Episode, Rating)
            VALUES (?, ?, ?, ?, ?)
        ''', to_db)
    print(f"Ingested data into 'episodes' table (duplicates were ignored).")

    conn.commit()
    conn.close()

def run_reports():
    """
    Connects to the database and runs the SQL queries to answer the
    questions from the assignment.
    """
    conn = sqlite3.connect('teleparty.db')
    cursor = conn.cursor()
    print("\n--- Generating Reports ---\n")

    # --- Query 1 & 2: Shows with a rating <= 5, and of those, which have > 1 season ---
    print("1. Shows with a rating less than or equal to 5:")
    cursor.execute('''
        SELECT Title, Rating
        FROM shows
        WHERE Rating <= 5.0
        ORDER BY Rating DESC;
    ''')
    results = cursor.fetchall()
    if results:
        for row in results:
            print(f"  - {row[0]} (Rating: {row[1]})")
    else:
        print("  No shows found with a rating of 5 or less.")
    print("-" * 30)

    print("2. Of the shows above, which have more than 1 season:")
    cursor.execute('''
        SELECT s.Title, COUNT(DISTINCT e.Season) as season_count
        FROM shows s
        JOIN episodes e ON s.Code = e.parent_code
        WHERE s.Rating <= 5.0
        GROUP BY s.Title
        HAVING season_count > 1
        ORDER BY season_count DESC;
    ''')
    results = cursor.fetchall()
    if results:
        for row in results:
            print(f"  - {row[0]} ({row[1]} seasons)")
    else:
        print("  No shows with a low rating also have more than 1 season.")
    print("-" * 30)

    # --- Query 3: Display the show(s) that have the highest rating count and the lowest rank ---
    print("3. Show(s) with the highest rating count and lowest rank:")
    cursor.execute('''
        SELECT Code, Title, RatingCount, Rank
        FROM shows
        ORDER BY RatingCount DESC, Rank ASC
        LIMIT 1;
    ''')
    result = cursor.fetchone()
    if result:
        show_code, show_title, rating_count, rank = result
        print(f"  - Show: {show_title}")
        print(f"    - Rating Count: {rating_count}")
        print(f"    - Rank: {rank}")

        # Sub-query to get episode and season counts from the 'episodes' table
        cursor.execute('''
            SELECT COUNT(*), COUNT(DISTINCT Season)
            FROM episodes
            WHERE parent_code = ?;
        ''', (show_code,))
        ep_count, season_count = cursor.fetchone()
        print(f"    - Total Episodes: {ep_count}")
        print(f"    - Total Seasons: {season_count}")
    else:
        print("  Could not determine the show with the highest rating count and lowest rank.")
    print("-" * 30)

    # --- Query 4: Display the show(s) that have the lowest rating count and the highest rank ---
    print("4. Show(s) with the lowest rating count and highest rank:")
    cursor.execute('''
        SELECT Code, Title, RatingCount, Rank
        FROM shows
        WHERE RatingCount IS NOT NULL
        ORDER BY RatingCount ASC, Rank DESC
        LIMIT 1;
    ''')
    result = cursor.fetchone()
    if result:
        show_code, show_title, rating_count, rank = result
        print(f"  - Show: {show_title}")
        print(f"    - Rating Count: {rating_count}")
        print(f"    - Rank: {rank}")

        # Sub-query to get episode and season counts
        cursor.execute('''
            SELECT COUNT(*), COUNT(DISTINCT Season)
            FROM episodes
            WHERE parent_code = ?;
        ''', (show_code,))
        ep_count, season_count = cursor.fetchone()
        print(f"    - Total Episodes: {ep_count}")
        print(f"    - Total Seasons: {season_count}")
    else:
        print("  Could not determine the show with the lowest rating count and highest rank.")
    print("-" * 30)

    conn.close()

if __name__ == '__main__':
    # Main execution block
    try:
        # 1. Create the database and tables
        create_database_and_tables()

        # 2. Ingest data from CSV files
        ingest_data()

        # 3. Run the reports
        run_reports()

    except FileNotFoundError as e:
        print(f"\nERROR: Could not find a CSV file. {e}")
        print("Please make sure your CSV files are in the same folder as the script.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
