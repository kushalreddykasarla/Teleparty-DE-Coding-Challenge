Teleparty Data Engineering Coding Challenge

This project contains a Python script that serves as a complete solution for the Teleparty Data Engineering Coding Challenge. The application ingests TV show data from several CSV files, loads it into a local SQLite database, and then runs a series of queries to generate a report based on the assignment's requirements.
How to Run
Prerequisites

    Python 3

    The following CSV files must be in the same directory as the script:

        all-series-ep-average.csv

        all-episode-ratings.csv

        top-seasons-full.csv (Note: This file is not used for ingestion but is required to be present).

Execution

To run the application, navigate to the project directory in your terminal and execute the following command:

python assignment.py

The script will perform all necessary steps automatically:

    Create a new database file named teleparty.db.

    Define the database schema and create the required tables.

    Clean and ingest the data from the CSV files.

    Run the required queries and print a formatted report to the console.

Design Decisions

As part of the challenge, several key data engineering decisions were made to handle real-world data quality issues:

    Database Choice (SQLite): SQLite was chosen as the relational database because it is serverless, self-contained, and requires no complex setup. It's a lightweight and portable solution perfect for this type of command-line application.

    Data Source Selection: The top-seasons-full.csv file was intentionally excluded from the data ingestion process. Upon inspection, it was determined that this file lacked a reliable foreign key to accurately link its season data back to the main shows table. All necessary season and episode metrics could be derived more reliably from the other two files.

    Handling Inconsistent Primary Keys: The id_code column in all-episode-ratings.csv was found to contain duplicate values, making it unsuitable as a primary key. To ensure data integrity, a composite primary key was created for the episodes table using (parent_code, Season, Episode). This guarantees that each episode is stored uniquely. The script uses an INSERT OR IGNORE command to gracefully handle and skip any duplicate episode entries found in the source file.

    Data Cleaning: The Rating Count column in the source data contained numeric values formatted with commas (e.g., "5,431"). The ingestion process includes a cleaning step to remove these commas before converting the values to integers, preventing data type errors.

    Interpreting Requirements: The reporting queries were carefully structured to match the indentation and hierarchy of the questions in the assignment PDF. For instance, the question about shows with more than one season was treated as a sub-question, filtering only from the pool of shows that had a low rating.

Generated Report

The application will generate a report that answers the following questions:

    Shows with a rating less than or equal to 5: A list of all shows with a low rating.

    Of the shows above, which have more than 1 season: A sub-list of the low-rated shows that also have multiple seasons.

    Show(s) with the highest rating count and lowest rank: Details of the top-performing show, including its total episode and season count.

    Show(s) with the lowest rating count and highest rank: Details of the bottom-performing show, including its total episode and season count.
