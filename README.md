# Book Scraping and Analysis App

This is a Python-based web app that allows users to search for books using a keyword, fetches detailed book information from the Google Books API, and saves the results in both an SQLite database and a JSON file. The app also includes data analysis features with predefined SQL queries to analyze the collected book data.

## Features

- **Streamlit Interface**: A simple web interface for interacting with the app.
- **Book Search**: Search for books by keyword, retrieve detailed data (title, authors, descriptions, ratings, etc.), and save the results to SQLite and JSON files.
- **Data Analysis**: Perform SQL queries to analyze the book data, including comparisons like eBooks vs physical books, identifying top publishers, and finding the most expensive books.

## Requirements

Make sure you have the following Python libraries installed:

- `requests`: For making API calls to Google Books API.
- `json`: For handling JSON data.
- `sqlite3`: For interacting with the SQLite database.
- `pandas`: For data manipulation and analysis.
- `streamlit`: For building the web interface.
- `seaborn , matplotlib` : For visualization

You can install the required libraries by running:

```bash
pip install requests pandas streamlit matplotlib seaborn plotly
```

you can run this code by using your own API Key and then run using
```bash
streamlit run app.py
```
