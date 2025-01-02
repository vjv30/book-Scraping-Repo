import requests
import json
import sqlite3
import pandas as pd
import streamlit as st
import os

# Your Google Books API key
api_key = "AIzaSyBtmQTeAb2Q2MCm6AF90KT8NgHBMHAAW9w"

# Define paths for writable directories
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILEPATH = os.path.join(BASE_DIR, "book_data.db")
JSON_FILEPATH = os.path.join(BASE_DIR, "book_data.json")


def get_books(keyword, api_key, max_results=1000):
    all_results = []
    startIndex = 0
    while len(all_results) < max_results:
        url = f"https://www.googleapis.com/books/v1/volumes?q={keyword}&startIndex={startIndex}&maxResults=40&key={api_key}"
        response = requests.get(url)

        if response.status_code == 200:
            try:
                data = response.json()
                if "items" in data:
                    all_results.extend(data["items"])
                    startIndex += 40
                else:
                    break
            except json.JSONDecodeError:
                st.error("Failed to parse API response.")
                break
        else:
            st.error(f"Error fetching data: {response.status_code}")
            break

    return all_results[:max_results]


def save_to_sql(book_data, keyword, DB_FILEPATH):
    conn = sqlite3.connect(DB_FILEPATH)
    cursor = conn.cursor()

    # Create the table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS books (
            book_id TEXT PRIMARY KEY,
            search_key TEXT,
            book_title TEXT,
            book_subtitle TEXT,
            book_authors TEXT,
            book_description TEXT,
            industryIdentifiers TEXT,
            pageCount INT,
            categories TEXT,
            language TEXT,
            imageLinks TEXT,
            ratingsCount INT,
            averageRating REAL,
            country TEXT,
            saleability TEXT,
            isEbook BOOLEAN,
            amount_listPrice REAL,
            currencyCode_listPrice TEXT,
            amount_retailPrice REAL,
            currencyCode_retailPrice TEXT,
            buyLink TEXT,
            year TEXT
        )
    """)

    # Clear all old data from the table
    cursor.execute("DELETE FROM books")

    for item in book_data:
        volume_info = item.get("volumeInfo", {})
        sale_info = item.get("saleInfo", {})
        try:
            book_id = item["id"]
            book_title = volume_info.get("title", "")
            book_subtitle = volume_info.get("subtitle", "")
            book_authors = ", ".join(volume_info.get("authors", []))
            book_description = volume_info.get("description", "")
            industryIdentifiers = json.dumps(volume_info.get("industryIdentifiers", []))
            pageCount = volume_info.get("pageCount", None)
            categories = ", ".join(volume_info.get("categories", []))
            language = volume_info.get("language", "")
            imageLinks = json.dumps(volume_info.get("imageLinks", {}))
            ratingsCount = volume_info.get("ratingsCount", None)
            averageRating = volume_info.get("averageRating", None)
            country = sale_info.get("country", "")
            saleability = sale_info.get("saleability", "")
            isEbook = sale_info.get("isEbook", None)
            amount_listPrice = sale_info.get("listPrice", {}).get("amount", None)
            currencyCode_listPrice = sale_info.get("listPrice", {}).get("currencyCode", "")
            amount_retailPrice = sale_info.get("retailPrice", {}).get("amount", None)
            currencyCode_retailPrice = sale_info.get("retailPrice", {}).get("currencyCode", "")
            buyLink = sale_info.get("buyLink", "")
            year = volume_info.get("publishedDate", "")

            cursor.execute("""
                INSERT OR IGNORE INTO books (
                    book_id, search_key, book_title, book_subtitle, book_authors, book_description, 
                    industryIdentifiers, pageCount, categories, language, imageLinks, ratingsCount, 
                    averageRating, country, saleability, isEbook, amount_listPrice, currencyCode_listPrice, 
                    amount_retailPrice, currencyCode_retailPrice, buyLink, year
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (book_id, keyword, book_title, book_subtitle, book_authors, book_description,
                  industryIdentifiers, pageCount, categories, language, imageLinks, ratingsCount,
                  averageRating, country, saleability, isEbook, amount_listPrice, currencyCode_listPrice,
                  amount_retailPrice, currencyCode_retailPrice, buyLink, year))
        except KeyError as e:
            print(f"Error inserting book {item.get('id', '')}: Missing key {e}")
            continue

    conn.commit()
    conn.close()


def load_books_from_sql(keyword, DB_FILEPATH):
    conn = sqlite3.connect(DB_FILEPATH)
    cursor = conn.cursor()

    # Select only the columns you need
    cursor.execute("""
        SELECT book_id, search_key, book_title, book_subtitle, book_authors, 
               book_description, industryIdentifiers, pageCount, categories, 
               language, imageLinks, ratingsCount, averageRating, country, 
               saleability, isEbook, amount_listPrice, currencyCode_listPrice, 
               amount_retailPrice, currencyCode_retailPrice, buyLink, year
        FROM books WHERE search_key = ?
    """, (keyword,))

    rows = cursor.fetchall()
    conn.close()
    return rows


# Streamlit app logic
st.sidebar.title("vjv Book Scraping App")
page = st.sidebar.radio("Menu", ["Search Books", "Data Analyzing"])

if page == "Search Books":
    st.title("Book Search")

    keyword = st.text_input("Enter a keyword:")
    if st.button("Search"):
        if keyword:
            with st.spinner("Fetching data..."):
                books = get_books(keyword, api_key)
            if books:
                st.success(f"Found {len(books)} books for keyword: {keyword}")

                # Save to JSON file
                try:
                    with open(JSON_FILEPATH, "w") as f:
                        json.dump(books, f, indent=4)
                    st.success(f"Saved data to JSON at {JSON_FILEPATH}")
                except Exception as e:
                    st.error(f"Error saving JSON: {e}")

                # Save to SQLite
                save_to_sql(books, keyword, DB_FILEPATH)
                st.success(f"Saved data to SQLite database at {DB_FILEPATH}")

                # Display data from the database
                books_from_db = load_books_from_sql(keyword, DB_FILEPATH)
                df = pd.DataFrame(books_from_db, columns=[
                    "book_id", "search_key", "book_title", "book_subtitle", "book_authors",
                    "book_description", "industryIdentifiers", "pageCount", "categories", 
                    "language", "imageLinks", "ratingsCount", "averageRating", "country", 
                    "saleability", "isEbook", "amount_listPrice", "currencyCode_listPrice", 
                    "amount_retailPrice", "currencyCode_retailPrice", "buyLink", "year"
                ])
                st.dataframe(df)
            else:
                st.warning("No books found.")
        else:
            st.warning("Please enter a keyword.")

elif page == "Data Analyzing":
    st.title("SQL Query Analysis")

    # Define query options
    query_options = [
        "Check Availability of eBooks vs Physical Books",
        "Find the Publisher with the Most Books Published",
        "Identify the Publisher with the Highest Average Rating",
        "Get the Top 5 Most Expensive Books by Retail Price",
        "Find Books Published After 2010 with at Least 500 Pages",
        "List Books with Discounts Greater than 20%",
        "Find the Average Page Count for eBooks vs Physical Books",
        "Find the Top 3 Authors with the Most Books",
        "List Publishers with More than 10 Books",
        "Find the Average Page Count for Each Category",
        "Retrieve Books with More than 3 Authors",
        "Books with Ratings Count Greater Than the Average",
        "Books with the Same Author Published in the Same Year",
        "Books with a Specific Keyword in the Title",
        "Year with the Highest Average Book Price",
        "Count Authors Who Published 3 Consecutive Years",
        "Authors Published Books in Same Year, Different Publishers",
        "Average Retail Price of eBooks vs Physical Books",
        "Books with Ratings Far from Average (Outliers)",
        "Publisher with Highest Average Rating (Min 10 Books)"
    ]

    # Radio button to select the query
    selected_query = st.radio("Select a Query", query_options)

    # Define the SQL queries
    queries = {
        "Check Availability of eBooks vs Physical Books": """
            SELECT 
                CASE WHEN isEbook = 1 THEN 'eBooks' ELSE 'Physical Books' END AS book_type, 
                COUNT(*) AS count 
            FROM books 
            GROUP BY isEbook;
        """,
        "Find the Publisher with the Most Books Published": """
            SELECT 
                book_authors AS publisher, 
                COUNT(*) AS num_books 
            FROM books 
            GROUP BY book_authors 
            ORDER BY num_books DESC 
            LIMIT 10;
        """,
        "Identify the Publisher with the Highest Average Rating": """
            SELECT 
                book_authors AS publisher, 
                AVG(averageRating) AS avg_rating 
            FROM books 
            WHERE averageRating IS NOT NULL 
            GROUP BY book_authors 
            ORDER BY avg_rating DESC 
            LIMIT 10;
        """,
        "Get the Top 5 Most Expensive Books by Retail Price": """
            SELECT 
                book_title, 
                amount_retailPrice, 
                currencyCode_retailPrice 
            FROM books 
            WHERE amount_retailPrice IS NOT NULL 
            ORDER BY amount_retailPrice DESC 
            LIMIT 5;
        """,
        "Find Books Published After 2010 with at Least 500 Pages": """
            SELECT 
                book_title, 
                pageCount, 
                year 
            FROM books 
            WHERE year >= '2010' AND pageCount >= 500;
        """,
        "List Books with Discounts Greater than 20%": """
            SELECT 
                book_title, 
                amount_listPrice, 
                amount_retailPrice, 
                100 - (amount_retailPrice * 100.0 / amount_listPrice) AS discount_percentage 
            FROM books 
            WHERE amount_listPrice > 0 
                AND amount_retailPrice > 0 
                AND (100 - (amount_retailPrice * 100.0 / amount_listPrice)) > 20;
        """,
        "Find the Average Page Count for eBooks vs Physical Books": """
            SELECT 
                CASE WHEN isEbook = 1 THEN 'eBooks' ELSE 'Physical Books' END AS book_type, 
                AVG(pageCount) AS avg_page_count 
            FROM books 
            WHERE pageCount IS NOT NULL 
            GROUP BY isEbook;
        """,
        "Find the Top 3 Authors with the Most Books": """
            SELECT 
                book_authors, 
                COUNT(*) AS num_books 
            FROM books 
            GROUP BY book_authors 
            ORDER BY num_books DESC 
            LIMIT 3;
        """,
        "List Publishers with More than 10 Books": """
            SELECT 
                book_authors AS publisher, 
                COUNT(*) AS num_books 
            FROM books 
            GROUP BY book_authors 
            HAVING num_books > 10;
        """,
        "Find the Average Page Count for Each Category": """
            SELECT 
                categories, 
                AVG(pageCount) AS avg_page_count 
            FROM books 
            WHERE categories IS NOT NULL AND pageCount IS NOT NULL 
            GROUP BY categories 
            ORDER BY avg_page_count DESC;
        """,
        "Retrieve Books with More than 3 Authors": """
            SELECT 
                book_title, 
                book_authors 
            FROM books 
            WHERE LENGTH(book_authors) - LENGTH(REPLACE(book_authors, ',', '')) + 1 > 3;
        """,
        "Books with Ratings Count Greater Than the Average": """
            SELECT 
                book_title, 
                ratingsCount 
            FROM books 
            WHERE ratingsCount > (SELECT AVG(ratingsCount) FROM books);
        """,
        "Books with the Same Author Published in the Same Year": """
            SELECT 
                book_authors, 
                year, 
                COUNT(*) AS num_books 
            FROM books 
            WHERE book_authors IS NOT NULL AND year IS NOT NULL 
            GROUP BY book_authors, year 
            HAVING num_books > 1;
        """,
        "Books with a Specific Keyword in the Title": """
            SELECT 
                book_title 
            FROM books 
            WHERE book_title LIKE '%<keyword>%';
        """,
        "Year with the Highest Average Book Price": """
            SELECT 
                SUBSTR(year, 1, 4) AS publication_year, 
                AVG(amount_retailPrice) AS avg_price 
            FROM books 
            WHERE year IS NOT NULL 
            GROUP BY publication_year 
            ORDER BY avg_price DESC 
            LIMIT 1;
        """,
        "Count Authors Who Published 3 Consecutive Years": """
            SELECT 
                book_authors, 
                COUNT(DISTINCT SUBSTR(year, 1, 4)) AS consecutive_years 
            FROM books 
            WHERE year IS NOT NULL 
            GROUP BY book_authors 
            HAVING consecutive_years >= 3;
        """,
        "Authors Published Books in Same Year, Different Publishers": """
            SELECT 
                book_authors, 
                SUBSTR(year, 1, 4) AS publication_year, 
                COUNT(DISTINCT book_authors) AS num_publishers 
            FROM books 
            GROUP BY book_authors, publication_year 
            HAVING num_publishers > 1;
        """,
        "Average Retail Price of eBooks vs Physical Books": """
            SELECT 
                AVG(CASE WHEN isEbook = 1 THEN amount_retailPrice END) AS avg_ebook_price, 
                AVG(CASE WHEN isEbook = 0 THEN amount_retailPrice END) AS avg_physical_price 
            FROM books;
        """,
        "Books with Ratings Far from Average (Outliers)": """
            SELECT 
                book_title, averageRating, ratingsCount 
            FROM books 
            WHERE ABS(averageRating - (SELECT AVG(averageRating) FROM books)) > 
            (2 * (SELECT SUM((averageRating - (SELECT AVG(averageRating) FROM books)) * 
            (averageRating - (SELECT AVG(averageRating) FROM books))) 
          / COUNT(averageRating) 
       FROM books));

        """,
        "Publisher with Highest Average Rating (Min 10 Books)": """
            SELECT 
                book_authors AS publisher, 
                AVG(averageRating) AS avg_rating, 
                COUNT(*) AS num_books 
            FROM books 
            WHERE averageRating IS NOT NULL 
            GROUP BY book_authors 
            HAVING num_books > 10 
            ORDER BY avg_rating DESC 
            LIMIT 1;
        """
    }

    # Display query results for the selected query
    if selected_query:
        query = queries[selected_query]
        try:
            conn = sqlite3.connect(DB_FILEPATH)
            result_df = pd.read_sql_query(query, conn)
            conn.close()

            if not result_df.empty:
                st.write(f"Results for: {selected_query}")
                st.dataframe(result_df)
            else:
                st.warning("No results found for the selected query.")
        except Exception as e:
            st.error(f"Error executing query: {e}")
