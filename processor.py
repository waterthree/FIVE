import openai
from db import NewsDatabase
from config import DB_CONFIG, OPENAI_API_KEY  # Import configurations

# Initialize the database with the configuration
db = NewsDatabase(**DB_CONFIG)

# Set OpenAI API key
openai.api_key = OPENAI_API_KEY

# Fetch raw news from the database
def fetch_raw_news() -> list:
    conn = db.connect()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT title, summary FROM news")
            raw_news = cursor.fetchall()
            return raw_news
    finally:
        db.release_connection(conn)

# Deduplicate and rank news using LLM
def deduplicate_and_rank(news_list: list) -> list:
    openai.api_key = "your_openai_api_key"
    deduplicated_news = []

    for news in news_list:
        title, summary = news
        response = openai.ChatCompletion.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a helpful assistant that deduplicates news articles."},
                {"role": "user", "content": f"Is this news similar to any in {deduplicated_news}? News: {title}"}
            ]
        )
        if "no" in response.choices[0].message.content.lower():
            deduplicated_news.append((title, summary, 1))  # Initial rank is 1
        else:
            # If similar, increase the rank of the existing news
            for idx, (t, s, r) in enumerate(deduplicated_news):
                if t == title:
                    deduplicated_news[idx] = (t, s, r + 1)
                    break

    return deduplicated_news

# Insert aggregated news into the database
def insert_aggregated_news(news_list: list):
    conn = db.connect()
    try:
        with conn.cursor() as cursor:
            for title, summary, rank in news_list:
                cursor.execute(
                    "INSERT INTO aggregated_news (title, summary, rank) VALUES (%s, %s, %s)",
                    (title, summary, rank)
                )
            conn.commit()
    finally:
        db.release_connection(conn)

# Main deduplication process
raw_news = fetch_raw_news()
deduplicated_news = deduplicate_and_rank(raw_news)
insert_aggregated_news(deduplicated_news)

# Close the database connection pool
db.close()