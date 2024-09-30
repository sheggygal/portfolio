import requests
import pandas as pd
import json
import os


# Function to load API key from JSON file
def load_api_key(file_path='tumblr_credentials.json'):
    with open(file_path, 'r') as file:
        config = json.load(file)
        return config['api_key']


# Load API key
API_KEY = load_api_key()


# Function to fetch posts using the public API key
def fetch_posts(keyword, limit=10, before=None):
    url = f'https://api.tumblr.com/v2/tagged'
    params = {
        'tag': keyword,
        'api_key': API_KEY,
        'limit': limit,
        'filter': 'raw'
    }
    if before:
        params['before'] = before

    response = requests.get(url, params=params)
    response.raise_for_status()  # Raise an exception for HTTP errors
    return response.json()['response']


# Load existing data or initialize empty DataFrames
def load_existing_data(posts_file='tumblr_posts.csv', comments_file='tumblr_comments.csv'):
    if os.path.exists(posts_file):
        posts_df = pd.read_csv(posts_file)
    else:
        posts_df = pd.DataFrame(columns=['id', 'series', 'title', 'post_text', 'likes', 'shares', 'reblogs'])

    if os.path.exists(comments_file):
        comments_df = pd.read_csv(comments_file)
    else:
        comments_df = pd.DataFrame(columns=['post_id', 'comment'])

    return posts_df, comments_df


# Function to process and insert post data
def process_posts(posts, posts_df, comments_df, series):
    # Ensure 'shares', 'reblogs', and 'series' columns exist in posts_df
    if 'shares' not in posts_df.columns:
        posts_df['shares'] = 0
    if 'reblogs' not in posts_df.columns:
        posts_df['reblogs'] = 0
    if 'series' not in posts_df.columns:
        posts_df['series'] = ''

    post_rows = []
    comment_rows = []

    for post in posts:
        post_data = {
            'id': post['id'],
            'series': series,
            'title': post.get('summary', ''),
            'post_text': post['trail'][0]['content'] if 'trail' in post and len(post['trail']) > 0 else '',
            'likes': post.get('note_count', 0),
            'shares': post.get('reblogged_from', {}).get('share_count', 0),  # Extract shares
            'reblogs': post.get('reblogged_from', {}).get('reblog_count', 0)  # Extract reblogs
        }
        post_rows.append(post_data)

        # Fetch comments (if available)
        if 'notes' in post:
            for note in post['notes']:
                if note['type'] == 'reply':
                    comment_data = {
                        'post_id': post['id'],
                        'comment': note['reply_text']
                    }
                    comment_rows.append(comment_data)

    posts_df_update = pd.DataFrame(post_rows)  # Create a temporary DataFrame for new data
    posts_df_update.set_index('id', inplace=True)

    # Update shares, reblogs, and series for existing posts in posts_df
    posts_df.update(posts_df_update[['shares', 'reblogs', 'series']])

    # Merge new data while preserving existing data
    posts_df_update.reset_index(inplace=True)
    posts_df = pd.concat([posts_df, posts_df_update]).drop_duplicates(subset=['id'], keep='last').reset_index(drop=True)

    comments_df = pd.concat([comments_df, pd.DataFrame(comment_rows)], ignore_index=True)

    return posts_df, comments_df


# Main function to fetch and store data
def main():
    posts_df, comments_df = load_existing_data(posts_file='tumblr_posts.csv)
    series_list = ["The Mandalorian", "The Book of Boba Fett", "Obi-Wan Kenobi", "Andor", "Ahsoka", "The Acolyte"]

    for series in series_list:
        print(f"Fetching posts for {series}...")
        posts = fetch_posts(series, limit=30)
        posts_df, comments_df = process_posts(posts, posts_df, comments_df, series)

    # Save to CSV files
    posts_df.to_csv('tumblr_posts.csv', index=False)
    comments_df.to_csv('tumblr_comments.csv', index=False)


if __name__ == '__main__':
    main()
