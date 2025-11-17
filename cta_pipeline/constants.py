"""Constants used throughout the CTA pipeline."""

# File paths - Raw data (output from fetch scripts)
RAW_DATA_DIR_BSKY = "data/posts/bsky"
RAW_DATA_DIR_REDDIT = "data/posts/reddit"
POSTS_PATH_BSKY = "data/posts/bsky/bsky_posts.csv"
COMMENTS_PATH_BSKY = "data/posts/bsky/bsky_comments.csv"
POSTS_PATH_REDDIT = "data/posts/reddit/reddit_posts.csv"
COMMENTS_PATH_REDDIT = "data/posts/reddit/reddit_comments.csv"

# Reddit subreddits to fetch from
REDDIT_SUBREDDITS = [
    "Chicago",
    "AskChicago",
    "CarFreeChicago",
    "AskCHI",
    "cta",
    "ChicagoUrbanism",
    "WindyCity",
    "ChicagoNWSide",
    "greatNWSide",
]

# Bluesky search queries
BLUESKY_QUERIES = [
    "cta AND train",
    "cta AND bus",
    "cta AND line",
    "chicago AND train",
    "chicago AND bus",
    "chicago AND line",
]
