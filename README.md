# Chicago Transit Equity

Pipeline for analyzing CTA transit feedback from social media (Reddit and Bluesky) to understand rider sentiment across routes.

## Project Structure

```
chicago_transit_equity/
├── cta_pipeline/           # Core NLP pipeline modules
│   ├── data_fetch.py       # Rate limiting, retry logic, anonymization
│   ├── text_processing.py  # Text cleaning and normalization
│   ├── route_extraction.py # Extract CTA routes from text
│   ├── transit_classification.py  # Classify transit-related content
│   ├── feedback_classification.py # Identify actionable feedback
│   ├── sentiment_analysis.py      # Route-specific sentiment scoring
│   ├── stop_extraction.py  # Extract stop mentions, detect sarcasm
│   ├── time_extraction.py  # Extract time-of-day references
│   └── ...
├── precompute/             # Precomputation scripts for dashboard
│   ├── compute_school_stops.py    # Match schools to nearby stops
│   ├── aggregate_sentiment.py     # Aggregate sentiment by route
│   └── simplify_routes.py         # Simplify route geometries
├── dashboard/              # Streamlit dashboard
├── notebooks/              # Demo notebooks
├── data/                   # Data directory (see below)
├── reddit_data_fetch.py    # Fetch Reddit posts/comments
├── atproto_data_fetch.py   # Fetch Bluesky posts
├── reddit_data_pipe.py     # Process Reddit data through pipeline
└── atproto_data_pipe.py    # Process Bluesky data through pipeline
```

## Data Directory Layout

After setup, the data directory will contain:

```
data/
├── cps_data/           # Chicago Public Schools locations (included)
├── gtfs/               # CTA GTFS data (download required)
│   ├── stops.txt
│   ├── routes.txt
│   └── shapes.txt
├── posts/              # Raw social media data (fetch required)
│   ├── reddit/
│   └── bsky/
└── processed/          # Pipeline output (generated)
    ├── reddit/
    └── bsky/
```

## Setup Instructions

### 1. Clone the repository

```bash
git clone --recursive https://github.com/acoffeerunner/chicago_transit_equity
cd chicago_transit_equity
```

### 2. Install dependencies

```bash
# Full installation with all features
pip install ".[all]"

# Or install specific extras
pip install ".[fetch]"      # For data fetching
pip install ".[dashboard]"  # For dashboard only
pip install ".[notebooks]"  # For Jupyter notebooks
```

### 3. Download GTFS data

Download CTA GTFS data from the [CTA Developer Center](https://www.transitchicago.com/developers/gtfs/) and extract to `data/gtfs/`.

### 4. Configure API credentials

Copy the example environment file and add your credentials:

```bash
cp .env.example .env
# Edit .env with your Reddit and Bluesky credentials
```

### 5. Fetch social media data

```bash
# Fetch Reddit posts
python reddit_data_fetch.py

# Fetch Bluesky posts
python atproto_data_fetch.py
```

### 6. Run the NLP pipeline

```bash
# Process Reddit data
python reddit_data_pipe.py

# Process Bluesky data
python atproto_data_pipe.py
```

### 7. Generate precomputed data for dashboard

```bash
python -m precompute.compute_school_stops
python -m precompute.aggregate_sentiment
python -m precompute.simplify_routes
```

### 8. Run the dashboard

```bash
cd dashboard
streamlit run app.py
```

## Pipeline Features

- **Transit Classification**: Uses SBERT embeddings to identify CTA-related content
- **Route Extraction**: Regex patterns for all CTA bus and train routes
- **Feedback Detection**: Semantic similarity to identify actionable rider feedback
- **Sentiment Analysis**: Route-specific sentiment with sarcasm detection
- **Time-of-Day Extraction**: Identifies when transit events occurred
- **Context Inheritance**: Propagates route context from posts to their comments

## Dashboard

The Streamlit dashboard provides:

- Interactive map of CTA routes and Chicago schools
- Sentiment breakdown by route and time of day
- Top positive/negative feedback posts
- School-to-route transit access analysis

See [dashboard/README.md](dashboard/README.md) for more details.

## License

MIT License - see [LICENSE](LICENSE) for details.
