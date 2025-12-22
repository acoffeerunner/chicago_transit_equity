# Data Directory

This directory contains all data files for the CTA Transit Equity Dashboard.

## Directory Structure

```
data/
├── cps_data/           # Chicago Public Schools data (included in repo)
│   ├── *.geojson       # School locations
│   └── chicago-community-areas.geojson
│
├── gtfs/               # CTA GTFS transit data (download required)
│   ├── stops.txt       # Bus stops and train stations
│   ├── routes.txt      # Route definitions
│   ├── shapes.txt      # Route geometries
│   └── ...             # Other GTFS files
│
├── posts/              # Raw social media data (fetch required)
│   ├── reddit/         # Reddit posts and comments
│   │   ├── reddit_posts.csv
│   │   └── reddit_comments.csv
│   └── bsky/           # Bluesky posts and comments
│       ├── bsky_posts.csv
│       └── bsky_comments.csv
│
└── processed/          # Pipeline output (generated)
    ├── reddit/         # Processed Reddit data
    │   └── reddit_transit_feedback_labeled.csv
    └── bsky/           # Processed Bluesky data
        └── bsky_transit_feedback_labeled.csv
```

## Setup Instructions

### 1. GTFS Data (Required)

Download CTA transit data:

```bash
cd data/gtfs
curl -o google_transit.zip https://www.transitchicago.com/downloads/sch_data/google_transit.zip
unzip google_transit.zip && rm google_transit.zip
```

### 2. Social Media Data (Optional - for full pipeline)

**Reddit** (no authentication required):
```bash
python reddit_data_fetch.py
```

**Bluesky** (requires authentication):
```bash
export BSKY_USERNAME=your-username.bsky.social
export BSKY_PASSWORD=your-app-password
python bsky_data_fetch.py
```

### 3. Run Processing Pipeline

```bash
# Process Reddit data
python reddit_data_pipe.py

# Process Bluesky data
python atproto_data_pipe.py
```

### 4. Run Precomputation

```bash
# Compute school-stop relationships
python -m precompute.compute_school_stops

# Aggregate sentiment data
python -m precompute.aggregate_sentiment

# Simplify route geometries for map
python -m precompute.simplify_routes
```

## Data Sources

| Data        | Source                             | Update Frequency |
|-------------|------------------------------------|------------------|
| GTFS        | [CTA GTFS Feed](https://www.transitchicago.com/downloads/sch_data/google_transit.zip)                    | Periodic         |
| CPS Schools | Chicago Data Portal                | Annual           |
| Reddit      | Reddit API (via fetch script)      | On demand        |
| Bluesky     | AT Protocol API (via fetch script) | On demand        |
