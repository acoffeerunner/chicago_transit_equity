# CTA Transit Equity Dashboard

Streamlit dashboard for visualizing CTA transit sentiment analysis.

## Structure

```
dashboard/
├── app.py              # Main Streamlit application
├── components/         # UI components
│   ├── sidebar.py      # Filter controls (routes, time of day, sources)
│   ├── map_display.py  # Folium map with routes and schools
│   ├── sentiment_charts.py  # Plotly sentiment visualizations
│   └── post_previews.py     # Sample post cards
├── utils/              # Utility functions
│   ├── data_loader.py  # Parquet data loading with caching
│   └── route_mapping.py # GTFS/sentiment route conversions
├── precomputed/        # Precomputed data files
│   ├── route_sentiment.parquet
│   ├── school_stops.parquet
│   ├── top_posts.parquet
│   ├── sentiment_time_series.parquet
│   └── routes_simplified.json
└── requirements.txt    # Dashboard-specific dependencies
```

## Running Locally

```bash
cd dashboard
pip install -r requirements.txt
streamlit run app.py
```

The dashboard will be available at `http://localhost:8501`.

## Features

### Interactive Map
- CTA train and bus routes with official colors
- Chicago Public Schools locations
- Route highlighting on selection
- School-to-stop distance visualization

### Sentiment Analysis
- Overall sentiment breakdown (positive/negative/neutral)
- Sentiment by time of day (morning/afternoon/evening/night)
- Source comparison (Reddit vs Bluesky)
- Sarcasm detection rates

### Feedback Posts
- Top positive and negative posts per route
- Expandable post previews
- Source and timestamp metadata

### Filtering
- Route selection (trains and buses)
- Time of day filtering
- Data source selection
- School-based route filtering

## Data Requirements

Before running the dashboard, ensure precomputed data exists in `precomputed/`. Generate it using:

```bash
# From project root
python -m precompute.compute_school_stops
python -m precompute.aggregate_sentiment
python -m precompute.simplify_routes
```

## Dependencies

- streamlit >= 1.28.0
- pandas >= 2.0.0
- folium >= 0.14.0
- streamlit-folium >= 0.15.0
- plotly >= 5.18.0
- pyarrow >= 14.0.0
- shapely >= 2.0.0
