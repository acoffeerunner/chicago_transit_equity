# CTA GTFS Data

This directory should contain CTA General Transit Feed Specification (GTFS) data.

## Download Instructions

1. Download the GTFS data from CTA:
   ```bash
   curl -o google_transit.zip https://www.transitchicago.com/downloads/sch_data/google_transit.zip
   ```

2. Extract the contents to this directory:
   ```bash
   unzip google_transit.zip -d .
   ```

3. You should have the following files:
   - `agency.txt`
   - `calendar.txt`
   - `calendar_dates.txt`
   - `routes.txt`
   - `shapes.txt`
   - `stop_times.txt`
   - `stops.txt`
   - `trips.txt`

## Quick Setup

Run this from the `data/gtfs` directory:

```bash
curl -o google_transit.zip https://www.transitchicago.com/downloads/sch_data/google_transit.zip && unzip google_transit.zip && rm google_transit.zip
```

## Data Usage

The GTFS data is used by:
- `precompute/compute_school_stops.py` - Find nearest stops for each school
- `precompute/simplify_routes.py` - Generate simplified route geometries for the map
- `cta_pipeline/gtfs_loader.py` - Load stop data for the pipeline

## Update Frequency

CTA updates the GTFS feed periodically. Re-download if you need the latest schedule data.
