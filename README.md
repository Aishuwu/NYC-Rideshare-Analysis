# NYC Rideshare Analysis Using PySpark

## Introduction

This project presents a comprehensive analysis of New York City rideshare data using Apache Spark. Through the integration of multiple datasets and advanced data processing techniques, it derives actionable insights relevant to drivers, platforms, and investors. The analysis includes trend identification, profitability comparisons, route popularity, anomaly detection, and service optimization strategies.

## Table of Contents

- [Introduction](#introduction)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Dataset](#dataset)
- [Analysis Tasks](#analysis-tasks)
- [Key Insights](#key-insights)
- [Dependencies](#dependencies)
- [Troubleshooting](#troubleshooting)
- [License](#license)

## Features

- Merges rideshare and taxi zone datasets for enriched spatial insights.
- Monthly aggregation of trip counts, platform profits, and driver earnings.
- Top-K analysis for boroughs and most profitable routes.
- Time-based average trip length and earnings evaluations.
- Anomaly detection in waiting times.
- Custom filtering and route analysis by time of day and location.

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/Aishuwu/nyc-rideshare-analysis.git
   cd nyc-rideshare-analysis
   ```

2. (Optional) Create and activate a virtual environment:
   ```bash
   python -m venv env
   source env/bin/activate  # On Windows: env\Scripts\activate
   ```

3. Install required Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

1. Ensure you have Apache Spark and PySpark installed.
2. Extract and explore the project files from the ZIP archive.
3. Run the Spark jobs (either through notebooks or `.py` scripts) corresponding to each task:
   - Data merging and schema transformation
   - Monthly aggregations and route analysis
   - Anomaly and trend detection
4. Visualizations and summary outputs will be generated for review and interpretation.

## Dataset

- **rideshare_data.csv**: Contains detailed records of trip data.
- **taxi_zone_lookup.csv**: Provides location metadata including boroughs and zones.

## Analysis Tasks

### Task 1: Data Merging & Transformation
- Join rideshare data with NYC taxi zone information.
- Convert timestamps and prepare schema for analysis.

### Task 2: Monthly Aggregations
- Total trip count, platform profits, and driver earnings by business and month.

### Task 3: Top-K Analysis
- Identify top 5 pickup and dropoff boroughs per month.
- Rank the 30 most profitable routes by driver pay.

### Task 4: Averages by Time of Day
- Analyze average trip lengths and driver earnings across time slots.

### Task 5: Anomaly Detection
- Detect days with irregular waiting times, notably spikes in January.

### Task 6: Filtering & Custom Querying
- Evaluate borough-level trip patterns by time of day.
- Focus on specific trip patterns, e.g., Brooklyn to Staten Island.

### Task 7: Route Popularity
- Determine top 10 routes based on trip frequency.
- Separate rankings for Uber and Lyft services.

## Key Insights

- **Uber leads in volume and profitability** compared to Lyft across all months.
- **January 1st shows abnormal wait times**, likely due to holiday demand.
- **Manhattan dominates** as a key borough for both pickups and dropoffs.
- **Airport routes (e.g., JFK)** are among the most frequent and profitable.
- **Evening hours** tend to have higher trip volumes and demand.

## Dependencies

```
pyspark
pandas
matplotlib
seaborn
```

Install dependencies with:
```bash
pip install -r requirements.txt
```

## Troubleshooting

- **Spark not found**: Make sure `SPARK_HOME` is set and PySpark is installed.
- **File path issues**: Check relative paths if loading from notebooks or scripts.
- **Memory limits**: Use Spark configuration options to allocate more resources if needed.

## License

This project is intended for educational and analytical purposes only.
