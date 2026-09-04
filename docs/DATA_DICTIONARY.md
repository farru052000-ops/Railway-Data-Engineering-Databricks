# Railway Dataset Data Dictionary

## Source Columns

| Column | Description |
|---|---|
| `Train_No` | Unique train number or identifier |
| `Train_Name` | Name of the train |
| `Source_Station_Name` | Origin station of the train |
| `Destination_Station_Name` | Destination station of the train |
| `days` | Original day-related source value |

## Silver Layer Columns

| Column | Description |
|---|---|
| `Train_No` | Train identifier |
| `Train_Name` | Trimmed train name |
| `Source_Station_Name` | Trimmed and uppercase source station |
| `Destination_Station_Name` | Trimmed and uppercase destination station |
| `days` | Original source day value |
| `ingestion_timestamp` | Timestamp captured during Bronze ingestion |
| `source_file` | Source file path captured during ingestion |
| `days_clean` | Trimmed day value |
| `day_name` | Normalized day name with trailing `d` removed |
| `day_type_flag` | Indicates whether the original value ended with `d` |

## Gold Tables

### gold_station_summary

Provides train record counts by source station.

### gold_day_summary

Provides train record counts and percentage of total by normalized day.

### gold_route_summary

Provides train record counts by source and destination station.

### gold_train_category

Provides train record counts grouped into weekday and weekend categories.

## Data Quality Rules

The project validates:

- NULL values
- Duplicate records
- Empty station names
- Invalid day values

Only data that passes the quality gate proceeds to the Gold layer.
