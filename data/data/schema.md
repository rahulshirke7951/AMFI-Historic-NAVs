# AMFI NAV Data Schema

## Columns

- scheme_code → Unique fund identifier
- date → NAV date
- nav → Net Asset Value

## Data Type

- scheme_code → string
- date → datetime
- nav → float

## Rules

- Raw data only
- No modification allowed
- Used by ingestion engine
