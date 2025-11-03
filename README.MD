# Snowflake Feature Engineering Project

This project demonstrates an end-to-end feature engineering and machine learning workflow using Snowflake.

## Workflow

1.  **Extract (Snowflake SQL):** Raw data is pulled from `SNOWFLAKE_SAMPLE_DATA`.
2.  **Transform (Snowflake SQL):** Features (aggregations, encodings) are engineered using a SQL query.
3.  **Load (Snowflake SQL):** A Snowflake `VIEW` (`CUSTOMER_FEATURE_STORE`) is created to act as a real-time, lightweight feature store.
4.  **Access for ML (Python):** A Python script connects to Snowflake, queries the feature store view, and trains a scikit-learn model.

## The Pivot (Snowpark vs. Python)

The original plan was to use a Snowpark Python Worksheet (100% in-Snowflake). However, the trial account environment ran into a package resolution error (`P0000`) preventing `scikit-learn` from being installed.

To prove the feature store is functional, I pivoted to a local Python script (`train.py`) which successfully connects to the Snowflake feature store, retrieves the features, and trains the model.

## Files

* `feature_engineering.sql`: The SQL code for Steps 1-3.
* `train.py`: The Python script for Step 4 (model training).
* `presentation.pptx`: The project presentation.
