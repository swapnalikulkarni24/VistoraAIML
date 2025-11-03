import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from snowflake.connector import connect

# --- 1. FILL IN YOUR DETAILS HERE ---
SNOWFLAKE_USER = "SWAPNALIKULKARNI"
SNOWFLAKE_PASSWORD = "Swapnali#2462004"
SNOWFLAKE_ACCOUNT = "dsbotub-et48428"
SNOWFLAKE_DATABASE = "MY_PROJECT_DB"
SNOWFLAKE_SCHEMA = "PUBLIC"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH" # Your trial warehouse
SNOWFLAKE_ROLE = "ACCOUNTADMIN"

print("Connecting to Snowflake...")
conn = connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA,
    role=SNOWFLAKE_ROLE
)
print("Connection successful.")

# --- 2. ACCESS THE FEATURE STORE ---
print("Accessing Feature Store View (CUSTOMER_FEATURE_STORE)...")
# This is how we "access" the features for ML
query = "SELECT TOTAL_ORDERS, ACCOUNT_BALANCE, TOTAL_SPEND FROM MY_PROJECT_DB.PUBLIC.CUSTOMER_FEATURE_STORE"
cursor = conn.cursor()
cursor.execute(query)

# Fetch the data into a pandas DataFrame
model_df = cursor.fetch_pandas_all()
print(f"Successfully fetched {len(model_df)} records.")

# --- 3. TRAIN THE MODEL (Locally) ---
model_df = model_df.dropna()
print(f"Data for training (first 5 rows):\n{model_df.head()}")

features = ["TOTAL_ORDERS", "ACCOUNT_BALANCE"]
target = "TOTAL_SPEND"

X = model_df[features]
y = model_df[target]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

print("Training Linear Regression model...")
model = LinearRegression()
model.fit(X_train, y_train)

# --- 4. SHOW RESULTS ---
print("\n--- MODEL TRAINING COMPLETE ---")
print(f"Model Intercept: {model.intercept_}")
print(f"Model Coefficients: {list(zip(features, model.coef_))}")
print("---------------------------------")

cursor.close()
conn.close()