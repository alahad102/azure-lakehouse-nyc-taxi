import pandas as pd

yellow = pd.read_parquet("data/yellow_tripdata_2023-01.parquet")
green = pd.read_parquet("data/green_tripdata_2023-01.parquet")

print("=== YELLOW TAXI ===")
print("Shape:", yellow.shape)
print("Columns:", list(yellow.columns))

print("\n=== GREEN TAXI ===")
print("Shape:", green.shape)
print("Columns:", list(green.columns))