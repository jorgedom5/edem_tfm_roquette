import pandas as pd
import os

for entry in os.scandir("./data/values"):
    if entry.name.endswith(".csv") and entry.is_file():
        file_name = entry.name[:-4]
        df = pd.read_csv(entry.path)
        parquet_path = os.path.join("data/values", f"{file_name}.parquet")
        df.to_parquet(parquet_path)
        print(f"Converted {entry.name} to {parquet_path}")
