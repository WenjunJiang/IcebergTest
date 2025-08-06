import pandas as pd
df = pd.DataFrame({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "age": [30, 25, 35]
})
df.to_parquet("data/sample.parquet")
