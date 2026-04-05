import requests
import time
import pandas as pd
from datetime import datetime

URL = "https://api.coingecko.com/api/v3/coins/markets"

def fetch(url):
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 50
    }

    all_data = []

    for page in range(1, 6):  # pagination
        params["page"] = page

        retries = 0
        max_retries = 5

        while retries < max_retries:
            try:
                response = requests.get(url, params=params, timeout=10)

                if response.status_code == 200:
                    data = response.json()

                    if not data:
                        break

                    all_data.extend(data)
                    break

                elif response.status_code == 429:
                    # rate limit handling
                    wait = 2 ** retries
                    time.sleep(wait)
                    retries += 1

                else:
                    print(f"Error: {response.status_code}")
                    break

            except Exception as e:
                wait = 2 ** retries
                time.sleep(wait)
                retries += 1

    # Convert to Pandas DataFrame
    df = pd.DataFrame(all_data)

    # Select only required columns
    df = df[[
        "id",
        "name",
        "current_price",
        "market_cap",
        "last_updated"
    ]]

    # Add ingestion timestamp
    df["ingestion_time"] = datetime.now()

    return df
pdf = fetch(URL)

df = spark.createDataFrame(pdf)

df.write.format("delta").mode("append").saveAsTable("crypto_dlt.source.crypto_raw")