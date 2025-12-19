
# Set your API key
import os
os.environ['OPENELECTRICITY_API_KEY'] = 'oe_3ZPqjof54kKpHxcpFqkpmYht'

# # **Market price and Demand**
import pandas as pd
import pandas as pd
import requests, csv   
from collections import defaultdict
import json, time
import paho.mqtt.client as mqtt
import json, time
from paho.mqtt import client as mqtt
import math


def get(url, params):
    r = requests.get(url, headers=H, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def markets_long_to_wide(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # sanity checks
    if "metric" not in df.columns or "value" not in df.columns:
        raise ValueError("Expected 'metric' and 'value' columns are missing.")

    # keep only what we care about and normalize
    df["metric"] = df["metric"].str.strip().str.lower()
    df["value"]  = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["ts"])  # drop bad timestamps

    # filter to the two metrics
    df = df[df["metric"].isin(["price", "demand"])]

    # Handle duplicates per (network_code, region, interval, ts, metric)
    # choose how to collapse duplicates: last/mean/max etc.
    collapsed = (
        df.sort_values("ts")
          .groupby(["network_code", "network_region", "interval", "ts", "metric"], dropna=False)["value"]
          .last()
    )

    #Unstack metrics into separate columns
    wide = collapsed.unstack("metric").reset_index()
    wide.columns.name = None

    #tidy order
    wide = wide.sort_values(["ts", "network_region"], kind="stable")
    return wide

# In[8]:


regions = ['VIC1', 'QLD1', 'SA1', 'NSW1', 'TAS1']
DATE_START = "2025-10-07T00:00:00"  # NEM time, timezone-naive
DATE_END   = "2025-10-14T00:00:00"
API = "https://api.openelectricity.org.au"
TOKEN = os.environ.get("OPENELECTRICITY_API_KEY") or "oe_3ZVkmJvbjLU4yPJCHaZXhBgS"
H = {"Authorization": f"Bearer {TOKEN}", "Accept":"application/json"}
frames = []
for r in regions:
    mresp = get(f"{API}/v4/market/network/NEM", {
        "metrics": ["price", "demand"],
        "network_region": r,           # one region per call
        "interval": "5m",
        "date_start": DATE_START,
        "date_end": DATE_END,
    })
    rows = []
    for block in mresp.get("data", []):
        metric = block.get("metric")
        unit   = block.get("unit")
        ivl    = block.get("interval")
        for series in block.get("results", []):
            region = (series.get("columns") or {}).get("network_region") or r
            for ts, val in series.get("data", []):
                rows.append({
                    "network_code": "NEM",
                    "network_region": region,
                    "metric": str(metric).lower(),
                    "value_unit": unit,
                    "interval": ivl,
                    "ts": pd.to_datetime(ts, utc=True, errors="coerce"),
                    "value": pd.to_numeric(val, errors="coerce"),
                })
    frames.append(pd.DataFrame(rows))

markets_long = pd.concat(frames, ignore_index=True)
markets_df   = markets_long_to_wide(markets_long)

# # **Data Cleaning for Market Data**

# In[ ]:


markets_df['network_region'] = markets_df['network_region'].astype(str).str.replace('1$', '', regex=True)
# Drop the 2nd and 3rd columns from markets_df (index 1 and 2)
markets_df = markets_df.drop(markets_df.columns[[0,2]], axis=1)

# In[ ]:
# Ensure correct data types
markets_df['ts'] = pd.to_datetime(markets_df['ts'], utc=True, errors='coerce')
markets_df['demand'] = pd.to_numeric(markets_df['demand'], errors='coerce')
markets_df['price'] = pd.to_numeric(markets_df['price'], errors='coerce')

# Broker info
BROKER_HOST = "broker.hivemq.com"
BROKER_PORT = 1883
CLIENT_ID    = "5339-ass2-market-publisher"
BASE_TOPIC   = "facility"

# MQTT client
client = mqtt.Client(client_id=CLIENT_ID, clean_session=True)
client.reconnect_delay_set(min_delay=1, max_delay=30)

def on_connect(cli, userdata, flags, rc):
    print(f"Connected with result code {rc}")

def on_publish(cli, userdata, mid):
    pass

client.on_connect  = on_connect
client.on_publish  = on_publish

client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
client.loop_start()


# In[ ]:

def publish_markets(markets_df, client, base_topic="market", qos=1, retain=False, row_delay=0.5):
    """Publish markets_df using an already-connected MQTT client."""
    # Make sure we're connected (but do NOT start another loop)
    if not client.is_connected():
        # brief wait for reconnect if previous loop is still negotiating
        for _ in range(40):  # ~10s max
            if client.is_connected():
                break
            time.sleep(0.25)

    print("\nPublishing data for markets_df")
    for _, row in markets_df.iterrows():
        # If the broker glitches, wait until we’re back
        while not client.is_connected():
            time.sleep(0.25)

        topic = f"{base_topic}/NEM"
        payload = json.dumps({
            "ts": row.ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "network_region": row.network_region,
            "demand": float(row.demand) if pd.notna(row.demand) else None,
            "price":  float(row.price)  if pd.notna(row.price)  else None,
        })

        info = client.publish(topic, payload, qos=qos, retain=retain)
        info.wait_for_publish()  # keep if you want QoS 1 guarantees

        print(f"Published → {topic}: {payload}")
        time.sleep(row_delay)

    print("\nFinished publishing markets_df data.")


# In[ ]:


publish_markets(markets_df, client, base_topic="market", qos=1, retain=False, row_delay=0.5)

client.loop_stop()
client.disconnect()
# In[ ]:




