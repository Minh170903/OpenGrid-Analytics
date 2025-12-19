# # Setting up the page

# We will use the Streamlit framework to create our web-app dashboard.

from threading import Lock
from datetime import datetime, timedelta, UTC
from paho.mqtt import client as mqtt
import streamlit as st
from streamlit_autorefresh import st_autorefresh
import plotly.graph_objects as go
import plotly.express as px
import json
import pandas as pd
import time

# auto-refresh page every 2 seconds 
st_autorefresh(interval=5000, key="live_refresh")


# PAGE CONFIGURATION

st.set_page_config(
    page_title="NEM Electricity Monitor Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS - Dark blue and purple theme
st.markdown("""
<style>
    /* Main theme colors */
    :root {
        --primary-blue: #1e3a8a;
        --primary-purple: #7c3aed;
        --dark-bg: #0f172a;
        --card-bg: #1e293b;
    }

    /* Background */
    .stApp {
        background: linear-gradient(135deg, #1e3a8a 0%, #312e81 50%, #7c3aed 100%);
    }

    /* Headers */
    h1, h2, h3 {
        color: #e0e7ff !important;
    }

    /* Cards */
    .metric-card {
        background: rgba(30, 41, 59, 0.8);
        backdrop-filter: blur(10px);
        padding: 20px;
        border-radius: 15px;
        border: 2px solid rgba(124, 58, 237, 0.3);
        box-shadow: 0 8px 32px rgba(124, 58, 237, 0.2);
    }
    
    /* Market card with chart background */
    .market-card-with-chart {
        position: relative;
        background: rgba(30, 41, 59, 0.5); 
        backdrop-filter: blur(10px);
        padding: 20px;
        border-radius: 15px;
        border: 2px solid rgba(124, 58, 237, 0.3);
        box-shadow: 0 8px 32px rgba(124, 58, 237, 0.2);
        overflow: hidden;
    }
    
    
    .market-card-content {
        position: relative;
        z-index: 2;  /* Above chart */
    }

    /* Sidebar */
    section[data-testid="stSidebar"] {
        background: rgba(15, 23, 42, 0.95);
        border-right: 2px solid rgba(124, 58, 237, 0.3);
    }

    /* Fixed filter panel */
    .fixed-filter-panel {
        position: sticky;
        top: 0;
        z-index: 999;
        background: rgba(15, 23, 42, 0.98);
        padding: 20px;
        border-radius: 10px;
        margin-bottom: 5px;
        box-shadow: 0 4px 16px rgba(0, 0, 0, 0.3);
    }

    /* Filters */
    .stMultiSelect, .stSelectbox {
        background: rgba(30, 41, 59, 0.8);
        border-radius: 10px;
    }

</style>
""", unsafe_allow_html=True)

# # Subcscribing to MQTT topics

# MQTT client library is loaded to allow subscribing to the 2 required topics.
# We use `lock` from `threading` library to ensure thread-safe data access when MQTT runs in the background and Dash runs in the visualization thread continuously - both accessing the same variables.
# We set up the connection details using the same broker (HiveMQ a public broker), same port (default unencrypted port 1883) as publishing side, with a new client ID to represent the dashboard on the subscriber side.

# setting up MQTT connection
BROKER = "broker.hivemq.com"
PORT = 1883
CLIENT_ID = "as2_t1g10_dashboard"


# When a connection is established with the publisher, `on_connect` is called to subscribe to the right topics.

def on_connect(client, userdata, flags, rc):
    """called after connection is established"""

    if rc == 0:  # check if the connection is active
        print("Connected to OpenElectricity MQTT publisher - proceed to subscribe now")
        # subscribe to all subtopics under facility
        client.subscribe("facility/#")
        client.subscribe("market/#")  # subscribe to all subtopics under market


# Now let's set up the storage to keep received data, we can use simple dictionaries for fast lookup by key. Once messages arrive, we will use 2 handler functions to update the corresponding data storage structures using `with data_lock` block.
#
# The use of `data_lock` ensures exclusive access - when a thread enters a `with data_lock` block, it acquires the lock, blocking other threads from entering any other `with data_lock` block until the lock is released - preventing corruption when multiple threads updating the same data objects. So when the background MQTT thread is updating, any other thread that also uses `with data_lock` to read or write must wait. This guarantees the dashboard reader sees fully updated, consistent dictionaries.


# initialize variables for data storage using @st.cache_resource to persist data structures across Streamlit reruns, this allows the MQTT background thread to update the same objects that the UI reads

@st.cache_resource
def get_data_storage():
    """Create persistent data storage that survives Streamlit reruns"""
    return {
        'facility_data': {},
        'market_data': {},
        'latest_timestamp': datetime(2025, 10, 6, 0, 0, 0, tzinfo=UTC),
        'data_lock': Lock()
    }

# get the persistent data storage
data_storage = get_data_storage()
facility_data = data_storage['facility_data']
market_data = data_storage['market_data']
latest_timestamp = data_storage['latest_timestamp']
data_lock = data_storage['data_lock']


# To efficiently manage data stored in memory, we will use a trimming function to drop any records older than 24hr before the latest received record - which is the time window that we want to do some reporting on later.


def trim_records(records, window_hours=24):

    if not records:
        return records  # do nothing if there is no record for that unit in memory

    latest_ts = records[-1]["ts"]
    # determine the cutoff point
    cutoff = latest_ts - timedelta(hours=window_hours)

    # keep only records newer than cutoff
    records[:] = [r for r in records if r["ts"] >= cutoff]


# define the function to parse facility messages and update the facility_data dict safely with lock
def handle_facility(topic, payload):
    """processing facility message"""

    global facility_data, latest_timestamp, data_lock, data_storage

    # extract indetifiers from each message
    # split the topic line into the facility key + unit key

    f_identifiers = topic.split("/")
    facility = f_identifiers[1]
    unit = f_identifiers[2]

    with data_lock:

        # check if a nested facility dictionary already exists for a facility key, if not create one
        if facility not in facility_data:
            facility_data[facility] = {
                "metadata": {
                    "name": payload.get("name"),
                    "state": payload.get("state"),
                    "lat": payload.get("lat"),
                    "lng": payload.get("lng"),
                    "network_id": payload.get("network_id")
                },
                "units": {}
            }

        # obtain the current unit list of the facility, create a nested dictionary with a fueltech_id (to later be used as filter) and a list of records
        units = facility_data[facility]["units"]
        if unit not in units:
            units[unit] = {
                "fueltech_id": payload.get("fueltech_id"),
                "records": []
            }

        # because the messages arrive in sorted chronological order, we can use simple list structure to store data at unit level for quick O(1) update
        # append the newest unit-level power and emission data to the list
        units[unit]["records"].append({
            "ts": datetime.fromisoformat(payload.get("ts").replace("Z", "+00:00")),
            "power": payload.get("power", 0),
            "emissions": payload.get("emissions", 0),
        })

        # call the trimming function to remove records older than 24hrs for that unit
        trim_records(units[unit]["records"])

        # update latest timestamp for Last updated field
        ts = datetime.fromisoformat(payload.get("ts").replace("Z", "+00:00"))
        if ts:
            if latest_timestamp is None or ts > latest_timestamp:
                # Update both the reference and the storage dict
                data_storage['latest_timestamp'] = ts
                # Note: latest_timestamp is a reference, but datetime is immutable
                # So we need to update the storage dict, and the reference will be updated on next rerun
                # For immediate access, we update the local reference too
                latest_timestamp = ts

        print(f"DEBUG: Facility '{facility}' now has {len(facility_data[facility]['units'])} units.")
        if unit in facility_data[facility]['units']:
            print(f"DEBUG: Unit '{unit}' in facility '{facility}' has {len(facility_data[facility]['units'][unit]['records'])} records.")
        print(f"DEBUG: Latest timestamp (after facility update): {latest_timestamp}")


# define the function to parse market messages and update the market_data dict safely with lock
def handle_market(topic, payload):
    """processing market message"""

    global market_data, data_lock

    # even though we only retrieved 1 region (NEM), this ensures efficient handling of any extension
    region = topic.split("/")[1]

    with data_lock:

        # similarly, market_data is implemented as a list of dictionaries for quick update - as the messages are already sorted and we don't have any filter applied at lower level
        # check if a nested region list already exists, if not create one
        if region not in market_data:
            market_data[region] = []

        # append the newest state-level power and emission data to the list
        market_data[region].append({
            "ts": datetime.fromisoformat(payload.get("ts").replace("Z", "+00:00")),
            "state": payload.get("network_region"),
            "demand": payload.get("demand", 0),
            "price": payload.get("price", 0),
        })

        # call the trimming function to remove records older than 24hrs for that region
        trim_records(market_data[region])

        print(f"DEBUG: Market now has {len(market_data[region])} records.")


# After subscribing, everytime the broker HiveMQ broker pushes data updates to subsribers and messages pertaining to `facility` (including all child levels) and `market` topics arrive, `on_message` extracts the topic name and payload (the message body) then passes them to the right handler.


def on_message(client, userdata, msg):
    """called when new message received"""
    print(f"DEBUG: Received MQTT message on topic: {msg.topic}")
    topic = msg.topic
    
    ## validate and parse JSON payload
    try:
        payload_str = msg.payload.decode()
        if not payload_str or not payload_str.strip():
            print(f"WARNING: Empty payload for topic {topic}, skipping")
            return
        payload = json.loads(payload_str)
    except json.JSONDecodeError as e:
        print(f"WARNING: Invalid JSON payload for topic {topic}: {e}, skipping")
        return
    except Exception as e:
        print(f"WARNING: Error decoding payload for topic {topic}: {e}, skipping")
        return

    ## validate topic format before processing: facility topics should be facility/{facility}/{unit}, market topics should be: market/{region} 
    topic_parts = topic.split("/")
    
    # process message
    if topic.startswith("facility/"):
        # only process if topic has exactly 3 segments 
        if len(topic_parts) == 3:
            handle_facility(topic, payload)
        else:
            print(f"WARNING: Unexpected facility topic format '{topic}' (expected facility/{{facility}}/{{unit}}), skipping")
    elif topic.startswith("market/"):
        # only process if topic has exactly 2 segments 
        if len(topic_parts) == 2:
            handle_market(topic, payload)
        else:
            print(f"WARNING: Unexpected market topic format '{topic}' (expected market/{{region}}), skipping")
    else:
        print(f"WARNING: Unknown topic prefix '{topic}', skipping")


# using cache decorator from Streamlit to avoid running multiple MQTT client initialization

@st.cache_resource
def get_mqtt_client():
    """initialize MQTT client"""

    # creates a new MQTT client object using the specified CLIENT_ID
    client = mqtt.Client(client_id=CLIENT_ID)
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(BROKER, PORT, 60)
        client.loop_start()
        return client
    except Exception as e:
        st.error(f"MQTT connection failed: {e}")
        return None

# call the function and store the client in a variable
mqtt_client = get_mqtt_client()


# # Helper functions

# Helper function `get_facility_snapshot()` to aggregate instantaneous data at facility-level for map visualization is defined below.
# The `power` and `emissions` metrics at facility level are sensitive to the fuel filter because one facility may have multiple fuel technologies. Hence, we account for that by adding a skipping logic if the unit's fuel

def get_facility_snapshot(fuel_filter=None, state_filter=None, name_filter=None):
    """
    Aggregate facility-level latest data for map display.
    - Sums all units of a facility **at the latest timestamp**.
    - Filters can be multi-selection sets or None.
    Args:
        fuel_filter: set of fueltech_ids to include (None = all)
        state_filter: set of state strings to include (None = all)
        name_filter: set of substrings for facility names (None = all)
    Returns:
        List of dicts: facility snapshot with aggregated power/emissions and metadata.
    """

    global facility_data, data_lock

    facility_snapshot = []

    with data_lock:
        for facility, data in facility_data.items():

            # extract facility-level metadata
            meta = data["metadata"]

            # skip facilities that don't match state or name filter
            if state_filter and meta.get("state") not in state_filter:
                continue
            if name_filter and all(nf.lower() not in meta.get("name", "").lower() for nf in name_filter):
                continue

            units = data["units"]

            # determine latest timestamp across all units of this facility
            latest_ts = None
            for unit_info in units.values():
                if unit_info["records"]:
                    unit_latest_ts = unit_info["records"][-1]["ts"]
                    if (latest_ts is None) or (unit_latest_ts > latest_ts):
                        latest_ts = unit_latest_ts

            if latest_ts is None:
                continue  # skip facility with no records

            # initialize the aggregate variables
            total_power = 0
            total_emissions = 0
            fuel_types = set()
            unit_count = 0

            for unit, info in units.items():

                # obtain the fuel type of the unit, if the fuel type is not in fuel filter, don't update the aggregate vars
                fuel = info.get("fueltech_id")
                if fuel_filter and fuel not in fuel_filter:
                    continue

                # obtain all the records for this unit
                records = info["records"]
                if not records:
                    continue

                # get the latest record for this unit and add to the aggregate variables
                latest_record = records[-1]
                if latest_record["ts"] != latest_ts:
                    continue

                total_power += latest_record.get("power", 0)
                total_emissions += latest_record.get("emissions", 0)
                fuel_types.add(fuel)
                unit_count += 1

            if unit_count > 0:
                facility_snapshot.append({
                    "facility": facility,
                    "name": meta.get("name"),
                    "state": meta.get("state"),
                    "lat": meta.get("lat"),
                    "lng": meta.get("lng"),
                    "fuel_types": list(fuel_types),
                    "units": unit_count,
                    "power": total_power,
                    "emissions": total_emissions,
                    "last_ts": latest_ts
                })

    return facility_snapshot


# Similarly, we need a way to update the historical power and emission charts at facility level - respecting the filters.

def get_historical_facility_data(fuel_filter=None, state_filter=None, name_filter=None, window_hours=24):
    """
    Prepare historical facility-level data for last 24h charts.
    - Aggregates all units per facility per timestamp.
    - Filters are multi-selection sets or None.
    Args:
        fuel_filter: set of fueltech_ids to include (None = all)
        state_filter: set of states (None = all)
        name_filter: set of substrings for facility names (None = all)
        window_hours: number of past hours to include
    Returns:
        Dict[ts] -> Dict[facility] -> aggregated values
    """

    global facility_data, data_lock, data_storage

    historical_snapshot = {}

    with data_lock:
        # read latest_timestamp directly from storage to get current value
        cutoff_time = data_storage['latest_timestamp'] - timedelta(hours=window_hours)

        for facility, data in facility_data.items():
            meta = data["metadata"]

            # check filters
            if state_filter and meta.get("state") not in state_filter:
                continue
            if name_filter and all(nf.lower() not in meta.get("name", "").lower() for nf in name_filter):
                continue

            # collect all records per unit within the time window
            ts_to_aggregate = {}
            for unit, info in data["units"].items():
                fuel = info.get("fueltech_id")
                if fuel_filter and fuel not in fuel_filter:
                    continue
                for record in info["records"]:
                    ts = record["ts"]
                    if ts < cutoff_time:
                        continue

                    if ts not in ts_to_aggregate:
                        ts_to_aggregate[ts] = {"power": 0.0, "emissions": 0.0, "fuel_types": set(), "units": 0}

                    ts_to_aggregate[ts]["power"] += float(record.get("power", 0) or 0)
                    ts_to_aggregate[ts]["emissions"] += float(record.get("emissions", 0) or 0)
                    ts_to_aggregate[ts]["fuel_types"].add(fuel)
                    ts_to_aggregate[ts]["units"] += 1

            # Store aggregated results per facility per timestamp
            for ts, agg in ts_to_aggregate.items():
                historical_snapshot.setdefault(ts, {})[facility] = {
                    "power": agg["power"],
                    "emissions": agg["emissions"],
                    "fuel_types": list(agg["fuel_types"]),
                    "units": agg["units"],
                    "name": meta.get("name"),
                    "state": meta.get("state"),
                    "lat": meta.get("lat"),
                    "lng": meta.get("lng"),
                }
    return historical_snapshot


# And a function to calculate the latest market data too.
def get_market_snapshot(state_filter=None):
    """
    Aggregate market data for left panel cards.
    - Returns latest value + trend for price and demand.
    - state_filter: set of states to include (None = all)
    - Aggregation logic: 
        - When NO filter (None) or ALL states selected: price = average, demand = sum
        - When single state: use values directly
        - When multiple states: price = average, demand = sum
    Returns:
        Dict with structure:
        {
            "price": {"latest": float, "trend": [(ts, value), ...]},
            "demand": {"latest": float, "trend": [(ts, value), ...]}
        }
    """

    global market_data, data_lock, data_storage

    price_trend = []
    demand_trend = []

    with data_lock:
        # Read latest_timestamp directly from storage to get current value
        cutoff_time = data_storage['latest_timestamp'] - timedelta(hours=24)

        # Collect all records within last 24h
        for region, records in market_data.items():
            for record in records:
                ts = record["ts"]
                if ts < cutoff_time:
                    continue
                if state_filter and record.get("state") not in state_filter:
                    continue

                price_trend.append((ts, float(record.get("price", 0) or 0)))
                demand_trend.append((ts, float(record.get("demand", 0) or 0)))

    # sort by timestamp
    price_trend.sort(key=lambda x: x[0])
    demand_trend.sort(key=lambda x: x[0])
    
    # check if we need aggregation (no filter means all states, same as multiple states)
    needs_aggregation = (state_filter is None) or (len(state_filter) > 1)
    
    if needs_aggregation and price_trend:
        # for trend, compute per timestamp (average for price, sum for demand)
        price_dict, demand_dict = {}, {}
        for ts, price in price_trend:
            price_dict.setdefault(ts, []).append(price)
        for ts, demand in demand_trend:
            demand_dict.setdefault(ts, []).append(demand)

        price_trend = [(ts, sum(vals)/len(vals)) for ts, vals in sorted(price_dict.items())]
        demand_trend = [(ts, sum(vals)) for ts, vals in sorted(demand_dict.items())]

    # compute latest value
    latest_price = price_trend[-1][1] if price_trend else 0
    latest_demand = demand_trend[-1][1] if demand_trend else 0

    return {
        "price": {"latest": latest_price, "trend": price_trend},
        "demand": {"latest": latest_demand, "trend": demand_trend},
    }



# # Dashboard building


# Refresh references from data_storage to get latest values (datetime is immutable, so reference needs refresh)

latest_timestamp = data_storage['latest_timestamp']

print(f"DEBUG: Global facility data keys: {list(facility_data.keys())}")
print(f"DEBUG: Global market data regions: {list(market_data.keys())}")
print(f"DEBUG: Current latest_timestamp for UI: {latest_timestamp}")

# HEADER AND FILTERS

st.title("NEM Electricity Network Power Generation and Emission")
col1, col2 = st.columns([2, 1])
with col1:
    st.markdown("**Measuring interval:** 5 minutes")
with col2:
    latest_ts = latest_timestamp or "Waiting for data..."
    st.markdown(f"**Last retrieved data:** {latest_ts}")


# FILTERS

filter_col1, filter_col2, filter_col3 = st.columns(3)

with filter_col1:
    name_filter = st.multiselect(
        "Station name",
        options=['Melbourne A1', 'Melbourne A2', 'Wambo', 'Adelaide Desalination',
                'Hallett', 'Aldoga', 'Angaston', 'Ararat', 'Avonlie', 'Braemar 2',
                'Ballarat', 'Bango', 'Bannerton', 'Barcaldine', 'Barker Inlet',
                'Barron Gorge', 'Bastyan', 'Bayswater', 'Bouldercombe', 'Beryl',
                'Broken Hill Battery', 'Bald Hills', 'Biala', 'The Bluff', 'Blyth',
                'Bungala One', 'Bungala Two', 'Boco Rock', 'Bodangora', 'Bolivar',
                'Bomen', 'Braemar', 'Brendale', 'Broken Hill', 'Berrybank',
                'Bulgana Green Power Hub', 'Butlers Gorge', 'Broadwater',
                'Callide C', 'Callide B', 'Canunda', 'Capital', 'Cathedral Rocks',
                'Christies Beach', 'Cethana', 'Challicum Hills', 'Chinchilla',
                'Childers', 'Cherry Tree', 'Clare', 'Clements Gap', 'Clermont',
                'Clarke Creek', 'Cluny', 'Cohuna', 'Coleambally', 'Colongra',
                'Columboola', 'Collector', 'Condong', 'Coopers Gap', 'Condamine A',
                'Crookwell 2', 'Crookwell 3', 'Crowlands', 'Crudine Ridge',
                'Corowa', 'Collinsville', 'Cattle Hill', 'Culcairn',
                'Cullerin Range', 'Dalrymple North', 'Darlington Point',
                'Dartmouth', 'Daydream', 'Darling Downs', 'Bairnsdale',
                'Devils Gate', 'Diapur 2', 'Dry Creek', 'Dulacca', 'Dundonnell',
                'Edenvale', 'Eildon', 'Elaine', 'Emerald', 'Eraring', 'Finley',
                'Fisher', 'Flyers Creek', 'Gangarri', 'Gannawarra', 'German Creek',
                'Girgarre', 'Glenrowan', 'Glenrowan West', 'Gunnedah', 'Goonumbla',
                'Gordon', 'Golden Plains East', 'Granville Harbour', 'Greenbank',
                'Griffith', 'Gladstone', 'Goyder South', 'Gullen Range', 'Gunning',
                'Hallett 1', 'Hallett 2', 'Hamilton', 'Haughton', 'Hayman',
                'Hazelwood BESS', 'Hawkesdale', 'Hunter Economic Zone',
                'Hillston Sun', 'Hornsdale', 'Hornsdale 2', 'Hornsdale 3',
                'Hornsdale Power Reserve', 'Hughenden', 'Hume', 'Happy Valley',
                'John Butters', 'Jeeralang B', 'Jemalong', 'Junee', 'Kaban',
                'Kareeya', 'Karadoc', 'Kennedy Energy Park', 'Kerang', 'Koorangie',
                'Kiamal', 'Kiata', 'Kingaroy', 'Kogan Creek', 'Kidston',
                'Ladbroke Grove', 'Lake Bonney', 'Lemonthyme / Wilmot',
                'Lincoln Gap', 'Lilyvale', 'Limondale', 'Limondale 2',
                'Catagunya / Liapootah / Wayatinah', 'Lake Bonney 3',
                'Lake Bonney 2', 'Loy Yang A', 'Loy Yang B', 'Longreach',
                'Latrobe Valley', 'Macarthur', 'Mackintosh', 'Mannum 2', 'Mannum',
                'Manildra', 'Mannum Pumping Station', 'Maryrorough',
                'Moranbah North', 'Murray Bridge-Onkaparinga', 'MacIntyre',
                'Bogong / Mackay', 'Meadowbank', 'Metz N', 'Mt Emerald',
                'Middlemount', 'Millmerran', 'Mintaro', 'Mugga Lane',
                'Mortons Lane', 'Mokoan', 'Molong', 'Moorabool', 'Moree',
                'Mortlake', 'Moura', 'Mt Piper', 'Mortlake South', 'Mt Gellibrand',
                'Mt Mercer', 'Mt Millar', 'Munna Creek', 'Murray', 'Musselroe',
                'Murra Warra', 'Morganwhyalla Pipeline', 'North Brown Hill',
                'Nevertire', 'New England', 'Newport', 'Pelican Point', 'Numurkah',
                'Nyngan', 'Oakey', 'Oakey Solar Farm', 'Oakey 2', 'Oaklands Hill',
                'Osborne', 'Paloona', 'Port Augusta Energy Park', 'Phillip Island',
                'Pioneer Sugar Mill', 'Portland', 'Parkes', 'Poatina',
                'Queanbeyan', 'Quarantine', 'Rangebank', 'Reece', 'Repulse',
                'Riverina 1', 'Riverina 2', 'Rowallan', 'Royalla',
                'Rocky Point Cogen', 'Ross River', 'Rubicon', 'Rugby Run',
                'Ryan Corner', 'Rye Park', 'Salt Creek', 'Sapphire', 'Sebastopol',
                'Shoalhaven Starches', 'Shoalhaven', 'Smithfield', 'Sun Metals',
                'Snapper Point', 'Snowtown North', 'Snowtown South', 'Snowtown',
                'Tumut', 'Guthega', 'Blowering', 'Snuggery', 'Susan River',
                'Stanwell', 'Starfish Hill', 'Stockyard Hill', 'Stubbo',
                'Silverton', 'Sunraysia', 'Suntop', 'Swanbank E', 'Tallawarra B',
                'Taralga', 'Tarong', 'Tarong North', 'Tarraleah', 'Tailem Bend 2',
                'Tailem Bend', 'Templers', 'Torrens Island', 'Trevallyn',
                'Tribute', 'Tully Sugar Mill', 'Tumut 3', 'Tungatinah',
                'Ulinda Park', 'Uranquinty', 'Victorian Big Battery',
                'Vales Point B', 'Valley Power Peaking', 'Wagga North',
                'Wallgrove', 'Wandoan South', 'Wandoan', 'Warwick', 'Waterloo',
                'Waubra', 'Western Downs', 'Western Downs 2', 'Wellington',
                'Wellington North', 'Wemen', 'Willogoleche', 'Whitsunday',
                'Winton', 'Wivenhoe', 'West Kiewa', 'Walla Walla', 'Wangaratta',
                'Wollar', 'Woodlawn', 'Woolooga', 'Woolnorth', 'Wattle Point',
                'White Rock Solar Farm', 'White Rock Wind Farm', 'West Wyalong',
                'Waratah Super Battery', 'Wunghnu', 'Wyalong', 'Townsville',
                'Yallourn W', 'Yambuk', 'Yarranlea', 'Yarwun', 'Yatpool', 'Yendon',
                'Yaloak South'],  
        key="name_filter"
    )

with filter_col2:
    fuel_filter = st.multiselect(
        "Fuel",
        options=['battery', 'battery_charging', 'battery_discharging', 'wind',
                'solar_utility', 'gas_ocgt', 'distillate', 'gas_recip', 'hydro',
                'coal_black', 'bioenergy_biomass', 'gas_ccgt', 'gas_wcmg',
                'coal_brown', 'gas_steam', 'pumps'],
        key="fuel_filter"
    )

with filter_col3:
    state_filter = st.multiselect(
        "State",
        options=['VIC', 'QLD', 'SA', 'NSW', 'TAS'],
        key="state_filter"
    )

# Convert filters to sets (or None)
fuel_set = set(fuel_filter) if fuel_filter else None
state_set = set(state_filter) if state_filter else None
name_set = set(name_filter) if name_filter else None


# MARKET DATA SIDEBAR

with st.sidebar:
    st.header("✦ Market Data & Statistics")

    # Market data cards
    market_snapshot = get_market_snapshot(state_set)
    print(f"DEBUG: Market snapshot data: {market_snapshot}")  

    price_data = market_snapshot["price"]
    demand_data = market_snapshot["demand"]

    # Price card
    price_col1, price_col2 = st.columns(2)
    with price_col1:
        # Format timestamp for display
        if price_data["trend"]:
            latest_ts = price_data["trend"][-1][0]
            if isinstance(latest_ts, datetime):
                time_str = latest_ts.strftime("%H:%M:%S")
            else:
                time_str = str(latest_ts)[-8:] if len(str(latest_ts)) >= 8 else "N/A"
        else:
            time_str = "N/A"
        
        # Format price with thousand separators
        price_formatted = f"{price_data['latest']:,.2f}"
        
        # Create chart first (behind card)
        if price_data["trend"]:
            df_price = pd.DataFrame(
                price_data["trend"], columns=["ts", "price"])
            fig_price = px.line(df_price, x="ts", y="price")
            fig_price.update_traces(
                line_color="rgba(124, 58, 237, 1)",  # Visible but faint purple line
                line_width=2
            )
            fig_price.update_layout(
                height=120,  # Smaller to avoid text area
                margin=dict(l=0, r=0, t=80, b=0),
                showlegend=False,
                xaxis_visible=False,
                yaxis_visible=False,
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                uirevision='price_stable',
                transition_duration=300,
            )
            # Render chart first (behind)
            st.plotly_chart(fig_price, use_container_width=True, config={'displayModeBar': False})
        
        # Card container overlaid on top
        st.markdown(f"""
        <div class="market-card-with-chart" style="position: relative; margin-top: -120px; height: 170px; display: flex; flex-direction: column; justify-content: center; z-index: 2;">
            <div class="market-card-content">
                <h4 style="text-align: center; margin-bottom: 0px;">Market Price ($/MWh)</h4>
                <p style="text-align: center; opacity: 0.6; font-size: 12px; margin: 0px 0;">Updated at: {time_str}</p>
                <h2 style="text-align: center; color: #7c3aed; font-size: 2.5em; margin: 10px 0;">${price_formatted}</h2>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with price_col2:
        # Format timestamp for display
        if demand_data["trend"]:
            latest_ts = demand_data["trend"][-1][0]
            if isinstance(latest_ts, datetime):
                time_str = latest_ts.strftime("%H:%M:%S")
            else:
                time_str = str(latest_ts)[-8:] if len(str(latest_ts)) >= 8 else "N/A"
        else:
            time_str = "N/A"
        
        # Format demand with thousand separators
        demand_formatted = f"{demand_data['latest']:,.0f}"
        
        # Create chart first (behind card)
        if demand_data["trend"]:
            df_demand = pd.DataFrame(
                demand_data["trend"], columns=["ts", "demand"])
            fig_demand = px.line(df_demand, x="ts", y="demand")
            fig_demand.update_traces(
                line_color="rgba(30, 58, 138, 1)",  # Visible but faint blue line
                line_width=2
            )
            fig_demand.update_layout(
                height=120,  # Smaller to avoid text area
                margin=dict(l=0, r=0, t=80, b=0),
                showlegend=False,
                xaxis_visible=False,
                yaxis_visible=False,
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                uirevision='demand_stable',
                transition_duration=300
            )
            # Render chart first (behind)
            st.plotly_chart(fig_demand, use_container_width=True, config={'displayModeBar': False})
        
        # Card container overlaid on top
        st.markdown(f"""
        <div class="market-card-with-chart" style="position: relative; margin-top: -120px; height: 170px; display: flex; flex-direction: column; justify-content: center; z-index: 2;">
            <div class="market-card-content">
                <h4 style="text-align: center; margin-bottom: 0px;">Market Demand (MW)</h4>
                <p style="text-align: center; opacity: 0.6; font-size: 12px; margin: 0px 0;">Updated at: {time_str}</p>
                <h2 style="text-align: center; color: #1e3a8a; font-size: 2.5em; margin: 10px 0;">{demand_formatted}</h2>
            </div>
        </div>
        """, unsafe_allow_html=True)

    st.divider()

    # Production breakdown
    st.header("✦ Last 24hr Production")
    # Radio button with label on same line
    prod_col1, prod_col2 = st.columns([1, 4])
    with prod_col1:
        st.markdown('<p class="toggle-label">Group by:</p>', unsafe_allow_html=True)
    with prod_col2:
        prod_mode = st.radio(
            "Production by", ["Fuel Type", "Facility"], horizontal=True, key="prod_mode", label_visibility="collapsed")

    # Get historical data
    hist_data = get_historical_facility_data(fuel_set, state_set, name_set)

    if hist_data:
        # Aggregate for donut
        if prod_mode == "Fuel Type":
            fuel_totals = {}
            for ts_data in hist_data.values():
                for fac_data in ts_data.values():
                    for fuel in fac_data["fuel_types"]:
                        fuel_totals[fuel] = fuel_totals.get(
                            fuel, 0) + fac_data["power"]

            # Sort by value descending and calculate percentages
            sorted_items = sorted(fuel_totals.items(), key=lambda x: -x[1])
            total = sum(power for _, power in sorted_items) if sorted_items else 1
            
            df_prod = pd.DataFrame([
                {
                    "label": fuel, 
                    "value": power,
                    "percent": (power / total * 100) if total > 0 else 0
                }
                for fuel, power in sorted_items
            ])
        else:
            fac_totals = {}
            for ts_data in hist_data.values():
                for fac_id, fac_data in ts_data.items():
                    name = fac_data["name"] or fac_id
                    fac_totals[name] = fac_totals.get(
                        name, 0) + fac_data["power"]

            # Sort by value descending and calculate percentages
            sorted_items = sorted(fac_totals.items(), key=lambda x: -x[1])[:10]
            total = sum(power for _, power in sorted_items) if sorted_items else 1
            
            df_prod = pd.DataFrame([
                {
                    "label": name, 
                    "value": power,
                    "percent": (power / total * 100) if total > 0 else 0
                }
                for name, power in sorted_items
            ])

        if not df_prod.empty:
            # Create custom text template: show only if >= 2%, hide others completely
            text_template = []
            textposition_list = []
            for percent in df_prod['percent']:
                if percent >= 2.0:
                    text_template.append(f'{percent:.0f}%')
                    textposition_list.append('outside')
                else:
                    text_template.append('')
                    textposition_list.append('none')  # Hide completely to avoid arrows
            
            fig_prod = px.pie(df_prod, values="value", names="label", hole=0.4)
            # Customize text labels: show only if >= 2%, sorted from 0 o'clock clockwise
            fig_prod.update_traces(
                text=text_template,
                textposition=textposition_list,
                # Sort by value descending (already sorted in DataFrame)
                sort=False,
                direction='clockwise',
                rotation=0  # Start from 0 o'clock (top)
            )
            fig_prod.update_layout(
                height=300,
                uirevision='prod_stable',
                transition_duration=300
            )
            st.plotly_chart(fig_prod, use_container_width=True)
    else:
        st.info("No production data available")

    st.divider()

    # Emissions breakdown
    st.header("✦ Last 24hr Emissions")
    # Radio button with label on same line
    emis_col1, emis_col2 = st.columns([1, 4])
    with emis_col1:
        st.markdown('<p class="toggle-label">Group by:</p>', unsafe_allow_html=True)
    with emis_col2:
        emis_mode = st.radio(
            "Emissions by", ["Fuel Type", "Facility"], horizontal=True, key="emis_mode", label_visibility="collapsed")

    if hist_data:
        if emis_mode == "Fuel Type":
            fuel_totals = {}
            for ts_data in hist_data.values():
                for fac_data in ts_data.values():
                    for fuel in fac_data["fuel_types"]:
                        fuel_totals[fuel] = fuel_totals.get(
                            fuel, 0) + fac_data["emissions"]

            # Sort by value descending and calculate percentages
            sorted_items = sorted(fuel_totals.items(), key=lambda x: -x[1])
            total = sum(emissions for _, emissions in sorted_items) if sorted_items else 1
            
            df_emis = pd.DataFrame([
                {
                    "label": fuel, 
                    "value": emissions,
                    "percent": (emissions / total * 100) if total > 0 else 0
                }
                for fuel, emissions in sorted_items
            ])
        else:
            fac_totals = {}
            for ts_data in hist_data.values():
                for fac_id, fac_data in ts_data.items():
                    name = fac_data["name"] or fac_id
                    fac_totals[name] = fac_totals.get(
                        name, 0) + fac_data["emissions"]

            # Sort by value descending and calculate percentages
            sorted_items = sorted(fac_totals.items(), key=lambda x: -x[1])[:10]
            total = sum(emissions for _, emissions in sorted_items) if sorted_items else 1
            
            df_emis = pd.DataFrame([
                {
                    "label": name, 
                    "value": emissions,
                    "percent": (emissions / total * 100) if total > 0 else 0
                }
                for name, emissions in sorted_items
            ])

        if not df_emis.empty:
            # Create custom text template: show only if >= 2%, hide others completely
            text_template = []
            textposition_list = []
            for percent in df_emis['percent']:
                if percent >= 2.0:
                    text_template.append(f'{percent:.0f}%')
                    textposition_list.append('outside')
                else:
                    text_template.append('')
                    textposition_list.append('none')  # Hide completely to avoid arrows
            
            fig_emis = px.pie(df_emis, values="value", names="label", hole=0.4)
            # Customize text labels: show only if >= 2%, sorted from 0 o'clock clockwise
            fig_emis.update_traces(
                text=text_template,
                textposition=textposition_list,
                # Sort by value descending (already sorted in DataFrame)
                sort=False,
                direction='clockwise',
                rotation=0  # Start from 0 o'clock (top)
            )
            fig_emis.update_layout(
                height=300,
                uirevision='emis_stable',
                transition_duration=300
            )
            st.plotly_chart(fig_emis, use_container_width=True)
    else:
        st.info("No emissions data available")



# MAIN AREA - MAP

# Metric toggle
metric_col1, metric_col2 = st.columns([3, 1])
with metric_col2:
    # Radio button with label on same line
    display_col1, display_col2 = st.columns([1, 2])
    with display_col1:
        st.markdown('<p class="toggle-label">Display:</p>', unsafe_allow_html=True)
    with display_col2:
        display_metric = st.radio(
            "Metric:", ["Power", "Emissions"], horizontal=True, key="display_metric", label_visibility="collapsed")

# Get facility snapshot
facilities = get_facility_snapshot(fuel_set, state_set, name_set)
print(f"DEBUG: Facilities snapshot data count: {len(facilities)}")

if not facilities:
    st.warning("No facilities match your filters. Waiting for data...")
else:
    df_map = pd.DataFrame(facilities)
    
    # Filter out facilities with invalid coordinates
    df_map = df_map[(df_map["lat"].notna()) & (df_map["lng"].notna())]
    # Remove 0,0 coordinates (invalid)
    df_map = df_map[~((df_map["lat"] == 0) & (df_map["lng"] == 0))]
    
    if df_map.empty:
        st.warning("No facilities with valid coordinates found. Waiting for data...")
    else:
        # Create map
        fig_map = go.Figure()
        
        # Collect all values for color scale
        all_values = df_map["power"].tolist() if display_metric == "Power" else df_map["emissions"].tolist()
        if all_values:
            min_val = min(all_values)
            max_val = max(all_values)
        else:
            min_val, max_val = 0, 1

        # Prepare data for a single trace (more efficient)
        lats = []
        lons = []
        values = []
        texts = []
        hover_texts = []
        
        for idx, row in df_map.iterrows():
            # Skip if coordinates are invalid
            if pd.isna(row["lat"]) or pd.isna(row["lng"]):
                continue
                
            value = row["power"] if display_metric == "Power" else row["emissions"]
            lats.append(row["lat"])
            lons.append(row["lng"])
            values.append(value)
            texts.append(f"{row['name']}<br>{value:.0f} {'MW' if display_metric == 'Power' else 't CO2'}")
            hover_texts.append(
                f"<b>{row['name']}</b><br>" +
                f"Units: {row['units']}<br>" +
                f"Fuel: {', '.join(row['fuel_types'])}<br>" +
                f"Power: {row['power']:.1f} MW<br>" +
                f"Emissions: {row['emissions']:.1f} t CO2<br>" +
                f"Updated: {row['last_ts']}"
            )
        
        # Only add trace if we have valid data

        if lats:  
            # Add single trace with all markers
            fig_map.add_trace(go.Scattermapbox(
                lat=lats,
                lon=lons,
                mode="markers",
                marker=dict(
                    size=15,
                    color=values,
                    colorscale="Viridis" if display_metric == "Power" else "Reds",
                    cmin=min_val,
                    cmax=max_val,
                    showscale=True,
                    colorbar=dict(
                        title=display_metric,
                        len=0.5,
                        y=0.5
                    ),
                    opacity=0.8
                ),
                hovertemplate="%{customdata}<extra></extra>",
                customdata=hover_texts,
                name="Facilities"
            ))
            
            # Add background boxes for text labels (light grey markers with text on top)
            label_lats = [lat + 0.3 for lat in lats]  # Slight offset above markers
            fig_map.add_trace(go.Scattermapbox(
                lat=label_lats,
                lon=lons,
                mode="markers+text",
                marker=dict(
                    size=80,  
                    color="rgba(209, 220, 220, 0.8)", 
                    symbol="rectangle",
                    opacity=0.85
                ),
                text=texts,
                textfont=dict(size=10, color="black"),
                textposition="middle center",
                hoverinfo="skip",
                showlegend=False
            ))

        # Initialize map view state in session_state if not exists (only on first load)
        
        if 'map_initialized' not in st.session_state:
            # Set initial view and center point that show all of Australia
            st.session_state.map_center = dict(lat=-27.0, lon=133.0)  
            st.session_state.map_zoom = 3.2 
            st.session_state.map_bearing = 0
            st.session_state.map_pitch = 0
            st.session_state.map_initialized = True

        # Build mapbox config
        mapbox_config = dict(
            style="open-street-map",
            center=st.session_state.map_center,
            zoom=st.session_state.map_zoom,
            bearing=st.session_state.map_bearing,
            pitch=st.session_state.map_pitch
        )

        fig_map.update_layout(
            mapbox=mapbox_config,
            margin=dict(l=0, r=0, t=0, b=0),
            height=700,
            showlegend=False,
            # uirevision preserves user interactions (zoom/pan/rotate)
            uirevision='map_stable',  # prevents view reset
            transition_duration=300   # smooth 300ms transitions
        )
        
        # Render map - uirevision will preserve user interactions
        st.plotly_chart(fig_map, use_container_width=True, key="main_map")

        st.caption(f"Showing {len(df_map)} facilities")


