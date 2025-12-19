# OpenGrid-Analytics
OpenGrid Analytics Platform is a real-time electricity market data pipeline built on the OpenElectricity API. It ingests, processes, and streams energy price and demand data using Python, enabling scalable analysis of Australia’s National Electricity Market (NEM).

# Key Features
- Real-time and historical electricity price and demand ingestion
- REST API integration with OpenElectricity
- Data cleaning and transformation using Python
- MQTT-based streaming for real-time data distribution
- Modular and extensible pipeline design

<img width="1846" height="913" alt="image" src="https://github.com/user-attachments/assets/01a155cd-ab69-420e-941d-2f2954305091" />

# Prerequisites

Before running the dashboard, ensure you have the following installed:
Python 3.9+
pip
An active OpenElectricity API key

# Running the Dashboard

Step 1: Execute the facility and the market file fully, you can see that the payloads will start publishing

Step 2: Run the dashboard
If processed or streaming data is already available:
streamlit run dashboard.py

You should be directed to an external browser, where you can see the dashboards and metrics updated in real time
