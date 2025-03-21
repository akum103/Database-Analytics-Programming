Electricity, Gas, and Water Usage Analysis in Ireland

📌 Overview

This project provides a comprehensive analysis of electricity, gas, and water usage in residential and commercial areas in Ireland. It integrates MongoDB and PostgreSQL for data storage, Python for ETL and visualization, and Dash for interactive dashboards.

📂 Datasets

The project uses multiple datasets covering energy and water consumption from 2016-2022, including:

Commercial Electricity: Consumption trends across Ireland, the Euro Area, and EU27.

Commercial Gas: Gas usage by commercial entities.

Household Electricity & Gas: Trends in residential energy usage.

Water Consumption: Per-county water usage trends.

Wastewater Treatment Systems: Domestic wastewater treatment registrations.

Metered Consumption Data (JSON): Electricity & gas consumption at a granular level.

🏗️ Tech Stack

Databases: MongoDB (for raw data storage), PostgreSQL (for structured data)

Programming Language: Python

Libraries: Pandas, PyMongo, SQLAlchemy, Dash, Plotly, Seaborn

Visualization: Dash (for interactive analysis), Plotly (for data visualization)

🚀 Features & Workflow

1️⃣ Data Extraction & Storage

Uploads JSON data to MongoDB using pymongo.

Cleans and transforms the data (renaming columns, removing missing values, standardizing field names).

Stores structured data in PostgreSQL using SQLAlchemy.

2️⃣ Data Processing & Transformation

Renames columns for better readability (e.g., C03815V04565 → county_code).

Removes unnecessary fields (e.g., STATISTIC, TLIST(A1)).

Filters out invalid data (e.g., missing county codes 9999).

Standardizes values (10 Residential → Residential).

3️⃣ Data Analysis & Visualization

Electricity & Gas Trends (line graphs showing price trends across Ireland, Euro Area, EU27)

County-wise energy usage (bar charts showing electricity consumption by year)

Top 10 counties by electricity price (interactive Dash dropdown selection)

Time-series analysis for water usage trends.

4️⃣ Web Dashboard (Dash)

Dropdown-based county selection for electricity usage visualization.

Top 10 counties per year visualization.

Dynamic bar charts for electricity & gas price trends.

🛠️ Installation & Setup

1️⃣ Clone the Repository

git clone https://github.com/akum103/energy-analysis-ireland.git
cd energy-analysis-ireland

2️⃣ Install Dependencies

pip install pymongo pandas sqlalchemy dash plotly psycopg2

3️⃣ Database Setup

MongoDB Configuration

Ensure MongoDB is running.

Update mongodb_uri in the script.

Run DAP_CA2.py to upload JSON data.

PostgreSQL Configuration

Create a database in PostgreSQL (electricity_gas_database).

Update database credentials in DAP_CA2.py.

Run data upload script to populate tables.

4️⃣ Run the Dashboard

python app.py

Access the dashboard at http://127.0.0.1:8050/.

📊 Results

Key Findings

Electricity Trends: Significant increase in commercial electricity usage after 2019.

Gas Trends: Ireland’s residential sector saw a surge in gas consumption in 2022.

Water Consumption: Dublin 17 had the highest water consumption between 2016-2022.

Wastewater Registrations: Increased adoption of domestic wastewater treatment.

Sample Visualization

fig = px.line(df_price_list_elec, x='date', y=['ireland', 'euro_area', 'eu_27'],
              title='Electric Connection Trends', facet_row="user_type")
fig.show()

🎯 Future Improvements

Deploy the dashboard using Flask for live tracking.

Expand dataset coverage to include real-time API energy data.

Enhance forecasting with ML-based prediction models.

💡 Author: akum103🎯 GitHub Repo: energy-analysis-ireland
