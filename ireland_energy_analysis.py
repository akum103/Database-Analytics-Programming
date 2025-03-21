#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# UPLOADING JSON TO MONGODB

import json
import pymongo

json_file_path = r"C:\Users\ankit\OneDrive\Desktop\wORK fiLES\NCI Files\Semester I\DAP\elec.json"

from pymongo import MongoClient 

mongodb_uri = "mongodb+srv://akum103:Sushma09(@dap.c7r880a.mongodb.net/dap"
database_name = "electricity_gas_usage"
collection_name = "electricity"

client = MongoClient(mongodb_uri)
db = client[database_name]
collection = db[collection_name]

try:
    with open(json_file_path, 'r') as json_file:
        json_data = json.load(json_file)

    # Insert data into MongoDB collection
    collection.insert_many(json_data)
    print("Data inserted successfully")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the MongoDB connection
    client.close()


# In[ ]:


#DATA TRANSFORMATION IN MONGODB

from pymongo import MongoClient

try:
    # Connect to MongoDB
    client = MongoClient(mongodb_uri)
    db = client[database_name]
    collection = db[collection_name]

    # Define the mapping of old column names to new column names
    column_change = {
        'Statistic Label': 'type_of_connection',
        'C03815V04565': 'county_code',
        'Counties & Dublin Postal Districts': 'county'
    }

    # Iterate over the mapping and update the column names
    for old_col, new_col in column_change.items():
        update_query = {"$rename": {old_col: new_col}}
        collection.update_many({}, update_query)

    print("Column names updated successfully.")
    
    
    delete_columns = ['TLIST(A1)', 'C03816V04566', 'STATISTIC']

    # Iterate over the documents and delete the specified columns
    for column in delete_columns:
        update_query = {"$unset": {column: 1}}
        collection.update_many({}, update_query)

    print("Columns deleted successfully.")
    
    # Define the values to be excluded
    delete_values = ['-', '9999']

    # Iterate over the values and delete rows with those values in the county_code field
    for value in delete_values:
        delete_query = {"county_code": value}
        collection.delete_many(delete_query)

    print("Rows deleted successfully.")
    
    #rename values
    sector_change_values = {
        '10 Residential': 'Residential',
        '20 Non-residential': 'Commercial'
    }

    # Iterate over the mapping and update the values in the Sector field
    for old_value, new_value in sector_change_values.items():
        update_query = {"Sector": old_value}
        update_operation = {"$set": {"Sector": new_value}}
        collection.update_many(update_query, update_operation)

    print("Values updated successfully.")
    
    documents = list(collection.find())

# Lowercase all field names in each document
    for doc in documents:
        updated_doc = {}
        for key, value in doc.items():
            new_key = key.lower()
            updated_doc[new_key] = value

    # Update the document with lowercase field names
    update_query = {"$set": updated_doc}
    collection.update_one({"_id": doc["_id"]}, update_query)
    
    print("DONE.")

except Exception as e:
    print(f"An unexpected error occurred: {e}")

finally:
    # Close the MongoDB client in the 'finally' block to ensure it's closed even if an error occurs
    if client:
        client.close()


# In[ ]:


#FETCHING  DATA FROM MONGO DB AND STORING IN DF USING PANDAS

try:
    # Connect to MongoDB
    client = MongoClient(mongodb_uri)
    db = client[database_name]
    collection = db[collection_name]

    # Fetch data from MongoDB and store it in a DataFrame
    mongo_data = list(collection.find())
    df = pd.DataFrame(mongo_data)

    print("Data fetched successfully from MongoDB.")

except Exception as e:
    print(f"An error occurred while fetching data from MongoDB: {e}")

finally:
    # Close the MongoDB connection in the 'finally' block to ensure it's closed even if an error occurs
    if client:
        client.close()
        
print(df)



# In[ ]:


# import more datasets


import pandas as pd

# IMPORT ELECTRIC PRICE FOR COMMERCIAL

commercial_elec = r'C:\Users\ankit\commercial_elec.csv'

df_commercial_elec = pd.read_csv(commercial_elec)

df_commercial_elec['Date'] = df_commercial_elec['Year'].str.extract(r'(\d{4})')

df_commercial_elec = df_commercial_elec.groupby('Date').mean(numeric_only = True).reset_index()

df_commercial_elec['Type of Connection'] = 'Electric'
df_commercial_elec['user'] = 'Commercial'

print('---ELECTRIC PRICE OF COMMERCIAL---')
print(df_commercial_elec)



# IMPORT ELECTRIC PRICE FOR HOUSE

house_elec = r'C:\Users\ankit\house_elec.csv'

df_house_elec = pd.read_csv(house_elec)

df_house_elec['Date'] = df_house_elec['Year'].str.extract(r'(\d{4})')

df_house_elec = df_house_elec.groupby('Date').mean(numeric_only = True).reset_index()

df_house_elec['Type of Connection'] = 'Electric'
df_house_elec['user'] = 'House'

print('\n\n---ELECTRIC PRICE OF HOUSE---')
print(df_house_elec)




# IMPORT GAS PRICE FOR COMMERCIAL

commercial_gas = r'C:\Users\ankit\commercial_gas.csv'

df_commercial_gas = pd.read_csv(commercial_gas)

df_commercial_gas['Date'] = df_commercial_gas['Year'].str.extract(r'(\d{4})')

df_commercial_gas = df_commercial_gas.groupby('Date').mean(numeric_only = True).reset_index()

df_commercial_gas['Type of Connection'] = 'Gas'

df_commercial_gas['user'] = 'Commercial'

print('\n\n---GAS PRICE OF COMMERCIAL---')
print(df_commercial_gas)



# IMPORT GAS PRICE FOR HOUSE

house_gas = r'C:/Users/ankit/house_gas.csv'

df_house_gas = pd.read_csv(house_gas)

df_house_gas['Date'] = df_house_gas['Year'].str.extract(r'(\d{4})')

df_house_gas = df_house_gas.groupby('Date').mean(numeric_only = True).reset_index()

df_house_gas['Type of Connection'] = 'Gas'

df_house_gas['user'] = 'House'

print('\n\n---GAS PRICE OF HOUSE---')
print(df_house_gas)


# MERGE THEM INTO 1 DATAFRAME

df_all = [df_commercial_elec,df_house_elec,df_commercial_gas,df_house_gas]
df_price_list = pd.concat(df_all,ignore_index = True)

print('\n\n---MERGING PRICE LISTS---')
print(df_price_list)


# print(df['Year'])



# In[ ]:


# Upload price list to Postgres

from sqlalchemy import create_engine
from sqlalchemy import Integer
from sqlalchemy import VARCHAR

import pandas as pd


# Replace these values with your actual database credentials
db_user = 'postgres'
db_password = 'Sushma09('
db_host = 'localhost'
db_port = '5432'
db_name = 'electricity_gas_database'

# Create a SQLAlchemy engine
engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

print('\n\n connection was extablished')
# Assuming your DataFrame is named d_price_list
# Map DataFrame columns to PostgreSQL table columns
column_mapping = {
    'Date': 'date',                 
    'Ireland': 'ireland',
    'Euro Area' : 'euro_area',
    'euro_27': 'eu_27',
    'Type of Connection': 'type_of_connection',
    'user': 'user_type'
}

# Rename DataFrame columns based on the mapping
df_price_list.rename(columns=column_mapping, inplace=True)

print('\n\n data frame was renamed')

# Insert the DataFrame into the PostgreSQL table
df_price_list.to_sql('price_list', con=engine, if_exists='append', index=False)

print('\n\n data was inserted')





# In[ ]:


# Upload DF to postgres

from sqlalchemy import create_engine
from sqlalchemy import Integer
from sqlalchemy import VARCHAR

#PostgreSQL connection parameters
db_params = {
    'user': 'postgres',
    'password': 'Sushma09(',
    'host': 'localhost',
    'port': '5432',
    'database': 'electricity_gas_database'
}

# Create a SQLAlchemy engine
engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}")

try:
    # Specify data types for each column if needed
    dtype_dict = {
        'Year': Integer,
        'Sector': VARCHAR(50),
        'UNIT' : VARCHAR(50),
        'VALUE' : Integer,
        'county' :VARCHAR(50),
        'county_code' : VARCHAR(50),
        'type_of_connection' : VARCHAR(50),
        

    }
    
    df.drop('_id', axis=1, inplace=True)
    
    # Store the DataFrame in PostgreSQL, replacing the existing data
    df.to_sql('elec', engine, if_exists='replace', index=False, dtype=dtype_dict)

    print("Data uploaded successfully to PostgreSQL.")

except Exception as e:
    print(f"An unexpected error occurred: {e}")

    
    
finally:
    # Close the database connection in the 'finally' block to ensure it's closed even if an error occurs
    engine.dispose()


# In[3]:


# call file from Postgre for viz

from sqlalchemy import create_engine
from sqlalchemy import Integer
from sqlalchemy import VARCHAR
import pandas as pd

#PostgreSQL connection parameters
db_params = {
    'user': 'postgres',
    'password': 'Sushma09(',
    'host': 'localhost',
    'port': '5432',
    'database': 'electricity_gas_database'
}

engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}")

sql_elec = "SELECT * FROM elec"

sql_price_list = "SELECT * FROM price_list"


try:
    elec_data = pd.read_sql_query(sql_elec,engine)
    price_list = pd.read_sql_query(sql_price_list,engine)
#     print(from_postgres.head())
    
    
except Exception as e:
    print(f"An unexpected error occured: {e}")
    
finally:
    engine.dispose()
    

df_elec = elec_data
df_price_list = price_list
print(df_price_list)


# In[4]:


# Create a new DF for house prices per year - output variable for this cell = merged_df_elec_house

import pandas as p

# create a new df and multiply usage with price

# df_elec_residential, df_price_list

df_price_list_eresidential = df_price_list[(df_price_list['type_of_connection'] == 'Electric')
                                           & (df_price_list['user_type'] == 'House')]

df_price_list_eresidential = df_price_list_eresidential.rename(columns ={'date':'Year'})
print (df_price_list_eresidential)

df_elec_residential = df_elec[df_elec['Sector'] == 'Residential']


merged_df_elec_house = pd.merge(df_price_list_eresidential, df_elec_residential, on = 'Year', how = 'left')
print(merged_df_elec_house)

# print(df_elec_residential)

merged_df_elec_house ['Total Electricity Price'] = merged_df_elec_house['ireland'] * 1/100 * merged_df_elec_house['VALUE'] * 1000

# merged_df_elec_house.to_csv('price.csv', index = True)
# print('File Created')

 


# In[5]:


# Create a new df for commercial prices per year output variable for this cell = merged_df_elec_commercial

import pandas as p


# df_elec_residential, df_price_list

df_price_list_commercial = df_price_list[(df_price_list['type_of_connection'] == 'Electric')
                                           & (df_price_list['user_type'] == 'Commercial')]

df_price_list_commercial = df_price_list_commercial.rename(columns ={'date':'Year'})
print (df_price_list_commercial)

df_elec_commercial = df_elec[df_elec['Sector'] == 'Commercial']

print(df_elec_commercial)

merged_df_elec_commercial = pd.merge(df_price_list_commercial, df_elec_commercial, on = 'Year', how = 'left')
print(merged_df_elec_commercial)


merged_df_elec_commercial ['Total Electricity Price'] = merged_df_elec_commercial['ireland'] * 1/100 * merged_df_elec_commercial['VALUE'] * 1000
print(merged_df_elec_commercial)
# merged_df_elec_house.to_csv('price.csv', index = True)
# print('File Created')

 


# In[6]:


# Visualization for price_list
import plotly

import plotly.graph_objects as go
import plotly.express as px
import pandas as pd


df_price_list_elec = df_price_list[df_price_list['type_of_connection'] == 'Electric']

df_price_list_gas = df_price_list[df_price_list['type_of_connection'] == 'Gas']

fig_electric = px.line(df_price_list_elec, x='date', y=['ireland', 'euro_area', 'eu_27'], title='Electric Connection Trends',
                      facet_row ="user_type", line_dash_map = {'ireland':'solid','euro_area': 'dash', 'eu_27':'dot'},
                      labels = {'value':'Price'},
                      line_shape = 'spline')

fig_electric.show()

fig_gas = px.line(df_price_list_gas, x='date', y=['ireland', 'euro_area', 'eu_27'], title='Gas Connection Trends',
                      facet_row ="user_type",
                      labels = {'value':'Price'},
                      line_shape = 'spline')

fig_gas.show()


# In[7]:


# Electricity usage by county

import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px

# Assuming df is your DataFrame with 'Year', 'VALUE', 'county', and 'Sector' columns
# ...

# Starting dash command
app = dash.Dash(__name__)

# Sort the DataFrame by 'VALUE' in descending order
# df_sorted = df.sort_values(by='VALUE', ascending=False)

df_sorted = df_elec

# Plot bar graph with my df_sorted data frame with x-axis as year and y-axis as value of electricity unit
fig = px.bar(df_sorted, x='Year', y='VALUE', color='VALUE', labels={'VALUE': 'Electricity Value'},
             title="County wise electricity usage", color_continuous_scale='blues')

# Define layout
app.layout = html.Div([
    html.H1(children='ELECTRICITY USAGE BY COUNTY', style={'textAlign': 'center', 'color': 'red'}),
    
    # New dropdown for selecting 'Residential' or 'Commercial'
    dcc.Dropdown(
        id='sector-dropdown',
        options=[{'label': sector, 'value': sector} for sector in df_elec['Sector'].unique()],
        value=df_elec['Sector'].unique()[0],  # Set default value
        multi=False
    ),
    
    dcc.Graph(figure=fig, id='electricity-bar-chart'),
    
    # Dropdown for selecting counties
    dcc.Dropdown(
        id='county-dropdown',
        options=[{'label': county, 'value': county} for county in df_elec['county'].unique()],
        value=df_elec['county'].unique()[0],  # Set default value
        multi=False
    )
])

# Define callback to update graph based on dropdown selections
@app.callback(
    Output('electricity-bar-chart', 'figure'),
    [Input('county-dropdown', 'value'),
     Input('sector-dropdown', 'value')]
)
def update_graph(selected_county, selected_sector):
    filtered_df = df_sorted[(df_sorted['county'] == selected_county) & (df_sorted['Sector'] == selected_sector)]

    # Create a new DataFrame with the sum of values for each year
    sum_df = filtered_df.groupby('Year')['VALUE'].sum().reset_index()

    updated_fig = px.bar(
        sum_df, x='Year', y='VALUE',
        labels={'VALUE': 'Electricity Value'},
        color='VALUE',  # Add color parameter to create a heatmap effect
        color_continuous_scale='blues'
    )

    # Add text annotations to each bar with the total value
    for i, row in sum_df.iterrows():
        updated_fig.add_annotation(
            x=row['Year'],
            y=row['VALUE'],
            text=str(row['VALUE']),
            showarrow=True,
            arrowhead=2,
            arrowcolor='black',
            ax=0,
            ay=-30
        )

    return updated_fig

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)


# In[12]:


# elec for Residential graph

import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import seaborn as sb

df_top_counties_elec = merged_df_elec_commercial

def top_ten_per_year(group):
    return group.nlargest(10, 'Total Electricity Price')

top_ten_df_elec = df_top_counties_elec.groupby('Year', group_keys=False).apply(top_ten_per_year)

top_ten_df_elec = top_ten_df_elec[top_ten_df_elec['Year'] != 2023]
print(top_ten_df_elec)


# Assuming your DataFrame is named top_ten_df_elec
df = top_ten_df_elec

# Initialize the Dash app
app = dash.Dash(__name__)

# Layout of the app
app.layout = html.Div([
    html.H1("Residential - Top 10 Counties for Each Year for Electricity"),

    # Dropdown for selecting the year
    dcc.Dropdown(
        id='year-dropdown',
        options=[
            {'label': str(year), 'value': year} for year in df['Year'].unique()
        ],
        value=df['Year'].max(),  # Set the initial selected year
        multi=False,
    ),

    # Bar plot
    dcc.Graph(id='bar-plot'),
])

# Callback to update the bar plot based on selected year
@app.callback(
    Output('bar-plot', 'figure'),
    [Input('year-dropdown', 'value')]
)
def update_bar_plot(selected_year):
    selected_data = df[df['Year'] == selected_year]

    fig = px.bar(selected_data, x='county', y='Total Electricity Price', title=f'Top 10 Counties in {selected_year}')
    fig.update_layout(barmode='group', xaxis_tickangle=-45)

    return fig

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)


# In[13]:


# Elec for Commercial graph


import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import seaborn as sb

df_top_counties_elec = merged_df_elec_house

def top_ten_per_year(group):
    return group.nlargest(10, 'Total Electricity Price')

top_ten_df_elec = df_top_counties_elec.groupby('Year', group_keys=False).apply(top_ten_per_year)

top_ten_df_elec = top_ten_df_elec[top_ten_df_elec['Year'] != 2023]
print(top_ten_df_elec)


# Assuming your DataFrame is named top_ten_df_elec
df = top_ten_df_elec

# Initialize the Dash app
app = dash.Dash(__name__)

# Layout of the app
app.layout = html.Div([
    html.H1("Commercial - Top 10 Counties for Each Year for Electricity"),

    # Dropdown for selecting the year
    dcc.Dropdown(
        id='year-dropdown',
        options=[
            {'label': str(year), 'value': year} for year in df['Year'].unique()
        ],
        value=df['Year'].max(),  # Set the initial selected year
        multi=False,
    ),

    # Bar plot
    dcc.Graph(id='bar-plot'),
])

# Callback to update the bar plot based on selected year
@app.callback(
    Output('bar-plot', 'figure'),
    [Input('year-dropdown', 'value')]
)
def update_bar_plot(selected_year):
    selected_data = df[df['Year'] == selected_year]

    fig = px.bar(selected_data, x='county', y='Total Electricity Price', title=f'Top 10 Counties in {selected_year}')
    fig.update_layout(barmode='group', xaxis_tickangle=-45)

    return fig

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)


# In[ ]:





# In[ ]:





# In[ ]:




