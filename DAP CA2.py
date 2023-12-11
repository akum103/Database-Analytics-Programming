#!/usr/bin/env python
# coding: utf-8

# In[38]:


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


# In[53]:


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


# In[50]:


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


# In[66]:


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


# In[ ]:




