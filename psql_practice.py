# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.4
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# __Import Libraries__

# %%
from sqlalchemy import create_engine 
from pandas.io import sql


# %% [markdown]
# __Function to Connect to the PostgreSQL__
#
# This function connects PostgreSQL to python and returns the query result in a dataframe

# %%
def retrieveData(query):
    engine = create_engine(
      'postgresql://{}:{}@{}:{}/{}?'.format("postgres", #Username
                                                    "", #Password
                                           "localhost", # Host
                                                "5432", #Port
                                          "dvdrental"), #Database
        connect_args={"options": "-c statement_timeout=500000000"}
    )
    connection = engine.connect()
    result = sql.read_sql(query,connection)
    connection.close()
    return result


# %% [markdown]
# __Situation 01:__ 
#
# We want to send out a promotinal email to our existing customers.

# %%
SELECT_CUSTOMER_DATA="SELECT first_name, last_name, email FROM customer;"

# %%
query = retrieveData(SELECT_CUSTOMER_DATA)
query
