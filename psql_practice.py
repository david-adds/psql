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
import pandas as pd


# %%
# pd.read_sql_table('film', f'postgresql://{"postgres"}:{""}@{"localhost"}:{"5432"}/{"dvdrental"}?')  

# %% [markdown]
# ____

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
# ______

# %% [markdown]
# __Situation 01:__ (SELECT FROM)
#
# __We want to send out a promotional email to our existing customers.__

# %%
SELECT_CUSTOMER_DATA="SELECT first_name, last_name, email FROM customer;"
query = retrieveData(SELECT_CUSTOMER_DATA)
query

# %% [markdown]
# __Situation 02:__ (SELECT DISTINCT)
#  
# __We want to know the types of MPAA movie available ratings (e.g. PG, PG-13, R, etc...) in the database.__

# %%
AVAILABLE_RATINGS="SELECT DISTINCT(rating) FROM film;"
query = retrieveData(AVAILABLE_RATINGS)
query

# %% [markdown]
# __Situation 03:__ (SELECT WHERE)
#
# A customer forgot their wallet at our store. We need to track down their email to inform them.\
# __What is the email for the customer with the name Nancy Thomas?__

# %%
CUSTOMER_EMAIL = '''
SELECT email FROM customer
WHERE first_name = 'Nancy' 
AND last_name = 'Thomas'
'''
query = retrieveData(CUSTOMER_EMAIL)
query

# %% [markdown]
# __Situation 4:__ (SELECT WHERE)
#  
# A customer wants to know what the movie "Outlaw Hank" is about.\
# __Could you give them the discription for the movie "Outlaw Hanky"?__

# %%
MOVIE_DESCRIPTION = '''
                        SELECT description FROM film
                        WHERE title = 'Outlaw Hanky'
                    '''

# %%
query = retrieveData(MOVIE_DESCRIPTION)
with pd.option_context('display.max_colwidth', None):
  display(query)

# %% [markdown]
# __Situation 5:__ (SELECT WHERE)
#
# A customer is late on their movie return, and we've mailed them a letter to their address at __259 Ipoh Drive__.\
# We should also call them on the phone to let them know.\
# __Can you get the phone number for the customers who lives at _259 Ipoh Drive_?__
#
#

# %%
COSTUMER_PHONE_NUMBER ='''
SELECT phone FROM address
WHERE address = '259 Ipoh Drive';
'''
query = retrieveData(COSTUMER_PHONE_NUMBER)
query

# %% [markdown]
# __Situation 6:__ (ORDER BY)
#
# We want to reward our first 10 paying customers.\
# __What are the customer ids of the first 10 customers who created a payment?__

# %%
COSTUMER_TOP10 ='''
SELECT customer_id, amount FROM payment
ORDER BY payment_date ASC
LIMIT 10;
'''
query = retrieveData(COSTUMER_TOP10)
query

# %% [markdown]
# __Situation 7:__ (ORDER BY)
#
# A customer wants to quickly reant a video to watch over their short lunch break.\
# __What are the titles of the first 5 shortest (in length of runtime) movies?__

# %%
SHORTEST_5_MOVIES ='''
SELECT title, length FROM film
ORDER BY length ASC
LIMIT 5;
'''
query = retrieveData(SHORTEST_5_MOVIES)
query

# %% [markdown]
# __if this custumer can watch any movie that is 50 minutes or less in run time, now many options does she or he have?__

# %%
SHORTEST_5_MOVIES ='''
SELECT COUNT(*) FROM film
WHERE length <=50
'''
query = retrieveData(SHORTEST_5_MOVIES)
query

# %% [markdown]
# ### General Challenge 1:  (COUNT / COUNT DISTINCT)

# %% [markdown]
# __How many payment transactions were greater than $5.00__

# %%
PAYMENT_GREATER_THAN_5USD ='''
SELECT count(*) FROM payment
WHERE amount > 5.00;
'''
query = retrieveData(PAYMENT_GREATER_THAN_5USD)
query

# %% [markdown]
# __How many unique disctricts are our customers from?__

# %%
CUSTOM_DIF_DISTRICTS ='''
SELECT COUNT(DISTINCT district) FROM address;
'''
query = retrieveData(CUSTOM_DIF_DISTRICTS)
query

# %% [markdown]
# __Retrive the list of names for those distinct districts.__

# %%
CUSTOM_DIF_DISTRICTS ='''
SELECT DISTINCT (district) FROM address;
'''
query = retrieveData(CUSTOM_DIF_DISTRICTS)
query

# %% [markdown]
# __How many films have a rating of R and a replacement cost between $5 and $15?__

# %%
CUSTOM_DIF_DISTRICTS ='''
SELECT COUNT(*) FROM film
WHERE rating IN ('R')
AND replacement_cost BETWEEN 5 and 15;
'''
query = retrieveData(CUSTOM_DIF_DISTRICTS)
query

# %% [markdown]
# ### Challenge 2: (AGG FUNCTIONS / GROUP BY) 
#

# %% [markdown]
# We have two staff members, with Staff IDs 1 and 2.\
# We want to give a bonus to the staff member that handled the most payments (Most in terms of number of payments processed, not total dollar amount).\
# __How many payments did each staff member handle and who gets the bonus?__

# %%
NUM_PROCESSED_PAYMENTS ='''
SELECT staff_id, COUNT(amount) AS qty_payments_processed 
FROM payment
GROUP BY staff_id;
'''
query = retrieveData(NUM_PROCESSED_PAYMENTS)
query

# %% [markdown]
# Corporate HQ is conducting a study on the relationship between replacement cost and the movie MPAA rating (e.g. G, PG, R, etc..).\
# __What is the average replacement cost per MPAA rating?__

# %%
AVG_REPLACEMENT_COST ='''
SELECT rating, ROUND(AVG(replacement_cost),3)
AS avg_replacement_cost 
FROM film
GROUP BY rating
ORDER BY avg_replacement_cost
'''
query = retrieveData(AVG_REPLACEMENT_COST)
query

# %% [markdown]
# We are running a promotion to reward our top 5 customers with cupons.\
# __What are the customers ids of the top 5 customers by total spend?__

# %%
TOP5_CUSTOMERS ='''
SELECT customer_id, SUM(amount) as total_spent
FROM payment
GROUP BY customer_id
ORDER BY SUM(amount) DESC
LIMIT 5;
'''
query = retrieveData(TOP5_CUSTOMERS)
query

# %% [markdown]
# ### Challenge 3: (HAVING) 
#

# %% [markdown]
# We are launching a platinum service for our most loyal customers.\
# We will assign platinum status to customers that have had 40 or more transaction payments.\
# __What customer_ids are eligible for platinum status?__

# %%
PLATINUM_STATUS='''
SELECT customer_id, COUNT(*) AS num_of_transactions
FROM payment
GROUP BY customer_id
HAVING COUNT(*) >= 40
'''
query = retrieveData(PLATINUM_STATUS)
query

# %% [markdown]
# __What are the customer ids of customers who have spent more than $100 in payment transactions with our staff_id member 2?__

# %%
SPENT_100_N_ABOVE='''
SELECT customer_id, SUM(amount) AS total_spent
FROM payment
WHERE staff_id IN (2)
GROUP BY customer_id
HAVING SUM(amount) > 100
'''
query = retrieveData(SPENT_100_N_ABOVE)
query

# %% [markdown]
# ### ASSESSMENT TEST 1

# %% [markdown]
# __1. Return the customer IDs of customers who have spent at least $110 with the staff member who has an ID of 2.__

# %%
SPENT_110_N_ABOVE='''
SELECT customer_id, SUM(amount) as total_spent
FROM payment
WHERE staff_id IN (2)
GROUP BY customer_id
HAVING SUM(amount) >=110
'''
query = retrieveData(SPENT_110_N_ABOVE)
query

# %% [markdown]
# __2. How many films begin with the letter J?__

# %%
SPENT_100_N_ABOVE='''
SELECT COUNT(*) FROM film
WHERE title like 'J%'
'''
#query = retrieveData(SPENT_100_N_ABOVE)
#query

# %% [markdown]
# __3. What customer has the highest customer ID number whose name starts with an 'E' and has an address ID lower than 500?__
#
# The answer is Eddie Tomlin

# %%
SPENT_110_N_ABOVE='''
SELECT first_name, last_name, MAX(customer_id)
FROM customer
WHERE first_name like 'E%'
AND address_id < 500
GROUP BY first_name, last_name
ORDER BY MAX(customer_id) DESC
LIMIT 1;
'''
# query = retrieveData(SPENT_110_N_ABOVE)
# query

# %% [markdown]
# ### JOIN Challenge Tasks

# %% [markdown]
# __INNER JOIN__
#
# California sales tax have changed and we need to alert our customers to this through email.\
# __What are the emails of the customers who live in California?__

# %%
CALIFORNIAN_CUSTUMERS='''
SELECT 
address.address_id,
customer.email,
district
FROM address
INNER JOIN customer
ON address.address_id = customer.address_id
WHERE address.district IN ('California')
'''
query = retrieveData(CALIFORNIAN_CUSTUMERS)
query

# %% [markdown]
# __FULL OUTER JOIN__
#
# A customer walks in and is a huge fan of the actor 'Nick Wahlberg' and wants to know which movies he is in.\
# **Get a list of all the movies 'Nick Wahlberg' has been in.**

# %%
ACTOR_IN_FILMS='''
SELECT film.title, UPPER(first_name) ||' '|| UPPER(last_name) AS actor_full_name --CONCATENATE 2 columns
FROM film_actor
INNER JOIN actor
ON film_actor.actor_id = actor.actor_id
INNER JOIN film
ON film.film_id = film_actor.film_id
WHERE first_name = 'Nick' and last_name = 'Wahlberg'
ORDER BY film.title
'''
query = retrieveData(ACTOR_IN_FILMS)
query

# %% [markdown]
# ### Timestamps and Extract - Challenge Tasks

# %% [markdown]
# __During which months did payments occur?__\
# Format your answer to return back full month name.

# %%
MONTH_PAYMENT='''
SELECT DISTINCT(TO_CHAR(payment_date, 'MONTH')) AS month_payment
FROM payment
'''
query = retrieveData(MONTH_PAYMENT)
query

# %% [markdown]
# __How many payments occurred on a Monday?__

# %%
MONDAYS_PAYMENT='''
SELECT COUNT(*) AS num_monday_payments
FROM payment
WHERE EXTRACT(DOW FROM payment_date) = 1 
'''
query = retrieveData(MONDAYS_PAYMENT)
query

# %%
