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
def retrieveData(query,dbname):
    engine = create_engine(
      'postgresql://{}:{}@{}:{}/{}?'.format("postgres", #Username
                                                    "", #Password
                                           "localhost", # Host
                                                "5432", #Port
                                          f"{dbname}"), #Database
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
query = retrieveData(SELECT_CUSTOMER_DATA,'dvdrental')
query

# %% [markdown]
# __Situation 02:__ (SELECT DISTINCT)
#  
# __We want to know the types of MPAA movie available ratings (e.g. PG, PG-13, R, etc...) in the database.__

# %%
AVAILABLE_RATINGS="SELECT DISTINCT(rating) FROM film;"
query = retrieveData(AVAILABLE_RATINGS, 'dvdrental')
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
query = retrieveData(CUSTOMER_EMAIL,'dvdrental')
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
query = retrieveData(MOVIE_DESCRIPTION,'dvdrental')
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
query = retrieveData(COSTUMER_PHONE_NUMBER,'dvdrental')
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
query = retrieveData(COSTUMER_TOP10,'dvdrental')
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
query = retrieveData(SHORTEST_5_MOVIES,'dvdrental')
query

# %% [markdown]
# __if this custumer can watch any movie that is 50 minutes or less in run time, now many options does she or he have?__

# %%
SHORTEST_5_MOVIES ='''
SELECT COUNT(*) FROM film
WHERE length <=50
'''
query = retrieveData(SHORTEST_5_MOVIES,'dvdrental')
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
query = retrieveData(PAYMENT_GREATER_THAN_5USD,'dvdrental')
query

# %% [markdown]
# __How many unique disctricts are our customers from?__

# %%
CUSTOM_DIF_DISTRICTS ='''
SELECT COUNT(DISTINCT district) FROM address;
'''
query = retrieveData(CUSTOM_DIF_DISTRICTS,'dvdrental')
query

# %% [markdown]
# __Retrive the list of names for those distinct districts.__

# %%
CUSTOM_DIF_DISTRICTS ='''
SELECT DISTINCT (district) FROM address;
'''
query = retrieveData(CUSTOM_DIF_DISTRICTS,'dvdrental')
query

# %% [markdown]
# __How many films have a rating of R and a replacement cost between $5 and $15?__

# %%
CUSTOM_DIF_DISTRICTS ='''
SELECT COUNT(*) FROM film
WHERE rating IN ('R')
AND replacement_cost BETWEEN 5 and 15;
'''
query = retrieveData(CUSTOM_DIF_DISTRICTS,'dvdrental')
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
query = retrieveData(NUM_PROCESSED_PAYMENTS,'dvdrental')
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
query = retrieveData(AVG_REPLACEMENT_COST,'dvdrental')
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
query = retrieveData(TOP5_CUSTOMERS,'dvdrental')
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
query = retrieveData(PLATINUM_STATUS,'dvdrental')
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
query = retrieveData(SPENT_100_N_ABOVE,'dvdrental')
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
query = retrieveData(SPENT_110_N_ABOVE,'dvdrental')
query

# %% [markdown]
# __2. How many films begin with the letter J?__

# %%
SPENT_100_N_ABOVE='''
SELECT COUNT(*) FROM film
WHERE title like 'J%%'
'''
query = retrieveData(SPENT_100_N_ABOVE,'dvdrental')
query

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
# query = retrieveData(SPENT_110_N_ABOVE,'dvdrental')
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
query = retrieveData(CALIFORNIAN_CUSTUMERS,'dvdrental')
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
query = retrieveData(ACTOR_IN_FILMS,'dvdrental')
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
query = retrieveData(MONTH_PAYMENT,'dvdrental')
query

# %% [markdown]
# __How many payments occurred on a Monday?__

# %%
MONDAYS_PAYMENT='''
SELECT COUNT(*) AS num_monday_payments
FROM payment
WHERE EXTRACT(DOW FROM payment_date) = 1 
'''
query = retrieveData(MONDAYS_PAYMENT,'dvdrental')
query

# %% [markdown]
# ___
# ___

# %% [markdown]
# ### DataBase Exercises (exercises.tar)

# %% [markdown]
# ***1. How can you retrieve all the information from the cd.facilities table?***
#

# %%
QUERY='''
SELECT * FROM cd.facilities; 
'''
query = retrieveData(QUERY,'exercises')
query

# %% [markdown]
# **2. You want to print out a list of all of the facilities and their cost to members.\
# How would you retrieve a list of only facility names and costs?**

# %%
QUERY='''
SELECT name,membercost FROM cd.facilities; 
'''
query = retrieveData(QUERY,'exercises')
query

# %% [markdown]
# __3. How can you produce a list of facilities that charge a fee to members?__\
# __Expected Results should have just 5 rows__
#

# %%
QUERY='''
SELECT * FROM cd.facilities
WHERE membercost != 0
'''
query = retrieveData(QUERY,'exercises')
query

# %% [markdown]
# __4. How can you produce a list of facilities that charge a fee to members, and that fee is less than 1/50th of the monthly maintenance cost?\
# Return the facid, facility name, member cost, and monthly maintenance of the facilities in question.__
# * __Result is just two rows:__
#

# %%
QUERY='''
SELECT facid,name,membercost,monthlymaintenance FROM cd.facilities
WHERE membercost < 0.02*(monthlymaintenance) AND membercost NOT IN (0)
'''
query = retrieveData(QUERY,'exercises')
query

# %% [markdown]
# __5. How can you produce a list of all facilities with the word 'Tennis' in their name?__
#

# %%
QUERY='''
SELECT * FROM cd.facilities
WHERE name LIKE '%%Tennis%%'
'''
query = retrieveData(QUERY,'exercises')
query

# %% [markdown]
# __6. How can you retrieve the details of facilities with ID 1 and 5?\
# Try to do it without using the OR operator.__

# %%
QUERY='''
SELECT * FROM cd.facilities
WHERE facid IN (1,5)
'''
query = retrieveData(QUERY,'exercises')
query

# %% [markdown]
# __7. How can you produce a list of members who joined after the start of September 2012?\
# Return the memid, surname, firstname, and joindate of the members in question.\
# Expected Result is 10 rows (not all are shown below)__
#

# %%
QUERY='''
SELECT memid, surname, firstname, joindate FROM cd.members
WHERE EXTRACT( MONTH FROM joindate) IN 
(SELECT EXTRACT( MONTH FROM joindate) AS month FROM cd.members
 WHERE EXTRACT( MONTH FROM joindate) >= 9 AND EXTRACT( YEAR FROM joindate) IN (2012))
'''
query = retrieveData(QUERY,'exercises')
query

# %% [markdown]
# __8. How can you produce an ordered list of the first 10 surnames in the members table? The list must not contain duplicates.__

# %%
QUERY='''
SELECT DISTINCT(surname) FROM cd.members
ORDER BY surname ASC
LIMIT 10
'''
query = retrieveData(QUERY,'exercises')
query

# %% [markdown]
# __9. You'd like to get the signup date of your last member. How can you retrieve this information?__

# %%
QUERY='''
SELECT joindate FROM cd.members
ORDER BY joindate DESC
LIMIT 1
'''
query = retrieveData(QUERY,'exercises')
query

# %% [markdown]
# __10. Produce a count of the number of facilities that have a cost to guests of 10 or more.__

# %%
QUERY='''
SELECT COUNT(*) FROM cd.facilities
WHERE guestcost >= 10
'''
query = retrieveData(QUERY,'exercises')
query

# %% [markdown]
# __11. Produce a list of the total number of slots booked per facility in the month of September 2012.\
# Produce an output table consisting of facility id and slots, sorted by the number of slots.__
#
#

# %%
QUERY='''
SELECT f.facid, SUM(b.slots) FROM cd.facilities AS f
INNER JOIN cd.bookings AS b
ON f.facid = b.facid
WHERE EXTRACT(MONTH FROM starttime) in (9)
GROUP BY f.facid
ORDER BY SUM(b.slots)
'''
query = retrieveData(QUERY,'exercises')
query

# %% [markdown]
# __12. Produce a list of facilities with more than 1000 slots booked.\
# Produce an output table consisting of facility id and total slots, sorted by facility id.__

# %%
QUERY='''
SELECT facid,SUM(slots) FROM cd.bookings
GROUP BY facid
HAVING SUM(slots) > 1000
ORDER BY facid
'''
query = retrieveData(QUERY,'exercises')
query

# %% [markdown]
# __13. How can you produce a list of the start times for bookings for tennis courts, for the date '2012-09-21'?\
# Return a list of start time and facility name pairings, ordered by the time.__

# %%
QUERY='''
SELECT B.starttime, F.name FROM cd.bookings AS B
LEFT JOIN cd.facilities AS F
on B.facid=F.facid
WHERE CAST(B.starttime AS DATE) = '2012-09-21' 
AND name LIKE 'Tennis Court _'
'''
query = retrieveData(QUERY,'exercises')
query

# %% [markdown]
# __14. How can you produce a list of the start times for bookings by members named 'David Farrell'?__

# %%
QUERY='''
SELECT b.starttime, (m.firstname||' '||m.surname) AS name FROM cd.bookings b
LEFT JOIN cd.members as m
ON b.memid=m.memid
WHERE m.firstname = 'David' AND m.surname = 'Farrell'
'''
query = retrieveData(QUERY,'exercises')
query

# %%
