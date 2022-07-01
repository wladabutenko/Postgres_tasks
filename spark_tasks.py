#!/usr/bin/env python
# coding: utf-8

# In[5]:


import findspark
findspark.init()


# In[87]:


import pandas as pd
import pyspark.sql.functions as ps
from pyspark.sql import SparkSession
from sqlalchemy import create_engine
from pyspark.sql.types import StructType, StructField, NumericType, IntegerType, StringType, TimestampType, FloatType


# In[7]:


appName = "task"
master = "local"


# In[8]:


spark = SparkSession.builder.master(master).appName(appName).getOrCreate()


# In[20]:


engine = create_engine('postgresql+psycopg2://postgres:secret@172.17.0.2:5432/pagila')

category = spark.createDataFrame(pd.read_sql('select * from category', engine))
film_category = spark.createDataFrame(pd.read_sql('select * from film_category', engine))
film_actor = spark.createDataFrame(pd.read_sql('select * from film_actor', engine))
actor = spark.createDataFrame(pd.read_sql('select * from actor', engine))
rental = spark.createDataFrame(pd.read_sql('select * from rental', engine))
inventory = spark.createDataFrame(pd.read_sql('select * from inventory', engine))
address = spark.createDataFrame(pd.read_sql('select * from address', engine))
payment = spark.createDataFrame(pd.read_sql('select * from payment', engine))
city = spark.createDataFrame(pd.read_sql('select * from city', engine))
customer = spark.createDataFrame(pd.read_sql('select * from customer', engine))


# In[21]:


schema = StructType([StructField("film_id",IntegerType(), True), 
                     StructField("title", StringType(), True), 
                     StructField("description", StringType(), True), 
                     StructField("release_year", IntegerType(), True), 
                     StructField("language_id", IntegerType(), True), 
                     StructField("original_language_id", IntegerType(), True), 
                     StructField("rental_duration", IntegerType(), True), 
                     StructField("rental_rate", FloatType(), True), 
                     StructField("length", IntegerType(), True), 
                     StructField("replacement_cost", FloatType(), True), 
                     StructField("rating", StringType(), True), 
                     StructField("last_update", TimestampType(), True), 
                     StructField("special_features", StringType(), True), 
                     StructField("fulltext", StringType(), True)])
film = spark.createDataFrame(pd.read_sql('select * from film', engine), schema=schema)


# In[55]:


# task 1

dt = category.join(film_category, category.category_id == film_category.category_id, "inner")
dt.groupBy("name").agg(ps.count("*").alias("film_counts")).orderBy("film_counts", ascending=False).show()


# In[62]:


# task 2

t1 = actor.join(film_actor,actor.actor_id == film_actor.actor_id, "inner") \
    .join(film, film.film_id == film_actor.film_id, "inner")
t1.groupBy("first_name", "last_name").agg(ps.count("rental_duration").alias("r_amount")).orderBy("r_amount", ascending=False).show(10)


# In[44]:


# task 3

dt3 = payment.join(rental, payment.rental_id == rental.rental_id) \
    .join(inventory, inventory.inventory_id == rental.inventory_id) \
    .join(film_category, inventory.film_id == film_category.film_id) \
    .join(category, film_category.category_id == category.category_id)
dt3.groupBy("name").agg(ps.sum("amount").alias("exspenses")).orderBy("exspenses", ascending=False).show(1)


# In[53]:


# task 4

t1 = film.select("film_id")
t2 = inventory.select("film_id")
t1.exceptAll(t2).show()


# In[ ]:


# task 5

from pyspark.sql.window import Window

dt = actor.join(film_actor, actor.actor_id == film_actor.actor_id, 'inner') \
    .join(film, film.film_id == film_actor.film_id, "inner") \
    .join(film_category, film.film_id ==film_category.film_id, "inner") \
    .join(category, category.category_id == film_category.category_id, "inner") \
    .where(category.name == "Children")

dt.groupBy("first_name","last_name").agg(ps.count("title").alias("film_counts")).orderBy("film_counts", ascending=False)

windowSpec = Window.orderBy(ps.desc("film_counts"))
dt = dt.withColumn("rank", ps.dense_rank().over(windowSpec))
dt.where(dt.rank <= 3).show()


# In[90]:


# task 6

import pyspark.sql.functions as ps

t = city.join(address, city.city_id == address.city_id, "inner") \
    .join(customer, address.address_id == customer.address_id, "inner")
t.groupBy("city").agg(ps.sum(ps.when(t.active == 1, 1).otherwise(0)).alias("active_clients"),
    ps.sum(ps.when(t.active == 0, 1).otherwise(0)).alias("not_active_clients")) \
    .orderBy("not_active_clients", ascending=False).show()


# In[128]:


# task 7

f = rental.join(inventory, rental.inventory_id == inventory.inventory_id) \
    .join(film_category, inventory.film_id == film_category.film_id) \
    .join(category, film_category.category_id == category.category_id) \
    .join(customer, customer.customer_id == rental.customer_id) \
    .join(address, customer.address_id == address.address_id) \
    .join(city, address.city_id == city.city_id) \
    .where((city.city.like('A%') | city.city.like('%-%')))

f = f.groupBy("city","name") \
    .agg(ps.sum(((ps.unix_timestamp(f.return_date) - ps.unix_timestamp(f.rental_date))/3600))
         .alias("rental_sum_hours")).orderBy("rental_sum_hours", ascending=True)
k = f.selectExpr("city as c", "rental_sum_hours as rh", "name as n")
f = f.join(k, ps.when(f.city == k.c, True) & ps.when(f.rental_sum_hours < k.rh, True), "left").where(k.rh.isNull() & f.rental_sum_hours.isNotNull())

f.select("city", "name", "rental_sum_hours").orderBy("city").show(10)


# In[118]:


city.show()


# In[ ]:





# In[ ]:




