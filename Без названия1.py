#!/usr/bin/env python
# coding: utf-8

# In[12]:


import findspark
findspark.init()


# In[13]:


import pandas as pd
import pyspark.sql.functions as ps
from pyspark.sql import SparkSession
from sqlalchemy import create_engine
from pyspark.sql.types import StructType, StructField, NumericType, IntegerType, StringType, TimestampType, FloatType


# In[14]:


appName = "task"
master = "local"


# In[15]:


spark = SparkSession.builder.config('spark.jars', '/home/user/Загрузки/postgresql-42.4.0.jar').master(master).appName(appName).getOrCreate()


# In[18]:


df = spark.read.format("jdbc")\
    .option("url", "jdbc:postgresql://172.17.0.2:5432/pagila") \
    .option("driver", "org.postgresql.Driver")\
    .option("user", "postgres")\
    .option("password", "secret")


# In[19]:


rental = df.option("dbtable", "rental").load()
inventory = df.option("dbtable", "inventory").load()
payment = df.option("dbtable", "payment").load()
address = df.option("dbtable", "address").load()
city = df.option("dbtable", "city").load()
customer = df.option("dbtable", "customer").load()
category = df.option("dbtable", "category").load()
film_category = df.option("dbtable", "film_category").load()
film_actor = df.option("dbtable", "film_actor").load()
actor = df.option("dbtable", "actor").load()
film = df.option("dbtable", "film").load()


# In[20]:


# task 1

dt = category.join(film_category, category.category_id == film_category.category_id, "inner")
dt.groupBy("name").agg(ps.count("*").alias("film_counts")).orderBy("film_counts", ascending=False).show()


# In[21]:


# task 2

t1 = actor.join(film_actor,actor.actor_id == film_actor.actor_id, "inner") \
    .join(film, film.film_id == film_actor.film_id, "inner")
t1.groupBy("first_name", "last_name").agg(ps.count("rental_duration").alias("r_amount")).orderBy("r_amount", ascending=False).show(10)


# In[22]:


# task 3

dt3 = payment.join(rental, payment.rental_id == rental.rental_id) \
    .join(inventory, inventory.inventory_id == rental.inventory_id) \
    .join(film_category, inventory.film_id == film_category.film_id) \
    .join(category, film_category.category_id == category.category_id)
dt3.groupBy("name").agg(ps.sum("amount").alias("exspenses")).orderBy("exspenses", ascending=False).show(1)


# In[23]:


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


# In[27]:


# task 6

import pyspark.sql.functions as ps

t = city.join(address, city.city_id == address.city_id, "inner") \
    .join(customer, address.address_id == customer.address_id, "inner")
t.groupBy("city").agg(ps.sum(ps.when(t.active == 1, 1).otherwise(0)).alias("active_clients"),
    ps.sum(ps.when(t.active == 0, 1).otherwise(0)).alias("not_active_clients")) \
    .orderBy("not_active_clients", ascending=False).show()


# In[28]:


# task 7

f = rental.join(inventory, rental.inventory_id == inventory.inventory_id) \
    .join(film_category, inventory.film_id == film_category.film_id) \
    .join(category, film_category.category_id == category.category_id) \
    .join(customer, customer.customer_id == rental.customer_id) \
    .join(address, customer.address_id == address.address_id) \
    .join(city, address.city_id == city.city_id) \
    .where((city.city.like('A%') | city.city.like('%-%')))
f = f.groupBy("city", "name") \
    .agg(ps.sum(((ps.unix_timestamp(f.return_date) - ps.unix_timestamp(f.rental_date))/3600))
         .alias("rental_sum_hours")).orderBy("rental_sum_hours", ascending=True)
k = f.selectExpr("city as c", "rental_sum_hours as rh", "name as n")
f = f.join(k, ps.when(f.city == k.c, True) & ps.when(f.rental_sum_hours < k.rh, True), "left").where(k.rh.isNull() & f.rental_sum_hours.isNotNull())

f.select("city", "name", "rental_sum_hours").orderBy("city").show()


# In[29]:


city.show()


# In[ ]:





# In[ ]:




