#!/usr/bin/python

import MySQLdb
from pymongo import MongoClient

client = MongoClient("localhost:27017")
db = client.etlpro
orders = db.orders

cnx = MySQLdb.connect(user='root', passwd='hello', db='etlpro')
#cnx.time_zone = 'UTC'
cursor = cnx.cursor()

order_query_stmt = ("select id as order_id, first_name, last_name, shipping_address "
                    "from orders")

cursor.execute(order_query_stmt)

for (order_id, first_name, last_name, shipping_address) in cursor:
    doc = { "order_id" : order_id,
            "first_name" : first_name,
            "last_name" : last_name,
            "shipping_address" : shipping_address,
            "items" : [],
            "tracking" : [] }
    orders.insert_one(doc)


cursor.execute("""
        select id as item_id, order_id, qty, description, price  from items
    """)
for (item_id, order_id, qty, description, price) in cursor:
    orders.update_one({"order_id" : order_id},
                      {"$push" : { "items" : { "item_id" : item_id,
                                               "qty" : qty,
                                               "description" : description,
                                               "price" : price }}})


cursor.execute("""
        select order_id, status, timestamp from tracking
    """ )
for (order_id, status, time_stamp) in cursor:
    orders.update_one({"order_id" : order_id},
                      {"$push" : { "tracking" : { "status" : status,
                                                  "timestamp" : time_stamp }}})


cursor.close()
cnx.close()

client.close()
