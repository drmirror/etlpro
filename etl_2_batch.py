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

bulk = orders.initialize_ordered_bulk_op()
bulk_count = 0

cursor.execute(order_query_stmt)

for (order_id, first_name, last_name, shipping_address) in cursor:
    doc = { "order_id" : order_id,
            "first_name" : first_name,
            "last_name" : last_name,
            "shipping_address" : shipping_address,
            "items" : [],
            "tracking" : [] }
    if bulk_count > 0 and bulk_count % 1000 == 0:
        bulk.execute()
        bulk = orders.initialize_ordered_bulk_op()
        bulk_count = 0

    bulk.insert(doc)
    bulk_count += 1

bulk.execute()
bulk = orders.initialize_ordered_bulk_op()
bulk_count = 0

cursor.execute("""
        select id as item_id, order_id, qty, description, price  from items
    """)
for (item_id, order_id, qty, description, price) in cursor:
    if bulk_count > 0 and bulk_count % 1000 == 0:
        bulk.execute()
        bulk = orders.initialize_ordered_bulk_op()
        bulk_count = 0
    bulk.find({"order_id" : order_id}).update(
                    {"$push" : { "items" : { "item_id" : item_id,
                                             "qty" : qty,
                                             "description" : description,
                                             "price" : price }}})
    bulk_count += 1

bulk.execute()
bulk = orders.initialize_ordered_bulk_op()
bulk_count = 0

cursor.execute("""
        select order_id, status, timestamp from tracking
    """ )
for (order_id, status, time_stamp) in cursor:
    if bulk_count > 0 and bulk_count % 1000 == 0:
        bulk.execute()
        bulk = orders.initialize_ordered_bulk_op()
        bulk_count = 0
    bulk.find({"order_id" : order_id}).update(
                    {"$push" : { "tracking" : { "status" : status,
                                                "timestamp" : time_stamp }}})
    bulk_count += 1

bulk.execute()

cursor.close()
cnx.close()

client.close()
