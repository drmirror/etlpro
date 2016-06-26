#!/usr/bin/python

import MySQLdb
from pymongo import MongoClient
from collections import defaultdict

client = MongoClient("localhost:27017")
db = client.etlpro
orders = db.orders

cnx = MySQLdb.connect(user='root', passwd='hello', db='etlpro')
#cnx.time_zone = 'UTC'
cursor = cnx.cursor()

items = defaultdict(list)
cursor.execute("""
        select id as item_id, order_id, qty, description, price  from items
    """)
prevlen = 0
for (item_id, order_id, qty, description, price) in cursor:
    items[order_id].append({ "item_id" : item_id,
                             "qty" : qty,
                             "description" : description,
                             "price" : price })
    if len(items) % 10000 == 0:
        if prevlen != len(items):
            print len(items)
            prevlen = len(items)

print "items loaded"

tracking = defaultdict(list)
cursor.execute("""
        select order_id, status, timestamp from tracking
    """ )
prevlen = 0
for (order_id, status, time_stamp) in cursor:
    tracking[order_id].append({ "status" : status,
                                "timestamp" : time_stamp })
    if len(tracking) % 10000 == 0:
        if prevlen != len(tracking):
            print len(tracking)
            prevlen = len(tracking)


print "tracking loaded"


cursor.execute("""
  select id as order_id, first_name, last_name, shipping_address from orders
""")

bulk = orders.initialize_ordered_bulk_op()
bulk_count = 0

for (order_id, first_name, last_name, shipping_address) in cursor:
    doc = { "order_id" : order_id,
            "first_name" : first_name,
            "last_name" : last_name,
            "shipping_address" : shipping_address,
            "items" : items[order_id],
            "tracking" : tracking[order_id] }
    if bulk_count > 0 and bulk_count % 1000 == 0: 
        bulk.execute()
        bulk = orders.initialize_ordered_bulk_op()
        bulk_count = 0

    bulk.insert(doc)
    bulk_count += 1

bulk.execute()
cursor.close()
cnx.close()

client.close()
