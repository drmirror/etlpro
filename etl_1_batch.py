#!/usr/bin/python

import MySQLdb
from pymongo import MongoClient

client = MongoClient("localhost:27017")
db = client.etlpro
orders = db.orders

cnx = MySQLdb.connect(user='root', passwd='hello', db='etlpro')
#cnx.time_zone = 'UTC'
cursor = cnx.cursor()

item_cnx = MySQLdb.connect(user='root', passwd='hello', db='etlpro')
#item_cnx.time_zone = 'UTC'
item_cursor = item_cnx.cursor()

tracking_cnx = MySQLdb.connect(user='root', passwd='hello', db='etlpro')
#tracking_cnx.time_zone = 'UTC'
tracking_cursor = tracking_cnx.cursor()

order_query_stmt = ("select id as order_id, first_name, last_name, shipping_address "
                    "from orders")

bulk = orders.initialize_ordered_bulk_op()
bulk_count = 0

cursor.execute(order_query_stmt)

for (order_id, first_name, last_name, shipping_address) in cursor:
    item_list = []
    tracking_list = []
    doc = { "order_id" : order_id,
            "first_name" : first_name,
            "last_name" : last_name,
            "shipping_address" : shipping_address,
            "items" : item_list,
            "tracking" : tracking_list }
    item_cursor.execute("""
        select id as item_id, order_id, qty, description, price 
        from items where order_id = %s
    """ % (order_id))
    for (item_id, order_id, qty, description, price) in item_cursor:
        item_list.append( { "item_id" : item_id,
                            "qty" : qty,
                            "description" : description,
                            "price" : price } )
    tracking_cursor.execute("""
        select order_id, status, timestamp 
        from tracking where order_id = %s
    """ % (order_id))
    for (order_id, status, time_stamp) in tracking_cursor:
        tracking_list.append( { "status" : status,
                                "timestamp" : time_stamp } )

    if bulk_count > 0 and bulk_count % 1000 == 0: 
        bulk.execute()
        bulk = orders.initialize_ordered_bulk_op()
        bulk_count = 0

    bulk.insert(doc)
    bulk_count += 1

bulk.execute()
cursor.close()
item_cursor.close()
tracking_cursor.close()
cnx.close()

client.close()
