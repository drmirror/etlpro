#!/usr/bin/python

import mysql.connector
from pymongo import MongoClient

client = MongoClient("localhost:27017")
db = client.etlpro
coll = db.orders

insert_order_stmt = ( "insert into orders (id, first_name, last_name, shipping_address) "
                      "values (%s, %s, %s, %s)" )
insert_item_stmt = ( "insert into items (id, order_id, qty, description, price) "
                     "values (%s, %s, %s, %s, %s)" )
insert_tracking_stmt = ( "insert into tracking (order_id, status, timestamp) "
                         "values (%s, %s, %s)" )

cnx = mysql.connector.connect(user='root', password='hello', database='etlout')
# cnx.time_zone = 'UTC'
cursor = cnx.cursor()

for order in coll.find({}):

    insert_order_data = (order["order_id"], order["first_name"], order["last_name"], order["shipping_address"])
    cursor.execute(insert_order_stmt, insert_order_data)

    for item in order["items"]:
        insert_item_data = (item["item_id"], order["order_id"], item["qty"], item["description"], item["price"])
        cursor.execute(insert_item_stmt, insert_item_data)

    for tracking in order["tracking"]:
        insert_tracking_data = (order["order_id"], tracking["status"], tracking["timestamp"])
        cursor.execute(insert_tracking_stmt, insert_tracking_data)

    cnx.commit()

