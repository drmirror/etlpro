#!/usr/bin/python

import random, string, datetime, pytz
import mysql.connector

def randomword(minlength, maxlength):
    length = random.randint(minlength, maxlength)
    return ''.join(random.choice(string.lowercase) for i in range(length))

tracking_state = [ "ORDERED", "PAID", "SHIPPED", "DELIVERED", "CANCELLED" ]

insert_order_stmt = ( "insert into orders (id, first_name, last_name, shipping_address) "
                      "values (%s, %s, %s, %s)" )
insert_item_stmt = ( "insert into items (id, order_id, qty, description, price) "
                     "values (%s, %s, %s, %s, %s)" )
insert_tracking_stmt = ( "insert into tracking (order_id, status, timestamp) "
                         "values (%s, %s, %s)" )

cnx = mysql.connector.connect(user='root', password='hello', database='etlpro')
cnx.time_zone = 'UTC'
cursor = cnx.cursor()

for order_num in range(1,1000000):
    first_name = randomword(5,15)
    last_name = randomword(10,20)
    shipping_address = randomword(10,15) + " " + randomword(10, 15) + ", " + randomword(10,15)
    insert_order_data = (order_num, first_name, last_name, shipping_address)

    cursor.execute(insert_order_stmt, insert_order_data)

    num_items = random.randint(3,20)
    for item_num in range(1,num_items):
        qty = random.randint(1,100)
        description = randomword(10, 15)
        price = random.randint(100,1000000)
        insert_item_data = (item_num, order_num, qty, description, price)
        cursor.execute(insert_item_stmt, insert_item_data)

    num_states = random.randint(2,5)
    ts = datetime.datetime (random.randint(1980,2016),
                            random.randint(1,12),
                            random.randint(1,28),
                            random.randint(0,23),
                            random.randint(0,59),
                            random.randint(0,59),
                            tzinfo=pytz.utc)
    for i in range(1,num_states):
        insert_tracking_data = (order_num, tracking_state[i-1], ts)
        cursor.execute(insert_tracking_stmt, insert_tracking_data)
        ts += datetime.timedelta(random.randint(1,7),
                                 random.randint(0,3600))

    if num_states < 5:
        insert_tracking_data = (order_num, "CANCELLED", ts)
        cursor.execute(insert_tracking_stmt, insert_tracking_data)
        
    if order_num % 1000 == 0:
        cnx.commit()
        print order_num

cnx.commit()
cursor.close()
cnx.close()


