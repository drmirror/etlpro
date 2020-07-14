db.createView("ordes_table", "orders", [
    {
        "$project" : {
            "items" : 0,
            "tracking" : 0
        }
    },
    {
        "$addFields" : {
            "id" : "$order_id"
        }
    },
    {
        "$unset" : [
            "_id",
            "order_id"
        ]
    }
])

db.createView("items_table", "orders", [
    {
        "$project" : {
            "tracking" : 0,
            "first_name" : 0,
            "last_name" : 0,
            "shipping_address" : 0
        }
    },
    {
        "$unwind" : "$items"
    },
    {
        "$project" : {
            "order_id" : "$order_id",
            "item_id" : "$items.item_id",
            "price" : "$items.price",
            "description" : "$items.description",
            "qty" : "$items.qty"
        }
    },
    {
        "$unset" : "_id"
    }
)]
db.createView("tracking_table", "orders", [
    {
        "$unset" : [
            "_id",
            "items",
            "first_name",
            "last_name",
            "shipping_address"
        ]
    },
    {
        "$unwind" : "$tracking"
    },
    {
        "$project" : {
            "order_id" : "$order_id",
            "status" : "$tracking.status",
            "timestamp" : "$tracking.timestamp"
        }
    }
])
