{
    start: {
	source: {
	    uri:  "jdbc:mysql://localhost:3306/etlpro?useSSL=false",
	    user: "root",
	    password: "hello"
	},
	target: {
	    mode: "insert",
	    uri: "mongodb://localhost:27017/",
	    namespace: "etlsiphon.orders"
	},
	template: {
	    _id: "$id",
	    first_name: "$first_name",
	    last_name : "$last_name",
	    shipping_address : "$shipping_address",
	    items : [ "@itemsection" ],
	    tracking : [ "@trackingsection" ]
	},
	query:{
	    sql: 'SELECT * FROM orders'
	}
    },

    itemsection : {
	template: {
	    "id" : "$id",
	    "qty" : "$qty",
	    "description" : "$description",
	    "price" : "$price"
	},
	query : {
	    sql: "select * from items where order_id = ?"
	},
	params: [ "id" ]
    },

    trackingsection : {
	template: {
	    "status" : "$status",
	    "timestamp" : "$timestamp"
	},
	query : {
	    sql: "select * from tracking where order_id = ?"
	},
	params: [ "id" ]
    }
}
