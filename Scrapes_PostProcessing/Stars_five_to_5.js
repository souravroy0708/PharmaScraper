db.getCollection("scrapes").updateMany({"Stars": "zero"} ,{ $set: { "Stars" : 0} })
