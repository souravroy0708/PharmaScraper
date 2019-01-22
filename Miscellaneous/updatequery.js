db.getCollection("scrapes").updateMany({ "template": { $exists: false }, "Source":/.*https:\/\/www\.pharmaciearnaudbernardlafayette\.com.*/i },{"$set": {"template": "lafayette_2"}})
