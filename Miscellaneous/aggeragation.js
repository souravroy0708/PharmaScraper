db.getCollection("scrapes").aggregate(

	// Pipeline
	[
		// Stage 1
		{
			$group: {
			    _id: {"Template":"$template","Source":"$Source"}, count:{"$sum":1}
			}
		},

	]

	// Created with Studio 3T, the IDE for MongoDB - https://studio3t.com/

);
