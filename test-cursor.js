var lsql = require("./index.js");
//var models = require("./Models/linkModel.js");
var async = require("async");
var util = require("util");

var extendedM = new lsql.Model("Extended", {
	_id:{type:lsql.Types.Number, autoIncrement:true, primaryKey:true},
	foo:lsql.Types.String,
	bar:lsql.Types.Number
});
var anotherM = new lsql.Model("Another", {
    _id:{type:lsql.Types.Number, autoIncrement:true, primaryKey:true},
    rand:lsql.Types.String
});
var basicM = new lsql.Model("Basic", {
	_id:{type:lsql.Types.Number, autoIncrement:true, primaryKey:true},
	extended:lsql.hasOne(extendedM)
});

//var m = new lsql.Model("testing", {"id":lsql.Types.String, "encounters":lsql.hasMany(new lsql.Model("testing2"))});
//var m = models.Links;
var fieldId = 0;
lsql.connectToDB("./testing.sqlite", function(err) {
	async.series([
		function(cb) {
			extendedM.create(function(err) {
				basicM.create(cb);
			})
		},
		function(cb) {
			extendedM.clear(function(err) {
				basicM.clear(cb);
			})
		},
		function(cb) {
			var entry = basicM.new();
			entry.extended.foo = "testing";
			entry.extended.bar = 5;
			entry.save(function(err, id){
                fieldId = id;
                console.log("Done: " + util.inspect(arguments));
				cb();
			});
		},
		function(cb) {
			basicM.find({_id:fieldId}).one(function(entry) {
				console.log("Basic query: " + util.inspect(entry));
				cb();
			});
		},
        function(cb) {
            basicM.find({"extended.bar":5}).one(function(entry) {
                console.log("Extended sub query: " + util.inspect(entry));
                cb();
            });
        }
	]);
	/*
	async.series([
		function(cb) {
			console.log("Creating all models");
			m.create(function(err) {
				models.Encounters.create(function(err) {
					models.Embed.create(function (err) {
						console.dir(err);
						cb();
					});
				});
			});
		},
		function(cb) {
			console.log("Clearing all models");
			m.clear(function(err) {
				models.Encounters.clear(function(err) {
					models.Embed.clear(cb);
				});
			});
		},
		function(cb) {
			var entry = m.new();
			entry.link = "http://www.sparecog.com";
			entry.save(function() {
				cb();
			});
		},
		function(cb) {
			m.count(function(err, count) {
				console.log("Count is " + count);
				cb();
			});
		},
		function(cb) {
			m.find({link:"http://www.sparecog.com"}).one(function(row) {
				row.update({text:"This is a test ~!@#$%^&*()_+-=[]\{}|;':\",./<>?∑´®†¥˙∆˜∫√ƒ©†¥¨"});
				row.save(cb);
			});
		},
		function(cb) {
			var embed = models.Embed.new();
			embed.version = "1.0";
			embed.type = "html";
			embed.save(function (err, id) {
				m.find({link:"http://www.sparecog.com"}).one(function(row) {
					row.embedID = id;
					row.save(cb);
				});
			});
		}
	]);
	*/
	/*
	m.find({id:"a1b2c3"}).sort({"id":1}).limit(10).each(function(row) {
		console.log("Row id: " + row.id);
		var newEncounter = row.encounters.new();
		newEncounter.insert(function(err) {
			console.log(err);
		});
		row.encounters.find().sort({"at":-1}).limit(1).one(function(err, item) {	
			console.log("encounters item is " + item);
		});
	}, function() {
		if (err) {
			console.log("Error on find: " + err);
			return;
		}
		console.log("All rows done");
		/*
		var entry = m.new()
		entry.id = "testa";
		entry.insert(function(err) {
			if (err) console.log(err.message);
			console.log("Done with insert");
		});
	});
	*/
});

