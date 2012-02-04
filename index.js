var util = require("util");
var sqlite = require("sqlite-fts");
var events = require("events");
var async = require("async");
var _debug = true;

//TODO:  Support for multiple sqlite DBs and using attach to queries
var primaryDB;

/// Helper to see if a variable is a callable function
var isFunction = function(func) {
	return typeof(func) == 'function' && (!Function.prototype.call || typeof(func.call) == 'function');
}

var isObject = function(obj) {
	return typeof(obj) == "object";
}

exports.connectToDB = function(path, cbDone) {
	primaryDB = new sqlite.Database();
	var self = this;
	var ev = new events.EventEmitter();
	primaryDB.open(path, function(err) {
		if (err) {
			ev.emit("error", err);
			return;
		}
		//console.log("DB is open");
		cbDone();
	});
	return ev;
}

/// Cursor for iterating results
var ModelCursor = function(model) {
	this.model = model;

	// Set our basic internals and create methods to set them
	["query", "resultFields", "sort", "limit", "offset", "joins"].forEach(function(item) {
		this["_" + item] = undefined;
		// Check this so we're not resetting it constantly
		if (!isFunction(ModelCursor.prototype[item])) {
			ModelCursor.prototype[item] = function(value) {
				this["_" + item] = value;
				return this;
			}
		}
	})
};
ModelCursor.prototype.each = function(cbEach, cbDone) {
	//console.log("In each for " + this.model.name);
	var self = this;
	// Build our select query
	var fields = (this._resultFields && this._resultFields.length > 0) ? this._resultFields.map(function(f) { return self.model.name + "." + f; }).join(",") : "*";
	// We check which our our joins to include
	this._joins = fields == "*" ? this.model.joins : this.model.joins.filter(function(join) {
        self._resultFields.forEach(function(field) {
            var ptIndex = field.indexOf(".");
            if (ptIndex > 0 && join.name == field.substr(0, ptIndex)) {
                return true;
            }
            return false;
        });
	});
	var query = {rootTable:this.model.name, format:("SELECT " + fields + " FROM " + this.model.name), bindings:[]};
	this._buildQuery(query);
	if (_debug) {
		console.log("Query: " + query.format);
		console.log("Binds: " + query.bindings.toString());
	}
	//console.dir(this);
	var ev = new events.EventEmitter();
    if (_debug) {
        var E = new Error();
        console.log("[DEBUG] Query stack:"+ E.stack);
    }
	primaryDB.query(query.format, query.bindings, function(err, row) {
		if (err) {
			ev.emit("error", err);
			return;
		}
		// All the rows done
		if (row === undefined) {
			cbDone.call(self);
			return;
		}
		cbEach.call(self, new self.model.ModelEntry(row));
	});
	return ev;
};
ModelCursor.prototype.one = function(cbOne) {
	var result;
	// TODO:  Some more error checking
	return this.each(function(item) { result = item; }, function() {cbOne(result)});
}
ModelCursor.prototype.remove = function(entry, cbDone) {
    if (entry) {
        //TODO:  impl the entry case
        // Modify the query args based on the primary key of the entry
    }
	var query = {rootTable:this.model.name, format:("DELETE FROM " + this.model.name), bindings:[]};
	this._buildQuery(query);
	return this._basicQuery(query, cbDone);
};
ModelCursor.prototype.update = function(entry, cbDone) {
	//TODO: impl
	var query = {rootTable:this.model.name, format:("UPDATE " + this.model.name), bindings:[]};
	query.format += " SET ";
	var hasFirst = false
	for (var k in entry._dirtyFields) {
		if (!entry._dirtyFields.hasOwnProperty(k)) continue;
		if (hasFirst) query.format += ","
		query.format += k + "=?";
		query.bindings.push(entry._dirtyFields[k]); 
		hasFirst = true;
	}
	if (!this._query) {
		this._query = {};
		this._query[entry.model.keys[0]] = entry.row[entry.model.keys[0]];
	}
	this._buildQuery(query);
	return this._basicQuery(query, function() {
		// Reset our dirty fields and carry on
		this._dirtyFields = {};
		cbDone();
	});
};
//--
// cbDone(error, lastInsertedId)
//--
ModelCursor.prototype.insert = function(entry, cbDone) {
	var query = {format:("INSERT INTO " + this.model.name), bindings:[]};
	var columns = [];
	var values = [];
	for (var k in entry._dirtyFields) {
		if (!entry._dirtyFields.hasOwnProperty(k)) continue;
		columns.push(k);
		values.push("?");
		query.bindings.push(entry._dirtyFields[k]); 
	}
	query.format += " (" + columns.map(function(k) { return "'" + k + "'";}).join(",") + ") VALUES (" + values.join(",") + ")"
	var ev = this._basicQuery(query, function(err) {
		// Reset the dirty fields and return the inserted id
		this._dirtyFields = {};
		primaryDB.execute("SELECT last_insert_rowid() AS last", function(err, rows) {
			if (err) {
				ev.emit("error", err);
				return;
			}
			cbDone(undefined, rows[0].last);
		});
	});
	return ev;
};
ModelCursor.prototype._basicQuery = function(query, cbDone) {
	if (_debug) {
		console.log("Query: " + query.format);
		console.log("Binds: " + query.bindings.toString());
	}
	var ev = new events.EventEmitter();
	primaryDB.execute(query.format, query.bindings, function(err, rows) {
		if (err) {
            if (_debug) console.error("[DEBUG] Error: " + err);
			ev.emit("error", err);
			return;
		}
		if (cbDone) cbDone();
	});
	return ev;
};
ModelCursor.prototype._buildQuery = function(query) {
	// TODO: Only accepts 1 joined table right now 
    if (this._joins) this._joins.forEach(function(join) { join.func.build(query, join.alias); });
    
	// The where clause
    if (this._query && Object.keys(this._query).length > 0) query.format += " WHERE ";
	var where = isFunction(this._query) ? this.query.build(query) : Ops.and(this._query).build(query);
	if (this._sort) {
		query.format += " ORDER BY ";
		var hasFirst
		for (var k in this._sort) {
			if (this._sort.hasOwnProperty(k)) {
				if (hasFirst) query.format += ","
				query.format += this.model.name + "." + k + (this._sort[k] > 1 ? " ASC" : " DESC");
			}
		}
	}
	if (this._limit) {
		query.format += " LIMIT "
		if (this._offset) query.format += this._offset.toString() + ","
		query.format += this._limit;
	}
	return query;
};

/// The model
var Model = function(name, spec) {
	this.name = name;
	this.spec = spec;
	this.keys = [];
	this.joins = [];
	var self = this;
	this.ModelEntry = function(row) {
		this.model = self;
		this.row = row;
		this._dirtyFields = {};
	}
	this.ModelEntry.prototype.update = function(fields) {
		for (var k in fields) {
			if (fields.hasOwnProperty(k) && self.spec.hasOwnProperty(k)) {
				this._dirtyFields[k] = fields[k];
			} else {
                if (_debug) console.log("[DEBUG] Unknown update field: " + k);
            }
		}
	};
	this.ModelEntry.prototype.remove = function(cbDone) {
		var cursor = new ModelCursor(self);
		cursor.remove(this, cbDone);
	}
	this.ModelEntry.prototype.insert = function(cbDone) {
		var cursor = new ModelCursor(self);
		cursor.insert(this, cbDone);
	};
	this.ModelEntry.prototype.save = function(cbDone) {
		var entry = this;
		async.forEach(self.joins, function(join, cb) {
			if (join.func.preSave) join.func.preSave(entry, entry[join.alias], cb);
		}, function(err) {
			var cursor = new ModelCursor(self);
			var func = entry.row === undefined ? "insert" : "update";
			cursor[func](entry, function(err, id) {
				async.forEach(self.joins, function(join, cb) {
					if (join.func.postSave) {
                        join.func.postSave(entry, entry[join.alias], cb);
                    } else {
                        cb();
                    }
				}, function() {
					//console.log("HERE WE ARE (" + id + ")" + util.inspect(entry))
					cbDone(err, id);
				})
			})
		});
	};
    this.ModelEntry.prototype.toJSON = function() {
        var simpleObject = {};
        var self = this;
        Object.keys(this.model.spec).forEach(function(k) {
            if (self.row.hasOwnProperty(k)) simpleObject[k] = self.row[k];
        });
        return simpleObject;
    };
	function addBasicProp(key) {
		Object.defineProperty(self.ModelEntry.prototype, key, {
			get:function() {
				if (this._dirtyFields.hasOwnProperty(key)) {
					return this._dirtyFields[key];
				} else {
					return this.row[key];
				}
			},
			set:function(value) {
				this._dirtyFields[key] = value;
			}
		});
	}
	function addCallableProp(key) {
		Object.defineProperty(self.ModelEntry.prototype, key, {
			get:function() {
				console.log("function get for " + key);
				return spec[key].type(this, self);
			}
		})
	}
	for (var k in spec) {
		if (!spec.hasOwnProperty(k)) continue;
		//console.log("Adding " + k + " to ModelEntry");
		// We call these indirectly due to the loop closure issue of javascript
		if (spec[k].hasOwnProperty("primaryKey")) {
			this.keys.push(k);
		}
		// This is getting gnarly
		if (spec[k].hasOwnProperty("join")) {
			this.joins.push({alias:k, func:spec[k].type});
		} else if (spec[k].type && isFunction(spec[k].type)) {
            addCallableProp(k);
		} else {
			addBasicProp(k);
		}
	}
	// TODO: Process the spec into this.fields
}
Model.prototype.getSpecType = function(key) {
	
};
Model.prototype.count = function(cb) {
	primaryDB.execute("SELECT COUNT(*) AS count FROM " + this.name, function(err, rows) {
		if (err || !rows || rows.length < 1) {
			cb(err || true);
		}
		cb(null, rows[0].count)
	})
};
Model.prototype.find = function(expressions) {
	var cursor = new ModelCursor(this);
	cursor.query(expressions);
	return cursor;
};
Model.prototype.update = function(entry, cbDone) {
    var cursor = new ModelCursor(this);
    cursor.update(entry, cbDone);
    return cursor;
};
Model.prototype.new = function() {
	var ret = new this.ModelEntry();
	this.joins.forEach(function(join) {
		ret[join.alias] = join.func.new();
	});
	return ret;
};
Model.prototype.remove = function(expression, cbDone) {
    var cursor = new ModelCursor(this);
    cursor.query(expression);
    cursor.remove(null, cbDone);
};
Model.prototype.clear = function(cbDone) {
    if (_debug) {
        console.log("DELETE FROM " + this.name);
    }
	primaryDB.execute("DELETE FROM " + this.name, function(err, rows) {
		cbDone(err);
	});
};
Model.prototype.create = function(cbDone) {
	var checkTableQuery = "SELECT name FROM sqlite_master WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%' UNION ALL SELECT name FROM sqlite_temp_master WHERE type IN ('table','view') ORDER BY 1";
	var self = this;
	primaryDB.execute(checkTableQuery, function(err, rows) {
		if (rows.filter(function(row) { return row.name == self.name; }).length > 0) {
			if (cbDone) cbDone();
			return;
		}
		var fields = [];
		for (var k in self.spec) {
			if (!self.spec.hasOwnProperty(k)) continue;
			//console.log("Creating key: " + k);
			var specEntry = self.spec[k];
			var field = {name:k};
			if (typeof(specEntry) == "object" && specEntry.hasOwnProperty("type")) {
				if (specEntry.hasOwnProperty("primaryKey")) field.primaryKey = specEntry.primaryKey;
				if (specEntry.hasOwnProperty("autoIncrement")) field.autoIncrement = specEntry.autoIncrement;
				specEntry = specEntry.type;
			}
			// Spec entries are either a direct function or an object with a create member, nothing else
			if (isFunction(specEntry.create)) {
				//console.log("Building the type");
				specEntry.create(field);
				fields.push(field);
			}
		}
		var sql = "CREATE TABLE " + self.name + " (";
		sql += fields.map(function(entry) { 
			return "'" + entry.name + "' " + entry.type + (entry.primaryKey ? " PRIMARY KEY" : "") + (entry.autoIncrement ? " AUTOINCREMENT" : "");
		}).join(",");
		sql += ")";
		if (_debug) process.stderr.write(sql + "\n");
		primaryDB.execute(sql, function(err, rows) {
			if (cbDone) cbDone(err);
		})
	});
};
exports.Model = Model;



/// Core operations supported by the expression builder
//--  A basic comparison between a field and a value
function ComparisonOp(operator, value) {
	this.operator = operator;
	this.expression = value;
}
ComparisonOp.prototype.build = function(query) {
	query.format += this.operator;
	if (isFunction(this.expression)) {
		this.expression.call(query);
	} else {
		query.format += this.expression;
	}
};
//-- A basic boolean operation on a series of expressions
function BooleanOp(type, expressions) {
	this.type = type;
	this.expressions = expressions;
}
BooleanOp.prototype.build = function(query) {
	var hasFirst = false;
	for (var x in this.expressions) {
		if (!this.expressions.hasOwnProperty(x)) continue;
		if (hasFirst) query.format += " " + this.type + " ";
		var expression = this.expressions[x];
		if (isFunction(expression.build)) {
			query.format += x;
			expression.build.call(this.expressions[x], query);
		} else {
            if (x.indexOf(".") < 0) x = query.rootTable + "." + x;
			query.format += x + " = ?";
			query.bindings.push(expression);
		}
		hasFirst = true;
	}
}
function InOp(values) {
    this.values = values;
}
InOp.prototype.build = function(query) {
    query.format += " IN (" + Array(this.values.length).join("?,").slice(0, -1) + ")";
    query.bindings = query.bindings.concat(this.values);
};

Ops = {
	gt:function(value) {
		return new ComparisonOp(">", value);
	},
	gte:function(value) {
		return new ComparisonOp(">=", value);
	},
	lt:function(value) {
		return new ComparisonOp("<", value);
	},
	lte:function(value) {
		return new ComparisonOp("<=", value);
	},
	ne:function(value) {
		return new ComparisonOp("!=", value);
	},
	or:function(expressions) {
		return new BooleanOp("OR", expressions);
	},
	and:function(expressions) {
		return new BooleanOp("AND", expressions);
	},
    in:function(values) {
        return new InOp(values);
    }
};
exports.Ops = Ops;

/************************************************************************************
/ Joins are so awesome...
*/
var InnerJoin = function(entry, parentModel, childModel) {
	this.entry = entry;
	this.parentModel = parentModel;
	this.childModel = childModel;

	var self = this;
	this.ProxyEntry = function(proxiedEntry) {
		this.entry = proxiedEntry;
	}
	util.inherits(this.ProxyEntry, childModel.ModelEntry);
	this.ProxyEntry.prototype.save = function(cbDone) {
		console.log("*** Save it via proxy" + util.inspect(this, true, 3));
		this.entry.save(function(err, lastInsertID){
			if (err) {
				cbDone(err);
				return;
			}
			var joinTable = self.parentModel.name + "_" + self.childModel.name;
			var query = "INSERT INTO " + joinTable + " (" + self.childModel.name + "_id," + self.parentModel.name + "_id) VALUES (?,?)";
			console.log("Query:" + query);
			console.log("Binds: " + [lastInsertID, self.entry.id]);
			primaryDB.execute(query, [lastInsertID, self.entry.id], function(error, rows){
				if (error) {
					cbDone(error);
					return;
				}
				cbDone();
			})
		})
	};
}
InnerJoin.prototype.build = function(query) {
	var joinTable = this.parentModel.name + "_" + this.childModel.name;
	var key = joinTable + "." + this.childModel.name + "_id";
	query.format += " INNER JOIN " + joinTable + " ON " + key + "=id"; 
};
InnerJoin.prototype.find = function(query) {
	var joinTable = this.parentModel.name + "_" + this.childModel.name;
	var key = joinTable + "." + this.parentModel.name + "_id";
	if (isFunction(query)) {
		var joinId = {};
		joinId[key] = this.entry.id;
		query = Ops.and([joinId].concat(query));
	} else {
		if (!query) query = {};
		query[key] = this.entry.id;
	}
	return this.childModel.find.call(this.childModel, query).joins(this)
};
InnerJoin.prototype.new = function() {
	return new this.ProxyEntry(this.childModel.new());
};
InnerJoin.prototype.create = function() {
    console.log("should create a hasMany");
};
//-- Methods to use in your model to do relationships
exports.hasMany = function(model) {
	return {
		type:function(childModel) {
			return function(entry, parentModel) {
				return new InnerJoin(entry, parentModel, childModel);
			}
		}
	};
}

exports.hasOne = function(model)  {
	if (!model || !model.spec) throw new Error("The child model is not fully defined.");
	var primaryKey = undefined;
	Object.keys(model.spec).forEach(function(key) {
		if (model.spec[key].primaryKey) {
			primaryKey = key;
		}
	});
	if (!primaryKey) throw new Error("The child model did not have a primary key.");
    var self = this;
	return {
		type:{
            build:function(query, alias) {
                query.format += " LEFT JOIN " + model.name + " AS " + alias + " ON " + query.rootTable + "._" + model.name + "_id = " + alias + "._id ";
            },
			create:function(field) {
				field.name = "_" + model.name + "_id";
				model.spec[primaryKey].type.create(field);
			},
			new:function() {
				return model.new();
			},
			preSave:function(joinCursor, parentCursor, callback) {
				parentCursor.save(function(err, id) {
					// TODO: This is a bit of a hack, it forces it into the updated fields.  Maybe consider a more complete mechanism
					joinCursor._dirtyFields["_" + model.name + "_id"] = id;
					callback();
				})
			}
		},
		join:true
	};
}

/// Core supported types
Types = {
	String:{
		create:function(field) {
			field.type = "text";
		}
	},
	Date:{
		create:function(field) {
			field.type = "integer";
		}
	},
	Number:{
		create:function(field) {
			field.type = "integer";
		}
	},
	Text:{
		create:function(field) {
			field.type = "text";
		}
	},
	PrimaryKey:function(type) {
		return {
			create:function(field) {
				type.create(field);
				field.type = field.type + " PRIMARY KEY";
			}
		};
	},
	AutoIncrement:function(type) {
		return {
			create:function(field) {
				type.create(field);
				field.type = field.type + " AUTOINCREMENT";
			}
		};
	}
};
exports.Types = Types;

