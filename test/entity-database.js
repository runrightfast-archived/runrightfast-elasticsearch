/**
 * Copyright [2013] [runrightfast.co]
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
'use strict';

var expect = require('chai').expect;
var lodash = require('lodash');

var ElasticSearchClient = require('..').ElasticSearchClient;
var EntityDatabase = require('..').EntityDatabase;
var Entity = require('runrightfast-commons').Entity;
var when = require('when');
var lodash = require('lodash');
var uuid = require('runrightfast-commons').uuid;

describe('EntityDatabase', function() {
	var ejs = new ElasticSearchClient({
		host : 'localhost',
		port : 9200
	}).ejs;

	var idsToDelete = [];

	var db = new EntityDatabase({
		ejs : ejs,
		index : 'EntityDatabaseSpec'.toLowerCase(),
		type : 'EntityDatabaseTestDoc'.toLowerCase(),
		entityConstructor : Entity,
		logLevel : 'WARN'
	});

	afterEach(function(done) {
		if (idsToDelete.length > 0) {
			when(db.deleteEntities(idsToDelete), function(result) {
				console.log('afterEach() : deleteEntities() : ' + result.items.length);
				idsToDelete = [];
				done();
			}, done);
		} else {
			done();
		}
	});

	it('can create a new Entity', function(done) {
		var entity = new Entity();
		idsToDelete.push(entity.id);
		when(db.createEntity(entity), function(result) {
			console.log(JSON.stringify(result, undefined, 2));
			done();
		}, done);
	});

	it('creating an Entity with the same id will fail', function(done) {
		var entity = new Entity();
		idsToDelete.push(entity.id);
		when(db.createEntity(entity), function(result) {
			console.log(JSON.stringify(result, undefined, 2));
			when(db.createEntity(entity), function(result) {
				console.log(JSON.stringify(result, undefined, 2));
				done(new Error('expected create to fail'));
			}, function(err) {
				console.log(err);
				expect(err.code).to.equal(409);
				done();
			});
		}, done);
	});

	it('can get a new Entity', function(done) {
		var entity = new Entity();
		idsToDelete.push(entity.id);
		when(db.createEntity(entity), function(result) {
			console.log('create response:\n' + JSON.stringify(result, undefined, 2));
			console.log('response type: ' + typeof result);
			console.log('result._id : ' + result._id);

			when(db.getEntity(result._id), function(result) {
				console.log('get response: ' + JSON.stringify(result, undefined, 2));
				done();
			}, done);
		}, done);
	});

	it('getting an Entity with an invalid id will fail', function(done) {
		when(db.getEntity(uuid()), function(result) {
			console.log('get response: ' + JSON.stringify(result, undefined, 2));
			done(new Error('expected entity to be not found'));
		}, function(err) {
			console.log(err);
			expect(err.info).to.exist;
			expect(err.code).to.equal(404);
			done();
		});
	});

	it('can set an Entity', function(done) {
		var entity = new Entity();
		idsToDelete.push(entity.id);
		when(db.createEntity(entity), function(result) {
			console.log('create response:\n' + JSON.stringify(result, undefined, 2));
			console.log('response type: ' + typeof result);
			console.log('result._id : ' + result._id);

			when(db.getEntity(result._id), function(result) {
				console.log('get response: ' + JSON.stringify(result, undefined, 2));

				var entity = result._source;
				entity.maxConns = 20;

				when(db.setEntity({
					entity : entity,
					version : result._version
				}), function(result) {
					console.log('update response : ' + JSON.stringify(result, undefined, 2));
					when(db.getEntity(result._id), function(result) {
						console.log('get response after update : ' + JSON.stringify(result, undefined, 2));
						done();
					});
				}, done);

			}, done);
		}, done);
	});

	it('setting an Entity with an expired version will fail', function(done) {
		var entity = new Entity();
		idsToDelete.push(entity.id);
		when(db.createEntity(entity), function(result) {
			console.log('create response:\n' + JSON.stringify(result, undefined, 2));
			console.log('response type: ' + typeof result);
			console.log('result._id : ' + result._id);

			when(db.getEntity(result._id), function(result) {
				console.log('get response: ' + JSON.stringify(result, undefined, 2));

				var entity = result._source;
				entity.maxConns = 20;

				when(db.setEntity({
					entity : entity,
					version : result._version
				}), function(result) {
					console.log('update response : ' + JSON.stringify(result, undefined, 2));
					when(db.getEntity(result._id), function(result) {
						console.log('get response after update : ' + JSON.stringify(result, undefined, 2));
						when(db.setEntity({
							entity : entity,
							version : result._version - 1
						}), function(result) {
							console.log(result);
							done(new Error('expected update to fail'));
						}, function(error) {
							console.log(error);
							expect(error.code).to.equal(409);
							done();
						});
					});
				}, done);

			}, done);
		}, done);
	});

	it('can retrieve multiple entities in a single request', function(done) {
		var promises = [];
		for ( var i = 0; i < 10; i++) {
			promises.push(when(db.createEntity(new Entity()), function(result) {
				return result;
			}, function(err) {
				return error;
			}));
		}

		var ids = [];
		when(when.all(promises), function(results) {
			lodash.forEach(results, function(result) {
				ids.push(result._id);
				idsToDelete.push(result._id);
			});
			console.log(ids);
			when(db.getEntities(ids), function(result) {
				console.log(JSON.stringify(result, undefined, 2));
				expect(result.docs.length).to.equal(10);
				done();
			}, done);
		}, done);
	});

	it('can create multiple entities in a bulk request', function(done) {
		var entities = [];
		for ( var i = 0; i < 10; i++) {
			entities.push(new Entity());
		}

		when(db.createEntities(entities), function(result) {
			console.log(JSON.stringify(result, undefined, 2));
			var items = result.items;
			expect(items.length).to.equal(10);
			items.forEach(function(item) {
				expect(item.index.ok).to.equal(true);
			});

			var ids = entities.map(function(entity) {
				return entity.id;
			});
			idsToDelete = idsToDelete.concat(ids);
			when(db.getEntities(ids), function(result) {
				console.log(JSON.stringify(result, undefined, 2));
				expect(result.docs.length).to.equal(10);
				done();
			}, done);

		}, done);

	});

	it('can delete an Entity', function(done) {
		var entity = new Entity();
		when(db.createEntity(entity), function(result) {
			console.log(JSON.stringify(result, undefined, 2));
			var deletePromise = when(db.deleteEntity(entity.id), function(result) {
				console.log('delete result: ' + JSON.stringify(result, undefined, 2));
				return result;
			}, done);

			when(deletePromise, function(result) {
				when(db.getEntity(result._id), function(result) {
					console.log(JSON.stringify(result, undefined, 2));
					done(new Error('expected entity to no exist'));
				}, function(err) {
					console.log(JSON.stringify(err, undefined, 2));
					done();

				});
			}, done);
		}, done);
	});

	it('can bulk delete entities', function(done) {
		var entities = [];
		for ( var i = 0; i < 10; i++) {
			entities.push(new Entity());
		}

		when(db.createEntities(entities), function(result) {
			console.log(JSON.stringify(result, undefined, 2));
			var items = result.items;
			expect(items.length).to.equal(10);
			items.forEach(function(item) {
				expect(item.index.ok).to.equal(true);
			});

			var ids = entities.map(function(entity) {
				return entity.id;
			});
			when(db.deleteEntities(ids), function(result) {
				console.log('bulf delete response: ' + JSON.stringify(result, undefined, 2));
				try {
					expect(result.items.length).to.equal(10);
					result.items.forEach(function(item) {
						console.log('item: ' + JSON.stringify(item, undefined, 2));
						expect(item['delete'].found).to.equal(true);
						expect(item['delete'].ok).to.equal(true);
					});
					done();
				} catch (err) {
					done(err);
				}
			}, done);

		}, done);
	});

	it('can count the total number of entities', function(done) {
		var entities = [];
		for ( var i = 0; i < 10; i++) {
			entities.push(new Entity());
		}

		when(db.getCount(), function(result) {
			console.log('count: ' + JSON.stringify(result, undefined, 2));
			var countBefore = result.count;

			when(db.createEntities(entities), function(result) {
				console.log(JSON.stringify(result, undefined, 2));
				var items = result.items;
				expect(items.length).to.equal(10);
				items.forEach(function(item) {
					expect(item.index.ok).to.equal(true);
				});

				var ids = entities.map(function(entity) {
					return entity.id;
				});
				idsToDelete = idsToDelete.concat(ids);
				when(db.getEntities(ids), function(result) {
					console.log(JSON.stringify(result, undefined, 2));
					expect(result.docs.length).to.equal(10);

					when(db.getCount(), function(result) {
						console.log('count: ' + JSON.stringify(result, undefined, 2));
						var countAfter = result.count;
						console.log('countBefore = ' + countBefore + ' | countAfter = ' + countAfter);
						done();
					}, done);

				}, done);

			}, done);

		}, done);

	});

	it('can find all and page through the results with default sort : updatedOn desc', function(done) {
		var entities = [];
		var promises = [];
		for ( var i = 0; i < 10; i++) {
			entities.push(new Entity());
			promises.push(db.createEntity(entities[i], true));
			idsToDelete = idsToDelete.concat(entities[i].id);
		}

		when(when.all(promises), function(result) {
			console.log('create results: ' + JSON.stringify(result, undefined, 2));
			when(db.findAll(), function(result) {
				console.log('db.getEntitiesByCreatedOn() result: ' + JSON.stringify(result, undefined, 2));
				console.log('result.hits.total = ' + result.hits.total);

				var updatedOn;
				var ids = [];
				try {
					result.hits.hits.forEach(function(hit) {
						ids.push(hit._id);
						if (lodash.isUndefined(updatedOn)) {
							updatedOn = hit._source.updatedOn;
						} else {
							expect(updatedOn).to.be.gte(hit._source.updatedOn);
							updatedOn = hit._source.updatedOn;
						}
					});

					promises = [];
					var from = 0;
					var searchParams = {
						from : from,
						pageSize : 2
					};
					while (promises.length < 5) {
						promises.push(db.findAll(searchParams));
						from += 2;
						searchParams = {
							from : from,
							pageSize : 2
						};
					}

					console.log('+++ promises.length = ' + promises.length);

					when(when.all(promises), function(results) {
						try {
							var page = 0;
							results.forEach(function(result) {
								expect(result.hits.hits.length).to.equal(2);
								console.log('page[' + page++ + '] : ' + JSON.stringify(result, undefined, 2));
							});
							done();
						} catch (err) {
							console.log(err);
						}
					}, done);

				} catch (err) {
					done(err);
				}
			}, done);
		}, done);
	});

	it('can find all and page through the results with specified sort', function(done) {
		var entities = [];
		var promises = [];
		for ( var i = 0; i < 10; i++) {
			entities.push(new Entity());
			promises.push(db.createEntity(entities[i], true));
			idsToDelete = idsToDelete.concat(entities[i].id);
		}

		when(when.all(promises), function(result) {
			console.log('create results: ' + JSON.stringify(result, undefined, 2));
			when(db.findAll({
				sort : {
					field : 'createdOn',
					descending : false
				}
			}), function(result) {
				console.log('db.getEntitiesByCreatedOn() result: ' + JSON.stringify(result, undefined, 2));
				console.log('result.hits.total = ' + result.hits.total);

				var createdOn;
				try {
					result.hits.hits.forEach(function(hit) {
						if (lodash.isUndefined(createdOn)) {
							createdOn = hit._source.createdOn;
						} else {
							expect(createdOn).to.be.lte(hit._source.createdOn);
							createdOn = hit._source.createdOn;
						}
					});

					done();
				} catch (err) {
					done(err);
				}
			}, done);
		}, done);
	});
});