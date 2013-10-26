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

	var db = new EntityDatabase({
		ejs : ejs,
		index : 'EntityDatabaseSpec'.toLowerCase(),
		type : 'EntityDatabaseTestDoc'.toLowerCase(),
		entityConstructor : Entity,
		logLevel : 'DEBUG'
	});

	it('can create a new Entity', function(done) {
		var entity = new Entity();
		when(db.createEntity(entity), function(result) {
			console.log(JSON.stringify(result, undefined, 2));
			done();
		}, done);
	});

	it('creating an Entity with the same id will fail', function(done) {
		var entity = new Entity();
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
			});
			console.log(ids);
			when(db.getEntities(ids), function(result) {
				console.log(JSON.stringify(result, undefined, 2));
				expect(result.docs.length).to.equal(10);
				done();
			}, done);
		}, done);
	});
});