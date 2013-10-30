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

/**
 * <code>
 * options = { 
 * 	 ejs: ejs,								// REQUIRED - elastic.js - ejs
 *   logLevel : 'WARN',						// OPTIONAL - Default is 'WARN',
 *   entityConstructor,						// REQUIRED - Entity constructor,
 *   index: 'index_name'					// REQUIRED - Index name - where the entity will be stored,
 *   type: 'document_type'					// REQUIRED - Type within the Index
 * }
 * </code>
 */
(function() {
	'use strict';

	var logging = require('runrightfast-commons').logging;
	var log = logging.getLogger('entity-database');
	var lodash = require('lodash');
	var Hoek = require('hoek');
	var assert = Hoek.assert;
	var when = require('when');
	var joi = require('joi');
	var extend = require('extend');

	var validateIdsArraySchema = {
		ids : joi.types.Array().required().includes(joi.types.String())
	};

	var validateIdsArray = function validateIdsArray(ids) {
		var err = joi.validate({
			ids : ids
		}, validateIdsArraySchema);
		if (err) {
			throw err;
		}
	};

	var getSortOrder = function getSortOrder(descending) {
		return descending ? 'desc' : 'asc';
	};

	var checkElasticsearchResult = function checkElasticsearchResult(resolve, reject, result) {
		if (log.isDebugEnabled()) {
			log.debug(JSON.stringify(result, undefined, 2));
		}
		if (result.error) {
			var err = new Error(result.error);
			err.code = result.status;
			reject(err);
		} else {
			resolve(result);
		}
	};

	var EntityDatabase = function(options) {
		assert(lodash.isObject(options), 'options is required');
		var optionsSchema = {
			ejs : joi.types.Object().required(),
			index : joi.types.String().required(),
			type : joi.types.String().required(),
			entityConstructor : joi.types.Function().required(),
			logLevel : joi.types.String()
		};

		var err = joi.validate(options, optionsSchema);
		if (err) {
			throw err;
		}

		var logLevel = options.logLevel || 'WARN';
		logging.setLogLevel(log, logLevel);
		if (log.isDebugEnabled()) {
			log.debug(JSON.stringify(options, undefined, 2));
		}

		this.Entity = options.entityConstructor;
		this.ejs = options.ejs;
		this.index = options.index.toLowerCase();
		if (options.type) {
			this.type = options.type.toLowerCase();
		}
	};

	/**
	 * @param entity
	 *            REQUIRED
	 * @param refresh
	 *            OPTIONAL - Enables the index to be refreshed immediately after
	 *            the operation occurs. This is an advanced setting and can lead
	 *            to performance issues.
	 * @return Promise that returns elasticsearch create response:
	 * 
	 * <code> 
	 * {
	 *  "ok": true,
	 *	  "_index": "index_name",
	 *	  "_type": "type_name",
	 *	"_id": "4dfa9961b2a74ed78b3b34fbdcf0e7a9",
	 *	"_version": 1
	 * }
	 * </code>
	 * 
	 * if the create fails, then an error message object is returned, e.g.,
	 * 
	 * <code>
	 * { error: 'DocumentAlreadyExistsException[[entitydatabasespec][1] [entitydatabasetestdoc][bbddf97584e04d3a8f7310ce8629ecf7]: document already exists]',
	 *   status: 409 }
	 * </code>
	 */
	EntityDatabase.prototype.createEntity = function(entity, refresh) {
		var self = this;
		return when.promise(function(resolve, reject) {
			if (!lodash.isObject(entity)) {
				reject(new Error('entity is required'));
				if (log.isDebugEnabled()) {
					log.debug('createEntity(): entity is not an object\n' + JSON.stringify(entity, undefined, 2));
				}
				return;
			}

			if (!lodash.isUndefined(refresh) && !lodash.isBoolean(refresh)) {
				reject(new Error('refresh must be a Boolean'));
				return;
			}

			var newEntity;
			try {
				newEntity = new self.Entity(entity);
			} catch (err) {
				err.code = 'INVALID_OBJ_SCHEMA';
				reject(err);
				return;
			}

			var doc = self.ejs.Document(self.index, self.type, newEntity.id);
			doc.opType('create');
			doc.refresh(!!refresh);
			doc.source(newEntity);
			doc.doIndex(checkElasticsearchResult.bind(null, resolve, reject), reject);
		});
	};

	/**
	 * 
	 * @param entities
	 * @returns Promise
	 */
	EntityDatabase.prototype.createEntities = function(entities) {
		var self = this;
		return when.promise(function(resolve, reject) {
			if (!lodash.isArray(entities)) {
				reject(new Error('entities is required to be an Array'));
				if (log.isDebugEnabled()) {
					log.debug('createEntities(): entities is not an array\n' + JSON.stringify(entities, undefined, 2));
				}
				return;
			}

			if (entities.length === 0) {
				resolve();
				return;
			}

			if (log.isDebugEnabled()) {
				log.debug('createEntities():\n' + JSON.stringify(entities, undefined, 2));
			}

			var newEntity;
			var payload = lodash.foldl(entities, function(payload, entity) {
				newEntity = new self.Entity(entity);
				payload += JSON.stringify({
					index : {
						_index : self.index,
						_type : self.type,
						_id : entity.id
					}
				});
				payload += '\n';
				payload += JSON.stringify(newEntity);
				payload += '\n';
				return payload;
			}, '');

			/*
			 * elastic.js does not support the elasticsearch's bulk api, so
			 * let's make the HTTP request ourselves
			 */
			self.ejs.client.post('/' + self.index + '/' + self.type + '/_bulk', payload, resolve, reject);
		});
	};

	/**
	 * 
	 * @param id
	 *            REQUIRED
	 * @return Promise that returns the elasticsearch response.
	 * 
	 * <code>
	 *  {
	 *	  "_index": "index_name",
	 *	  "_type": "type_name",
	 *	  "_id": "4dfa9961b2a74ed78b3b34fbdcf0e7a9",
	 *	  "_version": 1,
	 *	  "exists": true,
	 *	  "_source": {
	 *	    "id": "4dfa9961b2a74ed78b3b34fbdcf0e7a9",
	 *	    "createdOn": "2013-10-26T00:31:45.754Z",
	 *	    "updatedOn": "2013-10-26T00:31:45.754Z",
	 *      // other entity props
	 *	  }
	 *	}
	 * </code>
	 * 
	 * if the entity does not exist, then an Error is returned with 2 additional
	 * properties:
	 * 
	 * <code>
	 * code			ERROR CODE - will be 404 for Entities that do not exist
	 * info			Additional error info - this will only be set for 404 errors, which provides the elasticsearch index and type searched	 
	 * </code>
	 */
	EntityDatabase.prototype.getEntity = function(id) {
		var self = this;
		return when.promise(function(resolve, reject) {
			if (!lodash.isString(id)) {
				reject(new Error('id is required and must be a String'));
				return;
			}

			var doc = self.ejs.Document(self.index, self.type, id);
			doc.doGet(function(result) {
				var err;
				if (result.exists) {
					resolve(result);
				} else if (lodash.isBoolean(result.exists)) {
					err = new Error('Entity does not exist');
					err.code = 404;
					err.info = result;
					reject(err);
				} else if (result.error) {
					err = new Error(result.error);
					err.code = result.code;
					reject(err);
				} else {
					resolve(result);
				}
			}, reject);
		});
	};

	var updateEntityParamsSchema = {
		entity : joi.types.Object().required(),
		version : joi.types.Number().min(1),
		updatedBy : joi.types.String()
	};

	/**
	 * If an Entity with the same id already exists, then it will replace it,
	 * otherwise a new Entity is created.
	 * 
	 * The updatedOn will be set to the current time.
	 * 
	 * @param entity
	 * @param version
	 *            OPTIONAL used to ensure that no one else has updated the
	 *            schema since it was retrieved
	 * @param updatedBy
	 *            OPTIONAL
	 * @return Promise - If successful, the returned object has the following
	 *         properties:
	 * 
	 * <code>
	 *  {
	 *	  "ok": true,
	 *	  "_index": "index_name",
	 *	  "_type": "type_name",
	 *	  "_id": "4dfa9961b2a74ed78b3b34fbdcf0e7a9",
	 *	  "_version": 2
	 *	}
	 * </code>
	 */
	EntityDatabase.prototype.setEntity = function(params) {
		var self = this;
		return when.promise(function(resolve, reject) {
			var err = joi.validate(params, updateEntityParamsSchema);
			if (err) {
				reject(err);
				return;
			}

			var newEntity;
			try {
				newEntity = new self.Entity(params.entity);
			} catch (error) {
				error.code = 'INVALID_OBJ_SCHEMA';
				reject(error);
				return;
			}

			newEntity.updated(params.updatedBy);
			var type = newEntity._entityType || self.type;

			var doc = self.ejs.Document(self.index, type, newEntity.id);
			doc.source(newEntity);
			if (params.version) {
				doc.version(params.version);
			}
			doc.doIndex(checkElasticsearchResult.bind(null, resolve, reject), reject);
		});

	};

	/**
	 * 
	 * @param ids
	 *            REQUIRED - Array of entity ids.
	 * @return Promise
	 * 
	 */
	EntityDatabase.prototype.getEntities = function(ids) {
		var self = this;
		var ejs = this.ejs;
		return when.promise(function(resolve, reject) {
			try {
				validateIdsArray(ids);
			} catch (error) {
				reject(error);
				return;
			}

			/*
			 * elastic.js does not support the elasticsearch's multi get api, so
			 * let's make the HTTP request ourselves
			 */
			ejs.client.post('/' + self.index + '/' + self.type + '/_mget', JSON.stringify({
				ids : ids
			}), resolve, reject);
		});
	};

	/**
	 * 
	 * 
	 * @param id
	 * @param refresh
	 * @return Promise
	 * 
	 * sample response:
	 * 
	 * <code>
	 *  {
	 *	  "ok": true,
	 *	  "found": true,
	 *	  "_index": "entitydatabasespec",
	 *	  "_type": "entitydatabasetestdoc",
	 *	  "_id": "49f45542b6d54c4babe987d658d48c5d",
	 *	  "_version": 2
	 *	}
	 * </code>
	 */
	EntityDatabase.prototype.deleteEntity = function(id,refresh) {
		var self = this;
		return when.promise(function(resolve, reject) {
			if (!lodash.isString(id)) {
				reject(new Error('id is required and must be a String'));
				return;
			}

			if (!lodash.isUndefined(refresh) && !lodash.isBoolean(refresh)) {
				reject(new Error('refresh must be a Boolean'));
				return;
			}

			var doc = self.ejs.Document(self.index, self.type, id);
			doc.refresh(!!refresh);
			doc.doDelete(resolve, reject);
		});
	};

	/**
	 * 
	 * 
	 * @param ids
	 *            REQUIRED - Array of entity ids.
	 * 
	 * @return Promise where the result is elasticsearch response
	 * 
	 * sample response:
	 * 
	 * <code>
	 * {
	 *	  "took": 1,
	 *	  "items": [
	 *	    {
	 *	      "delete": {
	 *	        "_index": "entitydatabasespec",
	 *	        "_type": "entitydatabasetestdoc",
	 *	        "_id": "a5f680b2804c4b0bb5eb367d06d89d43",
	 *	        "_version": 2,
	 *	        "ok": true,
	 *	        "found": true
	 *	      }
	 *	    },
	 *	    {
	 *	      "delete": {
	 *	        "_index": "entitydatabasespec",
	 *	        "_type": "entitydatabasetestdoc",
	 *	        "_id": "11e55d7a4a6442778db5f7ab557f97d6",
	 *	        "_version": 2,
	 *	        "ok": true,
	 *	        "found": true
	 *	      }
	 *	    }
	 *	  ]
	 *	}
	 *
	 * </code>
	 */
	EntityDatabase.prototype.deleteEntities = function(ids) {
		var self = this;
		return when.promise(function(resolve, reject) {
			if (!lodash.isArray(ids)) {
				reject(new Error('ids is required to be an Array'));
				if (log.isDebugEnabled()) {
					log.debug('deleteEntities(): ids is not an array\n' + JSON.stringify(ids, undefined, 2));
				}
				return;
			}

			if (ids.length === 0) {
				resolve();
				return;
			}

			validateIdsArray(ids);

			if (log.isDebugEnabled()) {
				log.debug('ids():\n' + JSON.stringify(ids, undefined, 2));
			}

			var payload = lodash.foldl(ids, function(payload, id) {
				payload += JSON.stringify({
					"delete" : {
						_index : self.index,
						_type : self.type,
						_id : id
					}
				});
				payload += '\n';
				return payload;
			}, '');

			/*
			 * elastic.js does not support the elasticsearch's bulk api, so
			 * let's make the HTTP request ourselves
			 */
			self.ejs.client.post('/' + self.index + '/' + self.type + '/_bulk', payload, resolve, reject);
		});
	};

	EntityDatabase.prototype.refreshIndex = function(){
		var self = this;
		return when.promise(function(resolve, reject) {			
			self.ejs.client.post('/' + self.index + '/_refresh', '', resolve, reject);
		});
	};

	/**
	 * 
	 * @returns elastic.js Request initialized with the EntityDatabase's index
	 *          and type
	 */
	EntityDatabase.prototype.request = function() {
		return this.ejs.Request({
			indices : this.index,
			types : this.type
		});
	};

	/**
	 * 
	 * @param {Object}searchParams
	 * 
	 * <code>
	 * timeout			OPTIONAL - A timeout, bounding the request to be executed within the specified time value and bail when expired. Defaults to no timeout.
	 * returnFields		OPTIONAL - By default, searches return full documents, meaning every property or field. 
	 * 						       This method allows you to specify which fields you want returned. 
	 * 							   Pass a single field name or an array of fields.
	 * version			OPTIONAL - Enable/Disable returning version number for each search result.		
	 * from				OPTIONAL - Number - The lower bound. Defaults to start from the first.
	 * pageSize			OPTIONAL - Number - Sets the number of results/documents to be returned. This is set on a per page basis.
	 * sort:{					
	 *   field			OPTIONAL - Sets the sort field for the query - default is updatedOn
	 *   descending		OPTIONAL - If set to true, then sort order is descending - by default it is descending = true
	 * }
	 * </code>
	 * 
	 * @param {Object}schema
	 *            Joi schema used to validate the searchParams. If validation
	 *            fails, an error is thrown.
	 */
	EntityDatabase.prototype.newSearchRequest = function(searchParams, schema) {
		var err = joi.validate(searchParams, schema);
		if (err) {
			throw err;
		}

		var request = this.request();
		if (!lodash.isUndefined(searchParams.sort)) {
			request.sort(searchParams.sort.field, getSortOrder(searchParams.sort.descending));
		}

		if (!lodash.isUndefined(searchParams.timeout)) {
			request.timeout(searchParams.timeout);
		}
		if (!lodash.isUndefined(searchParams.returnFields)) {
			request.fields(searchParams.returnFields);
		}
		if (!lodash.isUndefined(searchParams.version)) {
			request.version(searchParams.version);
		}
		if (!lodash.isUndefined(searchParams.from)) {
			request.from(searchParams.from);
		}
		if (!lodash.isUndefined(searchParams.pageSize)) {
			request.size(searchParams.pageSize);
		}

		if (log.isDebugEnabled()) {
			log.debug('newSearchRequest() : ' + request);
		}

		return request;
	};

	/**
	 * 
	 * @param {Object}params
	 * 
	 * <code>
	 * field			REQUIRED - field name
	 * range:{			OPTIONAL
	 *   from			OPTIONAL - Object - The lower bound. Defaults to start from the first. Type depends on field type.
	 *   to				OPTIONAL - Object - The upper bound. Defaults to unbounded. Type depends on field type
	 *   includeLower	OPTIONAL - Should the first from (if set) be inclusive or not. Defaults to true
	 * 	 includeUpper	OPTIONAL - Should the last to (if set) be inclusive or not. Defaults to true. 
	 * }
	 * </code>
	 */
	EntityDatabase.prototype.newRangeFilter = function(params) {
		var query = this.ejs.RangeFilter(params.field);

		if (!lodash.isUndefined(params.range)) {
			if (!lodash.isUndefined(params.range.from)) {
				query.from(params.range.from);
			}
			if (!lodash.isUndefined(params.range.to)) {
				query.to(params.range.to);
			}
			if (!lodash.isUndefined(params.range.includeLower)) {
				query.includeLower(params.range.includeLower);
			}
			if (!lodash.isUndefined(params.range.includeUpper)) {
				query.includeUpper(params.range.includeUpper);
			}
		}

		return query;
	};

	var findByFieldParamsSchema = {
		// Request settings
		timeout : joi.types.Number(),
		returnFields : joi.types.Array().includes(joi.types.String()),
		version : joi.types.Boolean(),
		from : joi.types.Number(),
		pageSize : joi.types.Number(),
		sort : joi.types.Object({
			field : joi.types.String().required(),
			descending : joi.types.Boolean()
		}),
		// RangeQuery settings
		field : joi.types.String().required(),
		range : joi.types.Object({
			from : joi.types.Object(),
			to : joi.types.Object(),
			includeLower : joi.types.Boolean(),
			includeUpper : joi.types.Boolean(),
		}).without('value'),
		value : joi.types.Any().without('range')
	};

	/**
	 * 
	 * @param {Object}params
	 * 
	 * <code>
	 * field			REQUIRED - field name
	 * timeout			OPTIONAL - A timeout, bounding the request to be executed within the specified time value and bail when expired. Defaults to no timeout.
	 * returnFields		OPTIONAL - By default, searches return full documents, meaning every property or field. 
	 * 						       This method allows you to specify which fields you want returned. 
	 * 							   Pass a single field name or an array of fields.
	 * version			OPTIONAL - Enable/Disable returning version number for each search result.		
	 * from				OPTIONAL - Number - The lower bound. Defaults to start from the first.
	 * pageSize			OPTIONAL - Number - Sets the number of results/documents to be returned. This is set on a per page basis.
	 * sort:{					
	 *   field			OPTIONAL - Sets the sort field for the query - default is updatedOn
	 *   descending		OPTIONAL - If set to true, then sort order is descending - by default it is descending = true
	 * }
	 * value			OPTIONAL - mutually exclusive with 'range'
	 * range:{			OPTIONAL - mutually exclusive with 'value'
	 *   from			OPTIONAL - Object - The lower bound. Defaults to start from the first. Type depends on field type.
	 *   to				OPTIONAL - Object - The upper bound. Defaults to unbounded. Type depends on field type
	 *   includeLower	OPTIONAL - Should the first from (if set) be inclusive or not. Defaults to true
	 * 	 includeUpper	OPTIONAL - Should the last to (if set) be inclusive or not. Defaults to true. 
	 * }
	 * </code>
	 */
	EntityDatabase.prototype.findByField = function(params) {
		var self = this;
		return when.promise(function(resolve, reject) {
			var request = self.newSearchRequest(params, findByFieldParamsSchema);
			if (!lodash.isUndefined(params.value)) {
				request.filter(self.ejs.TermFilter(params.field, params.value));
			} else {
				request.filter(self.newRangeFilter(params));
			}
			request.doSearch(checkElasticsearchResult.bind(null, resolve, reject), reject);
		});
	};

	var findAllParamsSchema = {
		timeout : joi.types.Number(),
		returnFields : joi.types.Array().includes(joi.types.String()),
		version : joi.types.Boolean(),
		from : joi.types.Number(),
		pageSize : joi.types.Number(),
		sort : joi.types.Object({
			field : joi.types.String().required(),
			descending : joi.types.Boolean()
		})
	};

	/**
	 * 
	 * @param {Object}params
	 * 
	 * <code> 
	 * timeout			OPTIONAL - Number - A timeout, bounding the request to be executed within the specified time value and bail when expired. Defaults to no timeout.
	 * returnFields		OPTIONAL - By default, searches return full documents, meaning every property or field. 
	 * 						       This method allows you to specify which fields you want returned. 
	 * 							   Pass a single field name or an array of fields.
	 * version			OPTIONAL - Boolean - Enable/Disable returning version number for each search result.		
	 * from				OPTIONAL - Number - The lower bound. Defaults to start from the first.
	 * pageSize			OPTIONAL - Number - Sets the number of results/documents to be returned. This is set on a per page basis.
	 * sort:{					
	 *   field			OPTIONAL - String - Sets the sort field for the query - default is 'updatedOn'
	 *   descending		OPTIONAL - Boolean - If set to true, then sort order is descending - default is descending = true
	 * }
	 * </code>
	 */
	EntityDatabase.prototype.findAll = function(params) {
		var self = this;
		return when.promise(function(resolve, reject) {
			var searchParams = {
				sort : {
					field : 'updatedOn',
					descending : true
				}
			};

			extend(searchParams, params);
			if (log.isDebugEnabled()) {
				log.debug('findAll() : params : ' + JSON.stringify(searchParams, undefined, 2));
			}

			var request = self.newSearchRequest(searchParams, findAllParamsSchema);
			request.query(self.ejs.MatchAllQuery());
			request.doSearch(checkElasticsearchResult.bind(null, resolve, reject), reject);
		});

	};

	/**
	 * delegates to queryByDateField() with params.dateField = 'updatedOn'
	 * 
	 * @see queryByDateField
	 */
	EntityDatabase.prototype.getCount = function() {
		var self = this;
		return when.promise(function(resolve, reject) {
			var request = self.request();

			request.query(self.ejs.MatchAllQuery()).doCount(resolve, reject);
		});
	};

	EntityDatabase.prototype.getMapping = function(){
		var self = this;
		return when.promise(function(resolve,reject){
			self.ejs.client.get('/' + self.index + '/' + self.type + '/_mapping', '',resolve, reject);	
		});		
	};

	EntityDatabase.prototype.setMapping = function(mapping){
		var self = this;
		return when.promise(function(resolve,reject){
			if(!lodash.isObject(mapping)){
				reject(new Error('mapping is required'));
				return;
			}
			self.ejs.client.post('/' + self.index + '/' + self.type + '/_mapping', JSON.stringify(mapping),resolve, reject);	
		});		
	};

	EntityDatabase.prototype.deleteIndex = function(){
		var self = this;
		return when.promise(function(resolve,reject){			
			self.ejs.client.del('/' + self.index, '',resolve, reject);	
		});		
	};

	EntityDatabase.prototype.createIndex = function(settings){
		var self = this;
		return when.promise(function(resolve,reject){
			if(!lodash.isObject(settings)){
				reject(new Error('settings is required'));
				return;
			}
			self.ejs.client.put('/' + self.index, JSON.stringify(settings),resolve, reject);	
		});		
	};

	module.exports = EntityDatabase;
}());
