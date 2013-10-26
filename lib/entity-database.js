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
 * 	 ejs: ejs,								// REQUIRED - Couchbase.Connection
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

	var dateType = joi.types.Object();
	dateType.isDate = function() {
		this.add('isDate', function(value, obj, key, errors, keyPath) {
			if (lodash.isDate(value)) {
				return true;
			}
			errors.add(key + ' must be a Date', keyPath);
			return false;

		}, arguments);
		return this;
	};

	var DateType = dateType.isDate();

	var validateIdsArray = function validateIdsArray(ids) {
		var schema = {
			ids : joi.types.Array().required().includes(joi.types.String())
		};
		var validationError = joi.validate({
			ids : ids
		}, schema);
		if (validationError) {
			throw validationError;
		}
	};

	var updateEntityParamsSchema = {
		entity : joi.types.Object().required(),
		version : joi.types.Number().min(1),
		updatedBy : joi.types.String()
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
	 * 
	 * 
	 * @param entity
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
	EntityDatabase.prototype.createEntity = function(entity) {
		var self = this;
		return when.promise(function(resolve, reject) {
			if (!lodash.isObject(entity)) {
				reject(new Error('entity is required'));
				if (log.isDebugEnabled()) {
					log.debug('createEntity(): entity is not an object\n' + JSON.stringify(entity, undefined, 2));
				}
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

			var payload = lodash.foldl(entities, function(payload, entity) {
				payload += JSON.stringify({
					index : {
						_index : self.index,
						_type : self.type,
						_id : entity.id
					}
				});
				payload += '\n';
				payload += JSON.stringify(entity);
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

	/**
	 * If an Entity with the same id already exists, then it sill replace it,
	 * otherwise a new Entity is created.
	 * 
	 * The updatedOn will be set to the current time.
	 * 
	 * @param entity
	 * @param version
	 *            REQUIRED used to ensure that no one else has updated the
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
	 * @param namespace
	 * @param version
	 * @return Promise
	 */
	EntityDatabase.prototype.deleteEntity = function(id) {
		var self = this;
		return when.promise(function(resolve, reject) {
			if (!lodash.isString(id)) {
				reject(new Error('id is required and must be a String'));
				return;
			}

			var doc = self.ejs.Document(self.index, self.type, id);
			doc.doDelete(resolve, reject);
		});
	};

	/**
	 * 
	 * 
	 * @param ids
	 *            REQUIRED - REQUIRED - Array of entity ids.
	 * 
	 * @return Promise where the result is the Couchbase cas for each deleted
	 *         document. If an error occurs, then an object containing both the
	 *         error and result is returned in order to inspect what went wrong.
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

	/**
	 * delegates to queryByDateField() with params.dateField = 'createdOn'
	 * 
	 * @see queryByDateField
	 */
	EntityDatabase.prototype.getEntitiesByCreatedOn = function(params) {
		throw new Error('NOT IMPLEMENTED');
	};

	/**
	 * delegates to queryByDateField() with params.dateField = 'updatedOn'
	 * 
	 * @see queryByDateField
	 */
	EntityDatabase.prototype.getEntitiesByUpdatedOn = function(params) {
		throw new Error('NOT IMPLEMENTED');
	};

	/**
	 * delegates to queryByDateField() with params.dateField = 'updatedOn'
	 * 
	 * @see queryByDateField
	 */
	EntityDatabase.prototype.getCount = function() {
		var self = this;
		return when.promise(function(resolve, reject) {
			var request = self.ejs.Request({
				indices : self.index,
				types : self.type
			});

			request.query(self.ejs.MatchAllQuery()).doCount(resolve, reject);
		});
	};

	module.exports = EntityDatabase;
}());
