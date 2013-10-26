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
 *   type: 'document_type'					// OPTIONAL - If not specified, then the Enitity._entityType will be required - Enitity._entityType will trump type
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

	var EntityDatabase = function(options) {
		assert(lodash.isObject(options), 'options is required');
		var optionsSchema = {
			ejs : joi.types.Object().required(),
			index : joi.types.String().required(),
			type : joi.types.String(),
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
	 */
	EntityDatabase.prototype.createEntity =
			function(entity) {
				var self = this;
				return when
						.promise(function(resolve, reject) {
							if (!lodash.isObject(entity)) {
								reject(new Error('entity is required'));
								if (log.isDebugEnabled()) {
									log.debug('createEntity(): entity is not an object\n' + JSON.stringify(entity, undefined, 2));
								}
								return;
							}

							if (log.isDebugEnabled()) {
								log.debug('createEntity():\n' + JSON.stringify(entity, undefined, 2));
							}

							var newSchema;
							try {
								newSchema = new self.Entity(entity);
							} catch (err) {
								err.code = 'INVALID_OBJ_SCHEMA';
								reject(err);
								return;
							}

							var type = newSchema._entityType || self.type;
							if (!type) {
								reject(new Error(
										"type is required - it either needs to configured on the EntityDatabase via the 'type' option or specified on the entity via '_entityType'"));
							}

							var doc = self.ejs.Document(self.index, type, newSchema.id);
							doc.opType('create');
							doc.source(newSchema);
							doc.doIndex(resolve, reject);
						});
			};

	/**
	 * 
	 * @param namespace
	 *            REQUIRED
	 * @param version
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
	 *
	 * </code>
	 */
	EntityDatabase.prototype.getEntity = function(id, type) {
		var self = this;
		return when.promise(function(resolve, reject) {
			if (!lodash.isString(id)) {
				reject(new Error('id is required and must be a String'));
				return;
			}
			var indexType = type || self.type;
			if (log.isDebugEnabled()) {
				log.debug('indexType = ' + indexType);
			}

			var doc = self.ejs.Document(self.index, indexType, id);
			doc.doGet(resolve, reject);
		});
	};

	var updateEntityParamsSchema = {
		entity : joi.types.Object().required(),
		version : joi.types.Number().min(1),
		updatedBy : joi.types.String()
	};

	/**
	 * The updatedOn will be set to the current time
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
	EntityDatabase.prototype.updateEntity = function(params) {
		var self = this;
		return when.promise(function(resolve, reject) {
			var err = joi.validate(params, updateEntityParamsSchema);
			if (err) {
				reject(err);
				return;
			}

			var newSchema;
			try {
				newSchema = new self.Entity(params.entity);
			} catch (err) {
				err.code = 'INVALID_OBJ_SCHEMA';
				reject(err);
				return;
			}

			newSchema.updated(params.updatedBy);
			var type = newSchema._entityType || self.type;

			var doc = self.ejs.Document(self.index, type, newSchema.id);
			doc.source(newSchema);
			if (params.version) {
				doc.version(params.version);
			}
			doc.doIndex(resolve, reject);
		});
	};

	/**
	 * 
	 * @param ids
	 *            REQUIRED - Array of entity ids.
	 * @return Promise that returns an dictionary of entities that were found.:
	 * 
	 * <code>
	 * entityId -> {
	 * 						cas 			// Couchbase CAS
	 * 						value			// Entity
	 *  				 }
	 * <code>
	 *
	 */
	EntityDatabase.prototype.getEntities = function(ids) {
		throw new Error('NOT IMPLEMENTED');
	};

	/**
	 * 
	 * 
	 * @param namespace
	 * @param version
	 * @return Promise
	 */
	EntityDatabase.prototype.deleteEntity = function(id) {
		throw new Error('NOT IMPLEMENTED');
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
		throw new Error('NOT IMPLEMENTED');
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

	EntityDatabase.prototype.errorCodes = {
		DUP_NS_VER : 'An Entity with the same name id (namespace/version) already exists.',
		INVALID_OBJ_SCHEMA : 'The object schema is invalid.',
		UNEXPECTED_ERR : 'Unexpected error.',
		NOT_FOUND : 'Not found',
		STALE_OBJ : 'Entity is stale - an newer version is available'
	};

	module.exports = EntityDatabase;
}());
