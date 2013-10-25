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
 *   entityConstructor						// REQUIRED - Entity constructor
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
			log.debug(options);
		}

		this.Entity = options.entityConstructor;
		this.ejs = options.ejs;
	};

	/**
	 * 
	 * 
	 * @param entity
	 * @return Promise that returns the created Entity and cas - as an object
	 *         with 'schema' and 'cas' properties. If an Error occurs, it will
	 *         have an error code - defined by errorCodes keys
	 */
	EntityDatabase.prototype.createEntity = function(entity) {
		throw new Error('NOT IMPLEMENTED');
	};

	/**
	 * 
	 * @param namespace
	 *            REQUIRED
	 * @param version
	 *            REQUIRED
	 * @return Promise that returns the Entity and cas if found - as an object
	 *         with 'value' and 'cas' properties. If the Entity does not exist,
	 *         then an Error with code NOT_FOUND is returned
	 * 
	 * returned object has the following properties: <code>
	 * cas			Couchbase CAS
	 * value		Entity object
	 * </code>
	 */
	EntityDatabase.prototype.getEntity = function(id) {
		throw new Error('NOT IMPLEMENTED');
	};

	/**
	 * The updatedOn will be set to the current time
	 * 
	 * @param entity
	 * @param cas
	 *            REQUIRED used to ensure that no one else has updated the
	 *            schema since it was retrieved
	 * @param updatedBy
	 *            OPTIONAL
	 * @return Promise - If successful, the returned object has the following
	 *         properties:
	 * 
	 * <code>
	 * cas			Couchbase CAS
	 * value		Entity object
	 * </code>
	 */
	EntityDatabase.prototype.updateEntity = function(entity, cas, updatedBy) {
		throw new Error('NOT IMPLEMENTED');
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
