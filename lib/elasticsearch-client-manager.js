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
 *   host : [ 'localhost:8091' ],						// REQUIRED
 *   buckets : [										// REQUIRED
 *   	 {  bucket : 'default',							// REQUIRED - physical bucket name, which must be unique
 *			password : 'password',						// OPTIONAL
 *			aliases : ['alias'] }						// OPTIONAL - alias bucket names, which must be unique. Default is the bucket name	 
 *   ], 
 *   connectionListener : function(error,bucket){},		// OPTIONAL
 *   connectionErrorListener : function(bucket){},		// OPTIONAL
 *   logLevel : 'WARN' 									// OPTIONAL - Default is 'WARN'
 * }
 * </code>
 */
(function() {
	'use strict';

	var lodash = require('lodash');
	var Hoek = require('hoek');
	var assert = Hoek.assert;
	var ElasticSearchClient = require('./elasticsearch-client.js');

	var ElasticSearchClientManager = function(options) {

	};

	var elasticSearchClientManager = new ElasticSearchClientManager();

	module.exports = elasticSearchClientManager;
}());
