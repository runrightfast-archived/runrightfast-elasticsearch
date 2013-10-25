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
 * 	 couchbase : {										// REQUIRED
 * 		host : [ 'localhost:8091' ],					// REQUIRED 
 *		bucket : 'default',								// REQUIRED 
 *		password : 'password' 							// OPTIONAL
 *   },
 *   connectionListener : function(error){},			// OPTIONAL
 *   connectionErrorListener : function(){},			// OPTIONAL
 *   logLevel : 'WARN' 									// OPTIONAL - Default is WARN
 *   
 * }
 * </code>
 */
(function() {
	'use strict';

	var logging = require('runrightfast-commons').logging;
	var log = logging.getLogger('elasticsearch-client');
	var joi = require('joi');
	var ejs = require('elastic.js');
	var nc = require('elastic.js/elastic-node-client');

	var ElasticSearchClient = function ElasticSearchClient(options) {
		var schema = {
			host : joi.types.String().required(),
			port : joi.types.Number().min(0).required(),
			https : joi.types.Boolean(),
			logLevel : joi.types.String()
		};

		var err = joi.validate(options, schema);
		if (err) {
			throw err;
		}

		var logLevel = options.logLevel || 'WARN';
		logging.setLogLevel(log, logLevel);
		if (log.isLevelEnabled('DEBUG')) {
			log.debug(options);
		}

		ejs.client = nc.NodeClient('localhost', '9200');

	};

	module.exports = ElasticSearchClient;
}());
