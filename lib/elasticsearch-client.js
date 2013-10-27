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
 * host 			REQUIRED 
 * port 			REQUIRED 
 * </code>
 */
(function() {
	'use strict';

	var joi = require('joi');

	var nc = require('elastic.js/elastic-node-client');

	var schema = {
		host : joi.types.String().required(),
		port : joi.types.Number().min(0).required()
	};

	var ElasticSearchClient = function ElasticSearchClient(options) {
		var err = joi.validate(options, schema);
		if (err) {
			throw err;
		}

		this.ejs = require('elastic.js');
		this.ejs.client = nc.NodeClient(options.host, options.port);
	};

	module.exports = ElasticSearchClient;
}());
