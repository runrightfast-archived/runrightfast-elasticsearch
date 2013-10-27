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

describe('ElasticSearchClient', function() {
	it('must be constructed with a host and port', function(done) {
		var ejs = new ElasticSearchClient({
			host : 'localhost',
			port : 9200
		}).ejs;

		var clusterHealth = ejs.ClusterHealth();
		clusterHealth.doHealth(function(result) {
			console.log(JSON.stringify(result, undefined, 2));
			done();
		}, function(error) {
			done(error);
		});
	});

	it('throws an Error when constructed with invalid settings', function(done) {
		try {
			new ElasticSearchClient({
				host : 'localhost'
			});
			done(new Error('Expected validation error'));
		} catch (err) {
			done();
		}
	});

});