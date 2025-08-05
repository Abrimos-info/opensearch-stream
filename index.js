#!/usr/bin/env node
'use strict';
const commandLineArgs = require('command-line-args');
const { Client } = require("@opensearch-project/opensearch");
const JSONStream = require('JSONStream');
const es = require('event-stream');
const { Stream, Transform } = require('stream')
const fs = require('fs');
const hash = require('object-hash');

const optionDefinitions = [
    { name: 'elasticUri', alias: 'u', type: String, defaultValue: 'http://localhost:9200/' }, // Elasticsearch URI
    { name: 'index', alias: 'i', type: String }, // Elasticsearch Index
    { name: 'batchSize', alias: 'b', type: Number, defaultValue: 500 }, // Size of batch to send to Bulk API
    { name: 'mappingsFile', alias: 'm', type: String }, // Path to file containing index mappings for Elasticsearch
    { name: 'excludeKeys', alias: 'e', type: String, multiple: true, defaultValue: [] }, // Keys to exclude when hashing document
    { name: 'noData', alias: 'n', type: Boolean, defaultValue: false } // Use to terminate script when no data is passed
];
const args = commandLineArgs(optionDefinitions);

if(!args.index) {
    console.error('ERROR: no index specified.');
    process.exit(1);
}

// Make a new Elasticsearch client
const elasticNode = args.elasticUri;
let esClient = getClient(elasticNode);
let batchesProcessed = 0;

// Try to create the index or see if it exists
createElasticIndex(esClient, args.index, args.mappingsFile)
.catch((e) => {
    console.error('Error trying to create index: probably exists already.');
})
.then(() => {
    if(!args.noData) {
        getStdIn()
        .resume()
        // .pipe(es.map(function (data, cb) {
        //     cb(null, newData);
        // }))
        .pipe(collectData(args.batchSize))
        .pipe(sendToElastic())
        .pipe(process.stdout);
    }
})
.catch((e) => {
    console.error('Error during document processing:', e);
    process.exit(2);
});


async function createElasticIndex(client, index, mappingsFile) {
    let mappings = {};
    if(mappingsFile) {
        try{
            mappings = JSON.parse(fs.readFileSync(mappingsFile, 'utf8'));
        }
        catch(e) {
            console.error('Error trying to read mappings file:', e);
            process.exit(2);
        }
    }

    return client.indices.create({
        index: index,
        body: {
            ... mappings
        }
    });
}

const sendToElastic = function() {
    return new Transform({
        objectMode: true,
        async transform(data, encoding, callback) {
            if (!this.erroredDocuments) this.erroredDocuments = [];
            
            if (!data || data.length === 0) {
                callback();
                return;
            }

            const operations = data.flatMap(doc => [{ create: { _id: hash(doc, { excludeKeys: function(key) { return args.excludeKeys.includes(key) } }) } }, doc])

            try {
                const bulkResponse = await esClient.bulk({ index: args.index, body: operations });
                console.log('batchSize', data.length, operations.length, batchesProcessed++);
                if (bulkResponse.errors) {
                    bulkResponse.items.forEach((action, i) => {
                      const operation = Object.keys(action)[0]
                      if (action[operation].error) {
                        this.erroredDocuments.push({
                          status: action[operation].status,
                          error: action[operation].error,
                        })
                      }
                  });
                  console.log( JSON.stringify(this.erroredDocuments, null, 4) );
                }
            }
            catch(e) { 
                console.log('Error during bulk:', e);
            }
            callback();
        },
        flush(callback) {
            this.push(JSON.stringify(this.erroredDocuments, null, 4));
            this.push('Total batches: ' + batchesProcessed);
            callback();
        }
    });
}

const collectData = function(size) {
    return new Transform({
        objectMode: true,
        transform(chunk, encoding, callback) {
            if (!this.buffer) this.buffer = [];
            this.buffer.push(chunk);
            
            if (this.buffer.length === size) {
                this.push(this.buffer); // Pass array directly, no stringify
                this.buffer = [];
            }
            callback();
        },
        flush(callback) {
            if (this.buffer && this.buffer.length > 0) {
                this.push(this.buffer);
            }
            callback();
        }
    });
}

function getStdIn() {
    process.stdin.setEncoding('utf8');

    const dataStream = process.stdin
        .pipe(JSONStream.parse())

    dataStream.on('error', error => {
        process.stdout.write('streaming error: ');
        process.stdout.write(`${error.message}\n`);
    });
    dataStream.pause();
    return dataStream;
};

function getClient(elasticNode) {
    let client = null;
    try {
        client = new Client({ node: elasticNode, requestTimeout: 60000, maxRetries: 10, sniffOnStart: false, ssl: { rejectUnauthorized: false }, resurrectStrategy: "none", compression: "gzip" })
    }
    catch (e) {
        console.error("getClient",e);
    }
    return client;
}
