#!/usr/bin/env node
'use strict';
const commandLineArgs = require('command-line-args');
const { Client } = require("@opensearch-project/opensearch");
const JSONStream = require('JSONStream');
const es = require('event-stream');
const Stream = require('stream').Stream
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
    var stream = new Stream();
    stream.writable = stream.readable = true;
    let erroredDocuments = [];

    stream.write = async function (data) {
        let realData = JSON.parse(data);
        if(!realData || realData.length == 0) return;

        const operations = realData.flatMap(doc => [{ create: { _id: hash(doc, { excludeKeys: function(key) { return args.excludeKeys.includes(key) } }) } }, doc])

        try {
            const bulkResponse = await esClient.bulk({ index: args.index, body: operations });
            // const count = await esClient.count({ index: args.index });
            console.log('batchSize', realData.length, operations.length, batchesProcessed++);
            if (bulkResponse.errors) {
                // The items array has the same order of the dataset we just indexed.
                // The presence of the `error` key indicates that the operation
                // that we did for the document has failed.
                bulkResponse.items.forEach((action, i) => {
                  const operation = Object.keys(action)[0]
                  if (action[operation].error) {
                    erroredDocuments.push({
                      // If the status is 429 it means that you can retry the document,
                      // otherwise it's very likely a mapping error, and you should
                      // fix the document before to try it again.
                      status: action[operation].status,
                      error: action[operation].error,
                      // operation: body[i * 2],
                      // document: body[i * 2 + 1]
                    })
                  }
              });
              console.log( JSON.stringify(erroredDocuments, null, 4) );
            }
        }
        catch(e) { console.log('Error during bulk:', e) }
    }

    stream.end = function () {
        stream.emit('data', JSON.stringify(erroredDocuments, null, 4))
        stream.emit('data', 'Total batches: ' + batchesProcessed);
        erroredDocuments = [];
        stream.emit('close');
    }

    return stream;
}

const collectData = function(size) {
    var stream = new Stream();
    stream.writable = stream.readable = true;
    var buffer = [];

    stream.write = function (l) {
        buffer.push(l);
        if(buffer.length == size) {
            stream.emit('data', JSON.stringify(buffer));
            buffer = [];
        }
    }
    stream.end = function () {
        stream.emit('data', JSON.stringify(buffer));
        buffer = [];
        stream.emit('close');
    }
    return stream;
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
