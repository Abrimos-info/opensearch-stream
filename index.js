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
let batchesExpected = 0;
let batchesProcessed = 0;

// Try to create the index or see if it exists
createElasticIndex(esClient, args.index, args.mappingsFile)
.catch((e) => {
    console.log('Index ' + args.index + ' already exists.');
})
.then(() => {
    if(!args.noData) {
        getStdIn()
        .resume()
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
    let errorSummary = {};

    stream.write = async function (data) {
        let realData = JSON.parse(data);
        if(!realData || realData.length == 0) return;

        const operations = realData.flatMap(doc => [{ create: { _id: hash(doc, { excludeKeys: function(key) { return args.excludeKeys.includes(key) } }) } }, doc])

        try {
            const bulkResponse = await esClient.bulk({ index: args.index, body: operations });
            batchesProcessed++;
            
            if (bulkResponse.body?.errors) {
                bulkResponse.body.items.forEach((action, i) => {
                    const operation = Object.keys(action)[0]
                    if (action[operation].error) {
                        switch(action[operation].status) {
                            case 409:
                                // Document version conflict: doc has same ID as already inserted doc
                                break;
                            default:
                                erroredDocuments.push({
                                    // If the status is 429 it means that you can retry the document,
                                    // otherwise it's very likely a mapping error, and you should
                                    // fix the document before to try it again.
                                    status: action[operation].status,
                                    error: action[operation].error?.reason || action[operation].error
                                });
                                if(errorSummary[action[operation].status]) errorSummary[action[operation].status]++;
                                else errorSummary[action[operation].status] = 1;
                                break;
                        }
                    }
                });
                
                if(erroredDocuments.length > 0) {
                    process.stdout.write('Batch ' + batchesProcessed + ' (' + realData.length + ' docs):\n');
                    process.stdout.write( JSON.stringify(erroredDocuments, null, 4) );
                    process.stdout.write( JSON.stringify(errorSummary, null, 4))
                    process.stdout.write('\n**************************************************\n');
                    erroredDocuments = [];
                    errorSummary = {};
                }
            }

            if (batchesProcessed === batchesExpected) {
                console.log('All batches processed! Total:', batchesProcessed);
            }

        }
        catch(e) { console.log('Error during bulk:', e) }
    }

    stream.end = function () {
        console.log('stream end');
        // stream.emit('data', JSON.stringify(erroredDocuments, null, 4))
        stream.emit('data', 'Total batches: ' + batchesProcessed);
        // stream.emit('data', JSON.stringify(errorSummary, null, 4))
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
            batchesExpected++;
            stream.emit('data', JSON.stringify(buffer));
            buffer = [];
        }
    }
    stream.end = function () {
        if(buffer.length > 0) batchesExpected++;
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
