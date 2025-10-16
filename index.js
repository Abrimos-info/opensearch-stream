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
    { name: 'noData', alias: 'n', type: Boolean, defaultValue: false }, // Use to terminate script when no data is passed
    { name: 'verbose', alias: 'v', type: Boolean, defaultValue: false }, // Activate verbose output, as opposed to only errors
    { name: 'upsert', alias: 's', type: Boolean, defaultValue: false }, // Use upsert mode (index operation) instead of create-only mode
    { name: 'idField', alias: 'd', type: String } // Field name to use as document ID for upserts
];
const args = commandLineArgs(optionDefinitions);

if(!args.index) {
    console.error('ERROR: no index specified.');
    process.exit(1);
}

if(args.upsert && !args.idField) {
    console.error('ERROR: upsert mode requires specifying an ID field with --idField or -d.');
    process.exit(1);
}

// Make a new Elasticsearch client
const elasticNode = args.elasticUri;
let esClient = getClient(elasticNode);
let batchesExpected = 0;
let batchesProcessed = 0;
let streamingFinished = false;

// Try to create the index or see if it exists
createElasticIndex(esClient, args.index, args.mappingsFile)
.catch((e) => {
    switch(e.statusCode) {
        case 401:
            console.error('ERROR: Unauthorized.');
            process.exit(401);
        case 400:
            if(args.verbose) console.log(e.body?.error?.reason);
    }
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
    let skippedDocuments = 0;
    let batchSkipped = 0;
    let errorSummary = {};

    stream.write = async function (data) {
        let realData = JSON.parse(data);
        if(!realData || realData.length == 0) return;

        const operations = realData.flatMap(doc => {
            let docId;
            if (args.upsert && args.idField) {
                // Use specified field as ID for upserts, Base64-encoded
                const fieldValue = doc[args.idField];
                if (fieldValue === undefined || fieldValue === null) {
                    throw new Error(`Document missing required ID field '${args.idField}': ${JSON.stringify(doc)}`);
                }
                docId = Buffer.from(String(fieldValue)).toString('base64');
                // Use update operation with doc_as_upsert for partial updates
                return [
                    { update: { _id: docId } },
                    { doc: doc, doc_as_upsert: true }
                ];
            } else {
                // Use hash-based ID for create operations
                docId = hash(doc, { excludeKeys: function(key) { return args.excludeKeys.includes(key) } });
                return [{ create: { _id: docId } }, doc];
            }
        })

        try {
            const bulkResponse = await esClient.bulk({ index: args.index, body: operations });
            batchesProcessed++;
            if(args.verbose) process.stdout.write('Processing batch ' + batchesProcessed + ', ' + realData.length + ' docs\n');
            
            if (bulkResponse.body?.errors) {
                bulkResponse.body.items.forEach((action, i) => {
                    const operation = Object.keys(action)[0]
                    if (action[operation].error) {
                        switch(action[operation].status) {
                            case 409:
                                // Document version conflict: doc has same ID as already inserted doc
                                // This should only happen in create mode, not upsert mode
                                if(!args.upsert) {
                                    if(args.verbose) {
                                        if(!errorSummary['Skipped']) errorSummary['Skipped'] = 1;
                                        else errorSummary['Skipped']++;
                                    }
                                    batchSkipped++;
                                    break;
                                }
                                // In upsert mode, treat 409 as a regular error
                            default:
                                erroredDocuments.push({
                                    // If the status is 429 it means that you can retry the document,
                                    // otherwise it's very likely a mapping error, and you should
                                    // fix the document before to try it again.
                                    status: action[operation].status,
                                    error: action[operation].error?.reason || '(no reason found)',
                                    type: action[operation].error?.type,
                                    docIndex: i
                                });
                                if(errorSummary[action[operation].status]) errorSummary[action[operation].status]++;
                                else errorSummary[action[operation].status] = 1;
                                break;
                        }
                    }
                });
                
                if(erroredDocuments.length > 0) {
                    process.stdout.write('====> BATCH ' + batchesProcessed + ' | ' + realData.length + ' DOCS | ' + batchSkipped + ' SKIPPED | ' + erroredDocuments.length + ' ERRORS\n');
                    process.stdout.write( formatErroredDocs(erroredDocuments) );
                    process.stdout.write( '\n' );
                    erroredDocuments = [];
                }
                batchSkipped = 0;
            }

            if(args.verbose) process.stdout.write('Success!\n\n');

            if (batchesProcessed === batchesExpected && streamingFinished) {
                if(args.verbose) console.log('All batches processed! Total:', batchesProcessed);
                process.stdout.write( formatErrorSummary(errorSummary))
            }

        }
        catch(e) { console.log('Error during bulk:', e) }
    }

    return stream;
}

function formatErroredDocs(docs) {
    let errorStr = '';

    docs.map( doc => {
        errorStr += '* DOC ' + doc.docIndex + '\n';
        errorStr += '-- Status: ' + doc.status + '\n';
        errorStr += '-- Type: ' + doc.type + '\n';
        errorStr += '-- Reason: ' + doc.error + '\n'
    } );

    return errorStr;
}

function formatErrorSummary(summary) {
    let str = '';
    let total = 0;

    Object.keys(summary).map( (key, i) => {
        if(i == 0) str += '====> SUMMARY\n';
        str += 'Status: ' + key + '\t' + 'Count: ' + summary[key] + '\n';
        total += parseInt(summary[key]);
    } );
    if(total > 0) str += 'Total: ' + total + '\n';
    
    return str;
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
        streamingFinished = true;
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
