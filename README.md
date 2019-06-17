# batch-writer

[![npm (scoped)](https://img.shields.io/npm/v/batch-writer.svg)](https://github.com/MGardner02/batch-writer)
[![npm bundle size (minified)](https://img.shields.io/bundlephobia/min/batch-writer.svg)](https://github.com/MGardner02/batch-writer)
[![Build Status](https://travis-ci.com/MGardner02/batch-write-stream.svg?branch=master)](https://travis-ci.com/MGardner02/batch-writer)
[![codecov](https://codecov.io/gh/MGardner02/batch-write-stream/branch/master/graph/badge.svg)](https://codecov.io/gh/MGardner02/batch-writer)

This module provides a write stream which uses the maximum number of parallel write operations and the selected batch size to keep the stream moving
while preventing node from firing too many writes at a time. When the maximum number of operations have been reached the stream is paused until
one of the operations complete. This write stream implementation can optionally be configured in object mode.

## Instantiation

```javascript 
new BatchWriter(batchSize, parallelOperations, writeOperation, streamOptions)
```

-   `batchSize`: Maximum size of a batch of chunks, or objects, before starting a new write operation.

-   `parallelOperations`: Maximum number of write operations allowed to run in parallel before pausing the stream. (i.e.- number of database connections)

-   `writeOperation`: The write operation for each batch of data. This must be a function with one parameter `data` wrapped in a Promise to signal
    when the write operation has completed or failed.  Errors should be passed through a `Promise.reject(err)` which will then be emitted to the stream.
    On success `resolve()` should be called to allow another operation to begin if available.  Any data returned in the `resolve` will be ignored so no more processing
    can be done after the write operation for that batch of data.

    ```javascript
    async (data) => // The array of data is available in the one (only) parameter
    new Promise(async (resolve, reject) => { 
        // Perform write operation within a promise, resolve() on success and reject(err) to emit the error in stream
    });
    ```

-   `streamOptions` (optional): These are the stream options used to configure the base write stream implementation.
    By default, this will be set to `{}`. A common use of these options would be to set the stream to object mode.

### Example Implementation

```javascript
// Batch 1000 data objects per insert
const BATCH_SIZE = 1000;
// Limit insert operations to 5 at a time
const PARALLEL_OPS = 5;
// Turn on object mode (optional configuration)
const streamOptions = {
    objectMode: true
};

// Create enough connections for the maximum number of simultaneous write operations
const pool = mysql.createPool({
    connectionLimit: PARALLEL_OPS,
    ...
});

// Set up a write operation with one parameter for the array of data
const writeOp = async (data) =>
    new Promise(async (resolve, reject) => {
        const insertQuery = /* Format data into insert statement */;
        pool.getConnection((err, con) => {
            if (err) throw err;
            con.query(insertQuery)
                .on('error', err => {
                    console.error(err);
                    con.release();
                    reject(err);
                })
                .on('result', result => {
                    console.log(result);
                    con.release();
                    resolve();
                });
        });
    });

const dbWriter = new BatchWriter(BATCH_SIZE, PARALLEL_OPS, writeOp, streamOptions);

const readStream = /* Create read stream to obtain data to insert */;

// Pipe the read stream through the write stream to batch up the inserts
// into parallel operations
readStream.pipe(dbWriter).on('finish', () => {
    console.log('Writing Complete!');
});
```
