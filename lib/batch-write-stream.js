'use strict'
const Writable = require('stream').Writable

class batchWriteStream extends Writable {
    constructor(transformOptions) {
        super(transformOptions)
    }

    
}

module.exports = batchWriteStream
