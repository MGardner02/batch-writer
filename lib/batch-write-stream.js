'use strict'
const Writable = require('stream').Writable

class batchWriteStream extends Writable {
    constructor(batchSize, parallelOperations, writeOp, streamOptions = {}) {
        super(streamOptions)
        this.batchSize = batchSize
        this.parallelOperations = parallelOperations
        this.writeOp = writeOp
        this.queue = []
        this.operations = {}
        this.batchCount = 0
    }

    async triggerOperation(data, index) {
        return this.writeOp.call(null, data).then(() => Promise.resolve(index))
    }

    async batchData(chunk) {
        this.queue.push(chunk)
        if (this.queue.length === this.batchSize) {
            var tmp = this.queue
            this.queue = []
            this.operations[this.batchCount] = this.triggerOperation(tmp, this.batchCount)
            this.batchCount++
            if (Object.values(this.operations).length === this.parallelOperations) {
                var idx = await Promise.race(Object.values(this.operations))
                delete this.operations[idx]
            }

            return Promise.resolve(true)
        } else {
            return Promise.resolve(true)
        }
    }

    async _write(chunk, encoding, callback) {
        var res = await this.batchData(chunk)
        callback()
    }

    /*_writev(chunks, callback) {

    }*/

    async _final(callback) {
        await Promise.all(Object.values(this.operations))
        callback()
    }

}

module.exports = batchWriteStream
