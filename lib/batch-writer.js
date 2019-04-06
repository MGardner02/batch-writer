'use strict'
const Writable = require('stream').Writable

class batchWriteStream extends Writable {
  constructor (batchSize, parallelOperations, writeOp, options = {}) {
    super(options)
    this.batchSize = batchSize
    this.parallelOperations = parallelOperations
    this.writeOp = writeOp
    this.queue = []
    this.operations = {}
    this.batchCount = 0
  }

  async triggerOperation (data, index) {
    return this.writeOp(data).then(() => Promise.resolve(index)).catch(err => this.emit('error', err))
  }

  async batchData (chunk) {
    this.queue.push(chunk)
    if (this.queue.length === this.batchSize) {
      var tmp = this.queue
      this.queue = []
      this.operations[this.batchCount] = this.triggerOperation(tmp, this.batchCount)
      this.batchCount++
      if (Object.values(this.operations).length === this.parallelOperations) {
        // return Promise.resolve(false)
        var idx = await Promise.race(Object.values(this.operations))
        delete this.operations[idx]
      }
    }
  }

  async _write (chunk, encoding, callback) {
    await this.batchData(chunk)
    callback()
  }

  /* _writev(chunks, callback) {

    } */

  async _final (callback) {
    if (this.queue.length > 0) { this.operations[this.batchCount] = this.triggerOperation(this.queue, this.batchCount) }
    await Promise.all(Object.values(this.operations))
    callback()
  }
}

module.exports = batchWriteStream
