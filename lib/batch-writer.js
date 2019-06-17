'use strict'
const Writable = require('stream').Writable

class batchWriteStream extends Writable {
  /**
   * Creates a new write stream implementation with the defined batch size, 
   * number of operations, write operations and stream options.
   * 
   * @param {*} batchSize 
   * @param {*} parallelOperations 
   * @param {*} writeOperation 
   * @param {*} streamOptions 
   */
  constructor (batchSize, parallelOperations, writeOperation, streamOptions = {}) {
    super(streamOptions)
    this.batchSize = batchSize
    this.parallelOperations = parallelOperations
    this.writeOp = writeOperation
    this.queue = []
    this.operations = {}
    this.batchCount = 0
  }

  async triggerOperation (data, index) {
    // completes the write operation and resolves the index of the promise
    return this.writeOp(data).then(() => Promise.resolve(index)).catch(err => this.emit('error', err))
  }

  async batchData (chunk) {
    this.queue.push(chunk)
    if (this.queue.length === this.batchSize) {
      // write the next batch
      var tmp = this.queue
      this.queue = []
      // map the write operation to the current count
      this.operations[this.batchCount] = this.triggerOperation(tmp, this.batchCount)
      this.batchCount++
      if (Object.values(this.operations).length === this.parallelOperations) {
        // max parallel write operations reached, wait for one to complete
        var idx = await Promise.race(Object.values(this.operations))
        // remove completed operation
        delete this.operations[idx]
      }
    }
  }

  async _write (chunk, encoding, callback) {
    await this.batchData(chunk)
    callback()
  }

  async _final (callback) {
    // complete any final write operations before closing the stream
    if (this.queue.length > 0) { this.operations[this.batchCount] = this.triggerOperation(this.queue, this.batchCount) }
    await Promise.all(Object.values(this.operations))
    callback()
  }
}

module.exports = batchWriteStream
