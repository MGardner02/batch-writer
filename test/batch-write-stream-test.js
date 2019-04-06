'require strict'
const BatchWriter = require('../index').batchWriter
const stream = require('stream')
var assert = require('chai').assert

const mockReadStream = (array) => {
  var index = 0
  return new stream.Readable({
    objectMode: true,
    read: function (size) {
      if (index < array.length) {
        return this.push(array[index++])
      } else {
        return this.push(null)
      }
    }
  })
}

describe('Batch Write Stream', function () {
  describe('Basic Write Stream Functionality', function () {
    it('should write a full stream regardless of the order of objects', function (done) {
      var contents = []
      var MAX = 30
      var BATCH_SIZE = 10
      var PARALLEL_OPS = 10
      for (var i = 0; i < MAX; i++) { contents.push(i) }
      var input = mockReadStream(contents)
      var counter = 0
      var batchCount = 0
      var writeOp = async (data) =>
        new Promise((resolve, reject) =>
          setTimeout(() => {
            batchCount++
            counter += data.length
            resolve(data)
          }, (Math.ceil(MAX / 2)) - (batchCount * BATCH_SIZE)))
      var batchWriter = new BatchWriter(BATCH_SIZE, PARALLEL_OPS, writeOp, {
        objectMode: true
      })
      input.pipe(batchWriter).on('finish', () => {
        assert.equal(batchCount, Math.ceil(MAX / BATCH_SIZE))
        assert.equal(counter, MAX)
        done()
      })
    })

    it('should write the full stream including incomplete batch sets at the end', function (done) {
      var contents = []
      var MAX = 61
      var BATCH_SIZE = 20
      var PARALLEL_OPS = 10
      for (var i = 0; i < MAX; i++) { contents.push(i) }
      var input = mockReadStream(contents)
      var counter = 0
      var batchCount = 0
      var writeOp = async (data) =>
        new Promise((resolve, reject) =>
          setTimeout(() => {
            batchCount++
            counter += data.length
            resolve(data)
          }, (Math.ceil(MAX / 2)) - (batchCount * BATCH_SIZE)))

      var batchWriter = new BatchWriter(BATCH_SIZE, PARALLEL_OPS, writeOp, {
        objectMode: true
      })
      input.pipe(batchWriter).on('finish', () => {
        assert.equal(batchCount, Math.ceil(MAX / BATCH_SIZE))
        assert.equal(counter, MAX)
        done()
      })
    })

    it('should throw an error for every batch', function (done) {
      var contents = []
      var MAX = 15
      var BATCH_SIZE = 10
      var PARALLEL_OPS = 1
      for (var i = 0; i < MAX; i++) { contents.push(i) }
      var input = mockReadStream(contents)
      var writeOp = async (data) =>
        new Promise((resolve, reject) => setTimeout(() => reject('ERROR'), 10))
      var batchWriter = new BatchWriter(BATCH_SIZE, PARALLEL_OPS, writeOp, {
        objectMode: true
      })
      var errCnt = 0
      input.pipe(batchWriter)
        .on('error', err => {
          assert.equal(err, 'ERROR')
          errCnt++
        })
        .on('finish', () => {
          assert.equal(errCnt, Math.ceil(MAX / 10))
          done()
        })
    })

    it('should throw an error for only the first batch', function (done) {
      var contents = []
      var MAX = 15
      var BATCH_SIZE = 10
      var PARALLEL_OPS = 1
      for (var i = 0; i < MAX; i++) { contents.push(i) }
      var input = mockReadStream(contents)
      var batchCount = 0
      var writeOp = async (data) =>
        new Promise((resolve, reject) => setTimeout(() => {
          batchCount++
          if (batchCount === 1) { reject('ERROR') } else { resolve(data) }
        }, (Math.ceil(MAX / 2)) - (batchCount * BATCH_SIZE)))
      var batchWriter = new BatchWriter(BATCH_SIZE, PARALLEL_OPS, writeOp, {
        objectMode: true
      })
      var errCnt = 0
      input.pipe(batchWriter)
        .on('error', err => {
          assert.equal(err, 'ERROR')
          errCnt++
        })
        .on('finish', () => {
          assert.equal(errCnt, 1)
          done()
        })
    })

    it('should also parse streams without object mode', function (done) {
      var contents = []
      var MAX = 37
      var BATCH_SIZE = 10
      var PARALLEL_OPS = 10
      for (var i = 0; i < MAX; i++) { contents.push(String(i)) }
      var input = mockReadStream(contents)
      var counter = 0
      var batchCount = 0
      var writeOp = async (data) =>
        new Promise((resolve, reject) =>
          setTimeout(() => {
            batchCount++
            counter += data.length
            resolve(data)
          }, 10))

      var batchWriter = new BatchWriter(BATCH_SIZE, PARALLEL_OPS, writeOp)
      input.pipe(batchWriter).on('finish', () => {
        assert.equal(batchCount, Math.ceil(MAX / BATCH_SIZE))
        assert.equal(counter, MAX)
        done()
      })
    })

    it('should write the full stream a loop through a parallel set of data', function (done) {
      var contents = []
      var MAX = 20
      var BATCH_SIZE = 1
      var PARALLEL_OPS = 11
      for (var i = 0; i < MAX; i++) { contents.push(i) }
      var input = mockReadStream(contents)
      var counter = 0
      var batchCount = 0
      var array = [1, 2, 3, 4, 5]
      var total = 0
      var writeOp = async (data) =>
        new Promise((resolve, reject) =>
          setTimeout(() => {
            total += array[batchCount % array.length]
            batchCount++
            counter += data.length
            resolve(data)
          }, 10))

      var batchWriter = new BatchWriter(BATCH_SIZE, PARALLEL_OPS, writeOp, {
        objectMode: true
      })
      input.pipe(batchWriter).on('finish', () => {
        assert.equal(batchCount, Math.ceil(MAX / BATCH_SIZE))
        assert.equal(counter, MAX)
        assert.equal(total, 60)
        done()
      })
    })
  })
})
