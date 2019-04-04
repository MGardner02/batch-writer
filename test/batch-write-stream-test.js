'require strict'
const BatchWriter = require('../index').batchWriter
const stream = require('stream')
var assert = require('chai').assert;

const mockReadStream = (array) => {
    var index = 0;
    return new stream.Readable({
        objectMode: true,
        read: function (size) {
            if (index < array.length) {
                return this.push(array[index++])
            } else {
                return this.push(null);
            }
        }
    });
};

describe('Array', function () {
    describe('Base Stream', function () {
        it('should parse basic read stream', function (done) {
            var contents = [
                'HEADER',
                'LINE001',
                'LINE002',
                'FOOTER'
            ];

            contents = ['HEADER']
            for (var i = 0; i < 1000; i++) {
                var s = 'LINE'
                if (i < 10)
                    s += '0'
                if (i < 100)
                    s += '0'
                s += i
                s += '\n'
                contents.push(s)
            }
            contents.push('FOOTER')

            var options = {
                fields: [{
                    label: 'Text Column',
                    width: 4,
                    value: 'text'
                }, {
                    label: 'Line Number',
                    width: 3,
                    scale: 0,
                    value: 'record'
                }],
                header: [{
                    label: 'Header',
                    width: 6,
                    value: 'header'
                }],
                footer: [{
                    label: 'Footer',
                    width: 6,
                    value: 'footer'
                }],
                headerRequired: true,
                footerRequired: false
            }

            var input = mockReadStream(contents)

            var counter = 0
            var writeOp = (buffer) => {
                return new Promise((resolve, reject) => {
                    setTimeout(() => {
                        var list = buffer.toString().split('\n')
                        console.log(counter++)
                        //for (var data of list)
                        //console.log(buffer.toString())
                        resolve(true);
                    }, Math.floor(Math.random() * 100))
                })
            }

            var batchWriter = new BatchWriter(10, 200, writeOp)
            input.pipe(batchWriter).on('finish', () => {
                done()
            })
            /*.pipe(es.map((data, cb) => {
                cb(null, data.record)
            })).pipe(es.map((data, cb) => {
                console.log(data)
                cb()
            }))*/
        })

    })
})
