/**
 * Expose a writeable stream and execute it as a set of bulk requests.
 */
'use strict';

var Writable = require('stream').Writable;

module.exports = WritableBulk;

/**
 * @param bulkExec closure invoked with the bulk cmds as an array and a callback
 * @param highWaterMark number of bulk commands executed at once. 128 by default.
 */
function WritableBulk(bulkExec, highWaterMark, indexName, type) {
  if (!(this instanceof WritableBulk)) {
    return new WritableBulk(bulkExec, highWaterMark);
  }
  Writable.call(this, {objectMode:true});
  this.bulkExec = bulkExec;

  this.indexName = indexName;
  this.type = type;

  this.highWaterMark = highWaterMark/2;

  this.bulk = [];

  // when end is called we still need to flush but we must not overwrite end ourself.
  // now we need to tell everyone to listen to the close event to know when we are done.
  // Not great. See: https://github.com/joyent/node/issues/5315#issuecomment-16670354
  this.on('finish', function() {
    this._flushBulk(function() {
      this.emit('close');
    }.bind(this));
  }.bind(this));
}

WritableBulk.prototype = Object.create(Writable.prototype, {constructor: {value: WritableBulk}});

/**
 * @param chunk a piece of a bulk request as json.
 */
WritableBulk.prototype._write = function(_chunk, enc, next) {

  var chunk = [
    {index: {_index: this.indexName, _type: this.type}},
    _chunk
  ];

  var willExpectPayload = ['index', 'create', 'update'];

  if( chunk.length != 2 ){
    this.emit('error', new Error('Illegal chunk - should contain -> [ operationCmd, content ]'));
    return next();
  }

  this.bulk.push(chunk[0]);
  this.bulk.push(chunk[1]);
  
  if (this.highWaterMark*2 <= this.bulk.length ) {
    this._flushBulk(function(){
      next();
    });
  } else {
    next();
  }
};

// WritableBulk.prototype._writev = function(chunks, next) {
//   console.log("chunks num ", chunks.length);
//   next();
//
// };

WritableBulk.prototype._flushBulk = function(callback) {
  var self = this;
  this.bulkExec(this.bulk, function(e, resp) {
    if (e) {
      self.emit('error', e);
    }
    if (resp && resp.errors && resp.items) {
      for (var i = 0; i < resp.items.length; i++) {
        var bulkItemResp = resp.items[i];
        var key = Object.keys(bulkItemResp)[0];
        if (bulkItemResp[key].error) {
          self.emit('error', new Error(bulkItemResp[key].error));
        }
      }
    }
    callback();
  });
  self.bulk = [];
  return false;
};
