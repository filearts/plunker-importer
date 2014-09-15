var GitBlob = require("git-object-blob");
var GitCommit = require("git-object-commit");
var GitTree = require("git-object-tree");
var Hashify = require("git-object-hash");
var Path = require("path");
var _ = require("lodash");


var internals = {};

var MODES = exports.modes = {
  BLOB:       0100644,
  COMMIT:     0160000,
  TREE:        040000,
};

/**
 * An entry representing a git Tree
 **/
function CommitEntry (tree, message, userId, parents, created_at) {
  if (!tree.sha) throw new Error("Commit cannot be initialized with an un-frozen tree");
  
  this.tree = tree;
  this.message = message;
  this.userId = userId ? userId.toString() : "";
  this.parents = parents;
  this.created_at = created_at || new Date();
  this.person = internals.formatPerson({
    name: this.userId || "Anonymous",
    email: (this.userId || "Anonymous") + "@plnkr.co",
    date: this.created_at,
  });
  
  var object = GitCommit.create(this.person, this.tree.sha, this.message, this.parents);
  var buffer = object.serialize();
  var header = new Buffer(this.looseType + " " +  buffer.length + "\0");
      
  this.buffer = Buffer.concat([header, buffer]);
  this.sha = Hashify(object);
}

CommitEntry.prototype.looseType = "commit";
CommitEntry.prototype.mode = MODES.COMMIT;

CommitEntry.prototype.get = function (path) {
  return this.tree.get(path);
};

CommitEntry.prototype.getObjects = function () {
  if (!this.sha) throw new Error("Entry must be frozen to get Objects");
  
  var objects = {};
  
  objects[this.sha] = this.buffer;
  
  return _.extend(objects, this.tree.getObjects());
};


/**
 * An entry representing a git Tree
 **/
function DirectoryEntry () {
  this.children = {};
  this.buffer = new Buffer(0);
  this.sha = "";
}

DirectoryEntry.prototype.looseType = "tree";
DirectoryEntry.prototype.mode = MODES.TREE;

DirectoryEntry.prototype.get = function (path) {
  path = _.isArray(path) ? path : [path];
  
  var name = path[0];
  var child = this.children[name];
  
  if (child) {
    if (path.length > 1) {
      if (!_.isFunction(child.get)) throw new Error("Invalid path: " + path.join("/"));
      
      return child.get(path.slice(1));
    } else {
      return child;
    }
  }
};

DirectoryEntry.prototype.getObjects = function () {
  if (!this.sha) throw new Error("Entry must be frozen to get Objects");
  
  var objects = {};
  
  objects[this.sha] = this.buffer;
  
  return _.reduce(this.children, function (objects, child) {
    return _.extend(objects, child.getObjects());
  }, objects);
};


DirectoryEntry.prototype.set = function (name, entry) {
  if (this.sha) throw new Error("Cannot add a child to a frozen DirectoryEntry");
  
  this.children[name] = entry;
};

DirectoryEntry.prototype.freeze = function () {
  var children = _(this.children)
    .invoke("freeze")
    .map(function (entry, name) {
      return {
        hash: entry.sha,
        mode: entry.mode,
        name: name,
      };
    })
    .value();
    
  var object = GitTree.create(children);
  var buffer = object.serialize();
  var header = new Buffer(this.looseType + " " +  buffer.length + "\0");
      
  this.buffer = Buffer.concat([header, buffer]);
  this.sha = Hashify(object);
  
  return this;
};



/**
 * An entry representing a git Blob
 **/
function FileEntry (content, encoding) {
  this.content = content || "";
  this.encoding = encoding || "utf8";
  this.buffer = new Buffer(0);
  this.sha = "";
}

FileEntry.prototype.looseType = "blob";
FileEntry.prototype.mode = MODES.BLOB;

FileEntry.prototype.freeze = function () {
  var object = GitBlob.create(this.content ? new Buffer(this.content, this.encoding) : new Buffer(0));
  var buffer = object.serialize();
  var header = new Buffer(this.looseType + " " +  buffer.length + "\0");
      
  this.buffer = Buffer.concat([header, buffer]);
  this.sha = Hashify(object);
  
  return this;
};

FileEntry.prototype.getObjects = function () {
  if (!this.sha) throw new Error("Entry must be frozen to get Objects");
  
  var objects = {};
  
  objects[this.sha] = this.buffer;
  
  return objects;
};



exports.createCommit = function (tree, message, userId, parents, created_at) {
  return new CommitEntry(tree, message, userId, parents, created_at);
};


/**
 * Create a DirectoryEntry tree from an array of files
 * 
 * @param Files Array of { path, content } objects
 * @returns DirectoryEntry
 **/
exports.fromFiles = function (files) {
  var root = new DirectoryEntry();
  
  _.forEach(files, function (file) {
    var dirname = Path.dirname(file.path);
    var filename = Path.basename(file.path);
    var parent =_(dirname.split("/"))
      .filter(Boolean)
      .reduce(function (parent, name) {
        if (!parent || !parent.set) return null;
        
        var dir = parent.get(name);
        
        if (!dir) {
          dir = new DirectoryEntry();
          parent.set(name, dir);
        }
        
        return dir;
      }, root);
      
    if (!parent || !parent.set || !parent instanceof DirectoryEntry) {
      console.error("[ERR] Invalid file structure, skipping", filename);
      
      return;
    }
    
    parent.set(filename, new FileEntry(file.content, file.encoding));
  });
  
  root.freeze();

  return root;
};


/**
 * Walk the tree, depth-first, applying a visitor to each node
 **/
exports.walk = function walk (entry, visitor) {
  switch (entry.mode) {
    case MODES.COMMIT:
      walk(entry.tree, visitor);
      break;
    case MODES.TREE:
      _.forEach(entry.children, function (entry, name) {
        if (entry.mode === MODES.BLOB) visitor(entry, name);
        else walk(entry, visitor);
      });
      break;
    default:
      throw new Error("Unsupported entry for Git.walk");
  }
};









/**
 * Format a date object to the representation expected by git
 * 
 * Author: Tim Caswell
 * Source: https://github.com/creationix/js-git/blob/4c172fbb1154c7aee3bf3e3edab0188c24a6bf68/lib/object-codec.js
 */
internals.formatDate = function (date) {
  var seconds, offset;
  if (date.seconds) {
    seconds = date.seconds;
    offset = date.offset;
  }
  // Also accept Date instances
  else {
    seconds = Math.floor(date.getTime() / 1000);
    offset = date.getTimezoneOffset();
  }
  var neg = "+";
  if (offset <= 0) offset = -offset;
  else neg = "-";
  offset = neg + internals.two(Math.floor(offset / 60)) + internals.two(offset % 60);
  return seconds + " " + offset;
};

/**
 * Format a person object to the representation expected by git
 * 
 * Author: Tim Caswell
 * Source: https://github.com/creationix/js-git/blob/4c172fbb1154c7aee3bf3e3edab0188c24a6bf68/lib/object-codec.js
 */
internals.formatPerson = function (person) {
  return internals.safe(person.name) +
    " <" + internals.safe(person.email) + "> " +
    internals.formatDate(person.date);
};


/**
 * Strip unsafe characters
 * 
 * Author: Tim Caswell
 * Source: https://github.com/creationix/js-git/blob/4c172fbb1154c7aee3bf3e3edab0188c24a6bf68/lib/object-codec.js
 */
internals.safe = function (string) {
  return string.replace(/(?:^[\.,:;<>"']+|[\0\n<>]+|[\.,:;<>"']+$)/gm, "");
};



/**
 * Zero pad a two-digit number
 * 
 * Author: Tim Caswell
 * Source: https://github.com/creationix/js-git/blob/4c172fbb1154c7aee3bf3e3edab0188c24a6bf68/lib/object-codec.js
 */
internals.two = function (num) {
  return (num < 10 ? "0" : "") + num;
};