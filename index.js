var Bloom = require("bloomfilter");
var Diff = require("googlediff");
var Elasticsearch = require("elasticsearch");
var Fs = require("fs");
var Git = require("./lib/git");
var Humanize = require("humanize-plus");
var Level = require("level-hyper");
var Log = require("single-line-log");
var Memwatch = require("memwatch");
var Mongo = require("mongo-gyro");
var Pad = require("padded-semver");
var Promise = require("bluebird");
var Pull = require("pull-stream");
var Semver = require("semver");
var _ = require("lodash");


var internals = {};

var mongo = new Mongo(process.env.MONGO_URL);
var es = new Elasticsearch.Client({
  host: "http://localhost:9200",
});

var level = Level("./objects", {
  valueEncoding: 'utf8',
});

Promise.promisifyAll(level);

internals.pullMongo = Pull.Source(function (cursor) {
  return function (end, cb) {
    if (end) {
      cursor.close();
      return cb(end);
    }
    
    cursor.nextObject(function (err, doc) {
      if (!doc) {
        cursor.close();
        return cb(end || true);
      }
      
      if (err) {
        cursor.close();
        
        return cb(err);
      }
      
      cb(err, doc);
    });
  };
});

internals.parsePlunkVersions = function (oldPlunk) {
  var dmp = new Diff();
  var files = {};
  var lastCommit = null;
  var version = 0;
  
  _.forEach(oldPlunk.files, function (file) {
    files[file.filename] = file;
  });
  
  dmp.Match_Threshold = 0;
  dmp.Match_Distance = 0;
  dmp.Patch_DeleteThreshold = 0;
  
  if (!oldPlunk.history || !oldPlunk.history.length) {
    var tree = Git.fromFiles(toFilesArray());
    var commit = Git.createCommit(tree, "Initial commit", oldPlunk.user, []);
    
    saveCommit(commit);
  } else {
    _.forEachRight(oldPlunk.history, parseRevision);
  }
  
  return {
    commit: lastCommit,
    version: version,
  };
  
  function saveCommit (commit) {
    var batch = level.batch();
    
    _.forEach(commit.getObjects(), function (obj, sha) {
      if (!internals.seen.test(sha)) {
        batch.put(sha, obj);
        
        internals.seen.add(sha);
      }
    });
    
    batch.write();
    
    lastCommit = commit;
    version++;
  }
  
  function parseRevision (revision) {
      var message = version === 0 ? "Initial commit" : "Revision " + version;
      var tree = Git.fromFiles(toFilesArray());
      
      if (!lastCommit || tree.sha !== lastCommit.tree.sha) {
        
        var commit = Git.createCommit(tree, message, oldPlunk.user, lastCommit ? [lastCommit.sha] : []);
        
        saveCommit(commit);
      }
      
      // Initial revision has no changes
      _.forEach(revision.changes, function (chg) {
        if (chg.pn) {
          if (chg.fn) {
            if (chg.pl) {
              patch(chg.fn, dmp.patch_fromText(chg.pl));
            }
            if (chg.pn !== chg.fn) {
              rename(chg.fn, chg.pn);
            }
          } else {
            files[chg.pn] = {
              filename: chg.pn,
              content: chg.pl
            };
          }
        } else if (chg.fn) {
          remove(chg.fn);
        }
      });
    }

  function toFilesArray () {
    return _.map(files, function (file) {
      return {
        type: "file",
        path: "/" + file.filename,
        content: file.content,
      };
    });
  }
  
  function rename (fn, to) {
    var file = files[fn];
    if (file) {
      file.filename = to;
      delete files[fn];
      files[to] = file;
    }
  }
  
  function patch (fn, patches) {
    var file = files[fn];
    if (file) {
      file.content = dmp.patch_apply(patches, file.content)[0];
    }
  }
  
  function remove (fn) {
    delete files[fn];
  }

};

internals.findPackages = function (tree) {
  var isHtmlRx = /\.html?$/i;
  var pkgRefRx = /<(?:script|link) [^>]*?data-(semver|require)="([^"]*)"(?: [^>]*?data-(semver|require)="([^"]*)")?/g;
  var refs = {};
  
  Git.walk(tree, function (entry, name) {
    var match;
    
    if (!isHtmlRx.test(name)) return;
    
    while ((match = pkgRefRx.exec(entry.content))) {
      var pkg = {};
      
      pkg[match[1]] = match[2];
      if (match[3]) pkg[match[3]] = match[4];
      
      if (pkg.require) {
        var parts = pkg.require.split("@");
        
        delete pkg.require;
        
        if (!pkg.semver) continue;
        
        pkg.semver = Semver.valid(pkg.semver);
        pkg.name = parts.shift();
        pkg.semverRange = parts.join("@") || "*";
    
        if (pkg.semver) {
          pkg.semver = Pad.pad(pkg.semver);
        
          refs[pkg.name] = pkg;
        }
      }
    }
  });
      
  return _.values(refs);
};


internals.seen = new Bloom.BloomFilter(
  1024 * 1024,
  32
);


internals.parsePlunk = function (oldPlunk) {
  var currentVersion = internals.parsePlunkVersions(oldPlunk);
  var packages = internals.findPackages(currentVersion.commit.tree);

  // Define the plunk
  var plunk = {
    id: oldPlunk._id,
    fork_of: oldPlunk.fork_of || null,
    title: oldPlunk.description,
    readme: "",
    tags: _.unique(oldPlunk.tags),
    created_at: oldPlunk.created_at,
    updated_at: oldPlunk.updated_at,
    viewed_at: oldPlunk.updated_at,
    deleted_at: null,
    user_id: oldPlunk.user || null,
    session_id: null,
    packages: packages,
    commit_sha: currentVersion.commit.sha,
    tree_sha: currentVersion.commit.tree.sha,
    forks_count: (oldPlunk.forks || []).length,
    revisions_count: currentVersion.version,
    comments_count: 0,
    views_count: parseInt(oldPlunk.views || 0, 10),
    likes_count: parseInt(oldPlunk.thumbs || 0, 10),
    favorites_count: 0,
    collections: [],
    queued: [],
  };
  
  Git.walk(currentVersion.commit.tree, function (entry, name) {
    if (name.match(/readme(.md|markdown)?$/)) {
      plunk.readme = entry.content;
      
      return false;
    }
  });
  
  if (!oldPlunk.private) {
    plunk.collections.push("plunker/public");
  }
  
  return plunk;
};

internals.lastBulk = Promise.resolve(0);

internals.savePlunk = function (plunk, cb) {
  internals.lastBulk = internals.lastBulk
    .then(function () {
      cb(null, 1);
      
      return es.index({index: "plunker", type: "plunk", id: plunk.id, body: plunk});
    }, function (err) {
      cb(err);
    });
};

internals.savePlunks = function (plunks, cb) {
  var body = [];
  var updated_at = 0;
  
  _.forEach(plunks, function (plunk) {
    body.push({index: {_index: "plunker", _type: "plunk", _id: plunk.id}});
    body.push(plunk);
    
    updated_at = Math.max(new Date(plunk.updated_at).valueOf(), updated_at);
  });
  
  es.bulk({body: body})
    .then(function () {
      cb(null, { size: plunks.length, updated_at: updated_at });
    }, cb);
};

var progressFile = __dirname + "/progress.txt";
var last_updated_at = Fs.existsSync(progressFile) ? parseInt(Fs.readFileSync(progressFile, "utf8"), 10) : 0;

var query = {
};
var options = {
  skip: 0,
  limit: 100000000,
  sort: {
     updated_at: 1,
  },
};
    
if (last_updated_at) {
  query.updated_at = {
    $gte: new Date(last_updated_at)
  };
}

process.on("exit", function (code) {
  console.log("[WARN] About to exit with code", code);
});

process.on("uncaughtException", function (err) {
  console.log("[ERR] Uncaught exception", err);
  
  process.exit(1);
});


console.log("[OK] Checking index existance");


es.indices.exists({index: "plunker"})
  .then(function (result) {
    if (!result) return es.indices.create({
      index: "plunker",
      type: "plunk",
      body: {
        settings: {
          index: {
            analysis: {
              analyzer: {
                analyzer_keyword: {
                  tokenizer: "keyword",
                  filter: "lowercase",
                }
              }
            }
          }
        },
        mappings: {
          plunk: {
            _id: {
              path: "id",
            },
            properties: {
              'collections': { type: "string", index: "not_analyzed"},
              'queued': { type: "string", index: "not_analyzed"},
              'tags': { type: "string", analyzer: "analyzer_keyword"},
              'packages.name': { type: "string", analyzer: "analyzer_keyword"},
              'packages.semver': { type: "string", analyzer: "analyzer_keyword"},
              'packages.semverRange': { type: "string", analyzer: "analyzer_keyword"},
              'user_id': { type: "string", index: "not_analyzed"},
              'id': { type: "string", index: "not_analyzed"},
              'fork_of': { type: "string", index: "not_analyzed"},
              'session_id': { type: "string", index: "not_analyzed"},
              'commit_sha': { type: "string", index: "not_analyzed"},
              'tree_sha': { type: "string", index: "not_analyzed"},
            },
          },
        }
      },
    });
  })
  .then(function () {
    console.log("[OK] Indices created");
    
    mongo.findCursor("plunks", query, options)
      .then(function (cursor) {
        return cursor.countAsync()
          .then(function (count) {
            var startTime = Date.now();
            var lastPlunk = "";
            var batchSize = 16;
            var lastUpdate = 0;
            
            if (!options.limit) options.limit = Number.MAX_VALUE;
            if (!options.skip) options.skip = 0;
            
            count = Math.min(Math.max(0, count - options.skip), options.limit);
              
            console.log("[OK] Indexing", count, "plunks");
            
            Memwatch.on("leak", function (leak) {
              console.log("[WARN] Leak", leak);
            });
            
            cursor.batchSize(batchSize);
            
            Pull(
              internals.pullMongo(cursor),
              Pull.map(function (plunk) {
                lastPlunk = plunk.id;
                
                return plunk;
              }),
              Pull.map(internals.parsePlunk),
              Pull.group(batchSize),
              Pull.asyncMap(internals.savePlunks),
              Pull.reduce(function (sum, batch) {
                sum += batch.size;
                
                var speed = sum / ((Date.now() - startTime) / 1000);
                
                Log.stdout("Parsed:\t", sum, "/", count, "=", Humanize.formatNumber(100 * sum / count, 2), "\t", Humanize.formatNumber(speed, 2) + "/s", "\t", lastPlunk, "\t", new Date(batch.updated_at).toISOString(), "\n");
                
                lastUpdate = Math.max(lastUpdate, new Date(batch.updated_at).valueOf());
                
                if (sum % (batchSize * 10) === 0) Fs.writeFileSync(progressFile, lastUpdate, "utf8");
                
                return sum;
              }, 0, function (err, parsed) {
                var elapsed = Date.now() - startTime;
                
                if (err) {
                  console.log("[ERR] Error during import", lastPlunk, err.message);
                  console.trace(err);
                  process.exit(1);
                } else {
                  Fs.writeFileSync(progressFile, lastUpdate, "utf8");
                  
                  console.log("[OK] Import completed", parsed, elapsed, parsed/elapsed);
                  
                  setTimeout(function () {
                    process.exit(0);
                  }, 1000 * 60 * 5); // Every 5 minutes
                }
              })
            );
            
          });
      });
  });
