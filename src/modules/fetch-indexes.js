/**
 * pass in a a driver database handle and get index details back.
 * @param  {Mongo.db}   db     database handle from the node driver
 * @param  {Function} done     callback
 */

const _ = require('lodash');
const async = require('async');
const mongodbNS = require('mongodb-ns');
const isNotAuthorizedError = require('mongodb-js-errors').isNotAuthorized;

const debug = require('debug')('mongodb-index-model:fetch');
const util = require('util');

/**
  * helper function to attach objects to the async.auto task structure.
  * @param  {any}   anything  pass in any variable to attach it to the name
  * @param  {Function} done   callback function.
  */
function attach(anything, done) {
  done(null, anything);
}

/**
  * get basic index information via `db.collection.indexes()`
  * @param  {object}   results    results from async.auto
  * @param  {Function} done       callback
  */
function getIndexes(results, done) {
  const client = results.client;
  const ns = mongodbNS(results.namespace);
  client
    .db(ns.database)
    .collection(ns.collection)
    .indexes(function(err, indexes) {
      if (err) {
        debug('getIndexes failed!', err);
        done(err);
        return;
      }
      // add ns field to each index
      _.each(indexes, function(idx) {
        idx.ns = ns.ns;
      });
      done(null, indexes);
    });
}

/**
  * get index statistics via `db.collection.aggregate({$indexStats: {}})`
  * @param  {object}   results    results from async.auto
  * @param  {Function} done       callback
  */
function getIndexStats(results, done) {
  const client = results.client;
  const ns = mongodbNS(results.namespace);
  const pipeline = [
    { $indexStats: {} },
    {
      $project: {
        name: 1,
        usageHost: '$host',
        usageCount: '$accesses.ops',
        usageSince: '$accesses.since'
      }
    }
  ];
  debug('Getting $indexStats for %s', results.namespace);
  const collection = client.db(ns.database).collection(ns.collection);
  collection.aggregate(pipeline, { cursor: {} }).toArray(function(err, res) {
    if (err) {
      if (isNotAuthorizedError(err)) {
        debug('Not authorized to get index stats', err);
        /**
          * In the 3.2 server, `readWriteAnyDatabase@admin` does not grant sufficient privileges for $indexStats.
          * The `clusterMonitor` role is required to run $indexStats.
          * @see https://jira.mongodb.org/browse/INT-1520
          */
        return done(null, {});
      }

      if (err.message.match(/Unrecognized pipeline stage name/)) {
        debug('$indexStats not yet supported, return empty document', err);
        return done(null, {});
      }
      debug('Unknown error while getting index stats!', err);
      return done(err);
    }
    res = _.mapKeys(res, function(stat) {
      return stat.name;
    });
    done(null, res);
  });
}

/**
  * get index sizes via `db.collection.stats()` (`indexSizes` field)
  * @param  {object}   results    results from async.auto
  * @param  {Function} done       callback
  */

function getIndexSizes(results, done) {
  const client = results.client;
  const ns = mongodbNS(results.namespace);
  debug('Getting index sizes for %s', results.namespace);
  client
    .db(ns.database)
    .collection(ns.collection)
    .stats(function(err, res) {
      if (err) {
        if (isNotAuthorizedError(err)) {
          debug(
            'Not authorized to get collection stats.  Returning default for indexSizes {}.'
          );
          return done(null, {});
        }
        debug('Error getting index sizes for %s', results.namespace, err);
        return done(err);
      }

      res = _.mapValues(res.indexSizes, function(size) {
        return { size: size };
      });
      debug('Got index sizes for %s', results.namespace, res);
      done(null, res);
    });
}

/**
  * merge all information together for each index
  * @param  {object}   results    results from async.auto
  * @param  {Function} done       callback
  */
function combineStatsAndIndexes(results, done) {
  const indexes = results.getIndexes;
  const stats = results.getIndexStats;
  const sizes = results.getIndexSizes;
  _.each(indexes, function(idx, i) {
    _.assign(indexes[i], stats[idx.name]);
    _.assign(indexes[i], sizes[idx.name]);
  });
  done(null, indexes);
}

/**
  * get basic index information via `db.collection.indexes()`
  * @param  {MongoClient} client      handle from mongodb driver
  * @param  {String} namespace    namespace for which to get indexes
  * @param  {Function} done       callback
  */
function getIndexDetails(client, namespace, done) {
  const tasks = {
    client: attach.bind(null, client),
    namespace: attach.bind(null, namespace),
    getIndexes: ['client', 'namespace', getIndexes],
    getIndexStats: ['client', 'namespace', getIndexStats],
    getIndexSizes: ['client', 'namespace', getIndexSizes],
    indexes: [
      'getIndexes',
      'getIndexStats',
      'getIndexSizes',
      combineStatsAndIndexes
    ]
  };
  debug('Getting index details for namespace %s', namespace);
  async.auto(tasks, function(err, results) {
    if (err) {
      debug('Failed to get index details for namespace %s', namespace, err);
      return done(err);
    }
    debug('Index details for namespace %s', namespace, results.indexes);
    // all info was collected in indexes

    return done(null, results.indexes);
  });
}

/**
 * Load indexes for a namespace
 *
 * @param {DataService} dataService - a connected DataService instance
 * @param {string} namespace - the namespace to load indexes from
 */
export default async function fetchIndexes(dataService, namespace) {
  const indexes = await util.promisify(getIndexDetails)(dataService.client.client, namespace);
  const largestIndexSize = getLargestIndexSize(indexes);
  const viewModelIndexes = indexes.map((index) => toViewModel(
    index,
    namespace,
    largestIndexSize
  ));
  return viewModelIndexes;
}

function getLargestIndexSize(indexes) {
  return Math.max(...indexes.map((index) => {
    return index.size;
  }));
}

function toViewModel(index, ns, largestIndexSize) {
  const {
    v: version,
    key,
    name,
    usageCount,
    usageSince,
    usageHost,
    size,
    ...extra
  } = index;

  const fields = Object.entries(key).map(([k, v]) => ({
    field: k,
    value: v
  }));

  const relativeSize = (largestIndexSize && size) ?
    size / largestIndexSize * 100 : 0;

  const cardinality = fields.length === 1 ? 'single' : 'compound';
  return {
    ns,
    version,
    key,
    name,
    fields,
    usageCount,
    usageSince,
    usageHost,
    extra,
    size,
    relativeSize,
    cardinality,
    type: getIndexType(key, extra),
    properties: getIndexProperties(name, extra)
  };
}

function getIndexProperties(name, extra) {
  const indexHasProperty = {
    unique: name === '_id_' || !!extra.unique,
    sparse: !!extra.sparse,
    partial: !!extra.partialFilterExpression,
    ttl: !!extra.expireAfterSeconds,
    collation: !!extra.collation,
  };

  return [
    'unique',
    'sparse',
    'partial',
    'ttl',
    'collation'
  ].filter((propName) => indexHasProperty[propName]);
}

function getIndexType(key, extra) {
  if (isGeoIndex(extra, key)) {
    return 'geospatial';
  }

  if (hasValue(key, 'hashed')) {
    return 'hashed';
  }

  if (!!extra.textIndexVersion) {
    return 'text';
  }

  if (isWildcardIndex(key)) {
    return 'wildcard';
  }

  return 'regular';
}

function isGeoIndex(extra, key) {
  return extra['2dsphereIndexVersion'] ||
    hasValue(key, '2d') ||
    hasValue(key, 'geoHaystack');
}

function isWildcardIndex(key) {
  return Object.keys(key).some(
    k => k === '$**' || k.indexOf('.$**') > -1
  );
}

function hasValue(object, value) {
  return Object.values(object).indexOf(value) > -1;
}
