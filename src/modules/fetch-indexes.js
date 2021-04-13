const mongodbNS = require('mongodb-ns');
const isNotAuthorizedError = require('mongodb-js-errors').isNotAuthorized;
const debug = require('debug')('compass-indexes:fetch');

/**
 * Load indexes for a namespace
 *
 * @param {DataService} dataService - a connected DataService instance
 * @param {string} namespace - the namespace to load indexes from
 */
export default async function fetchIndexes(dataService, namespace) {
  const indexes = await dataServiceGetIndexes(dataService, namespace);
  const largestIndexSize = getLargestIndexSize(indexes);
  const viewModelIndexes = indexes.map((index) => toViewModel(
    index,
    namespace,
    largestIndexSize
  ));
  return viewModelIndexes;
}

async function dataServiceGetIndexes(dataService, namespace) {
  const client = dataService.client.client;
  const ns = mongodbNS(namespace);
  const collection = client
    .db(ns.database)
    .collection(ns.collection);

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

  const [indexes, collectionStats, indexStats] = await Promise.all([
    collection.indexes(),
    collection.stats()
      .catch(ignoreError(isNotAuthorizedError, {})),
    collection.aggregate(pipeline, { cursor: {} }).toArray()
      .catch(ignoreError(isNotAuthorizedError, {}))
      .catch(ignoreError(isUnrecognizedStageError, {}))
  ]);

  return indexes.map((index) => {
    return {
      size: collectionStats.indexSizes[index.name],
      ...indexStats.find(({name}) => name === index.name),
      ...index,
    };
  });
}

function isUnrecognizedStageError(err) {
  return err.message.match(/Unrecognized pipeline stage name/);
}

function ignoreError(shouldIgnore, fallbackValue) {
  return err => {
    if (shouldIgnore(err)) {
      debug('Ignoring error and returning fallback', err);
      return fallbackValue;
    }

    return Promise.reject(err);
  };
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
