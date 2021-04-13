const debug = require('debug')('mongodb-compass:modules:indexes');

import { handleError } from 'modules/error';
import { localAppRegistryEmit } from 'mongodb-redux-common/app-registry';
import fetchIndexes from './fetch-indexes';

/**
 * The module action prefix.
 */
const PREFIX = 'indexes';

/**
 * The loadIndexes action type.
 */
export const LOAD_INDEXES = `${PREFIX}/indexes/LOAD_INDEXES`;

/**
 * The sortIndexes action type.
 */
export const SORT_INDEXES = `${PREFIX}/indexes/SORT_INDEXES`;

/**
 * Default sortOrder
 */
export const DEFAULT = 'Name and Definition';
export const ASC = 'fa-sort-asc';
export const DESC = 'fa-sort-desc';
export const USAGE = 'Usage';

/**
 * The initial state.
 */
export const INITIAL_STATE = [];

/**
 * Get the comparator for properties.
 *
 * @param {Integer} order - The order.
 *
 * @returns {Function} The comparator function.
 */
const _propertiesComparator = (order) => {
  return function(a, b) {
    const aValue = (a.cardinality === 'compound') ? 'compound' : (a.properties[0] || '');
    const bValue = (b.cardinality === 'compound') ? 'compound' : (b.properties[0] || '');
    if (aValue > bValue) {
      return order;
    }
    if (aValue < bValue) {
      return -order;
    }
    return 0;
  };
};

/**
 * Get a comparator function for the sort.
 *
 * @param {String} field - The field to sort on.
 * @param {String} odr - The order.
 *
 * @returns {Function} The function.
 */
const _comparator = (field, odr) => {
  const order = (odr === ASC) ? 1 : -1;
  if (field === 'properties') {
    return _propertiesComparator(order);
  }
  return function(a, b) {
    if (a[field] > b[field]) {
      return order;
    }
    if (a[field] < b[field]) {
      return -order;
    }
    return 0;
  };
};

/**
 * Get the name of the field to sort on based on the column header.
 *
 * @param {String} f - The field.
 * @returns {String} The field.
 */
const _field = (f) => {
  if (f === DEFAULT) {
    return 'name';
  } else if (f === USAGE) {
    return 'usageCount';
  }
  return f.toLowerCase();
};

/**
 * Data Service attaches string message property for some errors, but not all
 * that can happen during index creation/dropping. Check first for data service
 * custom error, then node driver errmsg, lastly use default error message.
 *
 * @param {Object} err - The error to parse a message from
 *
 * @returns {string} - The found error message, or the default message.
 */
export const parseErrorMsg = (err) => {
  if (typeof err.message === 'string') {
    return err.message;
  } else if (typeof err.errmsg === 'string') {
    return err.errmsg;
  }
  return 'Unknown error';
};


/**
 * Reducer function for handle state changes to indexes.
 *
 * @param {Array} state - The indexes state.
 * @param {Object} action - The action.
 *
 * @returns {Array} The new state.
 */
export default function reducer(state = INITIAL_STATE, action) {
  if (action.type === SORT_INDEXES) {
    return [...action.indexes].sort(_comparator(_field(action.column), action.order));
  } else if (action.type === LOAD_INDEXES) {
    return action.indexes;
  }
  return state;
}

/**
 * Action creator for load indexes events.
 *
 * @param {Array} indexes - The raw indexes list.
 *
 * @returns {Object} The load indexes action.
 */
export const loadIndexes = (indexes) => ({
  type: LOAD_INDEXES,
  indexes: indexes
});

/**
 * Action creator for sort indexes events.
 *
 * @param {Array} indexes - The raw indexes list.
 * @param {String} column - The column.
 * @param {String} order - The order.
 *
 * @returns {Object} The load indexes action.
 */
export const sortIndexes = (indexes, column, order) => ({
  type: SORT_INDEXES,
  indexes: indexes,
  column: column,
  order: order
});

/**
 * Load indexes from DB.
 *
 * @param {String} ns - The namespace.
 *
 * @returns {Function} The thunk function.
 */
export const loadIndexesFromDb = () => {
  return (dispatch, getState) => {
    const state = getState();
    if (state.isReadonly) {
      dispatch(loadIndexes([]));
      dispatch(localAppRegistryEmit('indexes-changed', []));
    } else if (state.dataService && state.dataService.isConnected()) {
      fetchIndexes(state.dataService, state.namespace)
        .then(indexes => {
          const sortedIndexes = indexes.sort(
            _comparator(
              _field(state.sortColumn),
              state.sortOrder
            )
          );
          dispatch(loadIndexes(sortedIndexes));
          dispatch(localAppRegistryEmit('indexes-changed', sortedIndexes));
        })
        .catch(err => {
          debug('error loading indexes', err);
          dispatch(handleError(parseErrorMsg(err)));
          dispatch(loadIndexes([]));
          dispatch(localAppRegistryEmit('indexes-changed', []));
        });
    } else if (state.dataService && !state.dataService.isConnected()) {
      debug(
        'warning: trying to load indexes but dataService is disconnected',
        state.dataService
      );
    }
  };
};
