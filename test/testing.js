'use strict';

/**
 * Utilities for unit testing with tape
 * @author Byron du Preez
 */
module.exports = {
  okNotOk: okNotOk,
  checkOkNotOk: checkOkNotOk,
  checkMethodOkNotOk: checkMethodOkNotOk,
  equal: equal,
  checkEqual: checkEqual,
  checkMethodEqual: checkMethodEqual,
  immutable: immutable
};

const strings = require('core-functions/strings');
const stringify = strings.stringify;

const Numbers = require('core-functions/numbers');

const prefixFnArgs = require('./config.json').prefixFnArgs;

function toPrefix(prefix) {
  return prefix ? prefix.endsWith(' ') ? prefix : prefix + ' ' : '';
}

function toFnArgsPrefix(fn, args) {
  return prefixFnArgs ? `${fn.name ? fn.name : '' }${stringify(args)} -> ` : '';
}

function toMethodArgsPrefix(obj, method, args) {
  return prefixFnArgs ? `${obj.name ? obj.name : stringify(obj)}.${method.name ? method.name : '<anon>' }${stringify(args)} -> ` : '';
}

function equal(t, actual, expected, prefix) {
  const msg = `${toPrefix(prefix)}${stringify(actual)} must be ${stringify(expected)}`;

  if (Numbers.isNaN(actual)) {
    if (Numbers.isNaN(expected)) {
      t.pass(msg); //`${prefix} must be NaN`);
    } else {
      t.fail(msg); //`${prefix} must NOT be NaN`)
    }
  } else if (actual instanceof Object) {
    t.deepEqual(actual, expected, msg);
  } else {
    t.equal(actual, expected, msg);
  }
}

function checkEqual(t, fn, args, expected, prefix) {
  // Apply the function
  const actual = fn.apply(null, args);
  // check if actual equals expected
  equal(t, actual, expected, `${toFnArgsPrefix(fn, args)}${toPrefix(prefix)}`);
}

function checkMethodEqual(t, obj, method, args, expected, prefix) {
  // Apply the method
  const actual = method.apply(obj, args);
  // check if actual equals expected
  equal(t, actual, expected, `${toMethodArgsPrefix(obj, method, args)}${toPrefix(prefix)}`);
}

function okNotOk(t, actual, expected, okSuffix, notOkSuffix, prefix) {
  if (expected) {
    t.ok(actual, `${toPrefix(prefix)}${stringify(actual)} ${okSuffix}`);
  } else {
    t.notOk(actual, `${toPrefix(prefix)}${stringify(actual)} ${notOkSuffix}`);
  }
}

function checkOkNotOk(t, fn, args, expected, okSuffix, notOkSuffix, prefix) {
  // Apply the function
  const actual = fn.apply(null, args);
  // check if actual equals expected
  okNotOk(t, actual, expected, okSuffix, notOkSuffix, `${toFnArgsPrefix(fn, args)}${toPrefix(prefix)}`);
}

function checkMethodOkNotOk(t, obj, method, args, expected, okSuffix, notOkSuffix, prefix) {
  // Apply the method
  const actual = method.apply(obj, args);
  // check if actual equals expected
  okNotOk(t, actual, expected, okSuffix, notOkSuffix, `${toMethodArgsPrefix(obj, method, args)}${toPrefix(prefix)}`);
}

function immutable(t, obj, propertyName, prefix) {
  const now = new Date().toISOString();
  try {
    obj[propertyName] = now;
    t.fail(`${prefix ? prefix : ''}${stringify(obj)} ${propertyName} is supposed to be immutable`);
  } catch (err) {
    // Expect an error on attempted mutation of immutable property
    t.pass(`${prefix ? prefix : ''}${stringify(obj)} ${propertyName} is immutable`);
    //console.log(`Expected error ${err}`);
  }
}
