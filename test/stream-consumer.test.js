'use strict';

/**
 * Unit tests for aws-stream-consumer/stream-consumer.js
 * @author Byron du Preez
 */

const test = require("tape");

// The test subject
const streamConsumer = require('../stream-consumer');

const streamProcessing = require('../stream-processing');

const taskStates = require('task-utils/task-states');
const TaskDefs = require('task-utils/task-defs');
const TaskDef = TaskDefs.TaskDef;
const Tasks = require('task-utils/tasks');
const Task = Tasks.Task;
const taskUtils = require('task-utils/task-utils');

const stages = require("aws-core-utils/stages");
const kinesisCache = require("aws-core-utils/kinesis-cache");
const dynamoDBDocClientCache = require("aws-core-utils/dynamodb-doc-client-cache");

require("core-functions/promises");

const strings = require("core-functions/strings");
const stringify = strings.stringify;

const Arrays = require("core-functions/arrays");

const logging = require("logging-utils");

const samples = require("./samples");

function setRegionStageAndDeleteCachedInstances(region, stage) {
  // Set up region
  process.env.AWS_REGION = region;
  // Set up stage
  process.env.STAGE = stage;
  // Remove any cached entries before configuring
  deleteCachedInstances();
  return region;
}

function deleteCachedInstances() {
  const region = process.env.AWS_REGION;
  kinesisCache.deleteKinesis(region);
  dynamoDBDocClientCache.deleteDynamoDBDocClient(region);
}

function sampleKinesisEvent(streamName, partitionKey, data, omitEventSourceARN) {
  const region = process.env.AWS_REGION;
  const eventSourceArn = omitEventSourceARN ? undefined : samples.sampleKinesisEventSourceArn(region, streamName);
  return samples.sampleKinesisEventWithSampleRecord(partitionKey, data, eventSourceArn, region);
}

function sampleAwsContext(functionVersion, functionAlias, maxTimeInMillis) {
  const region = process.env.AWS_REGION;
  const functionName = 'sampleFunctionName';
  const invokedFunctionArn = samples.sampleInvokedFunctionArn(region, functionName, functionAlias);
  return samples.sampleAwsContext(functionName, functionVersion, invokedFunctionArn, maxTimeInMillis);
}

function configureDefaults(t, context, kinesisError) {
  const options = require('../default-kinesis-options.json');
  logging.configureDefaultLogging(context, options.loggingOptions);
  stages.configureDefaultStageHandling(context, options.stageHandlingOptions);
  context.kinesis = dummyKinesis(t, 'Stream consumer', kinesisError);
  //kinesisCache.configureKinesis(context, config.kinesisOptions);
  streamProcessing.configureDefaultKinesisStreamProcessing(context, options.streamProcessingOptions);
}

function dummyKinesis(t, prefix, error) {
  return {
    putRecord(request) {
      return {
        promise() {
          return new Promise((resolve, reject) => {
            t.pass(`${prefix} simulated putRecord to Kinesis with request (${stringify(request)})`);
            if (error)
              reject(error);
            else
              resolve({});
          })
        }
      }
    }
  };
}

let messageNumber = 0;

function sampleMessage(i) {
  ++messageNumber;
  return {
    id: messageNumber,
    msg: `Sample Message ${i}`,
    date: new Date().toISOString(),
    loc: {
      lat: 123.456 + i,
      lon: -67.890 - i
    },
    tags: [`Tag A${i}`, `Tag B${i}`]
  };
}

function execute1() {
  console.log(`Executing execute1 on task (${this.name})`);
}
function execute2() {
  console.log(`Executing execute2 on task (${this.name})`);
}
function execute3() {
  console.log(`Executing execute3 on task (${this.name})`);
}
function execute4() {
  console.log(`Executing execute4 on task (${this.name})`);
}

function sampleExecuteOneAsync(ms, mustRejectWithError, callback) {
  function executeOneAsync(message, context) {
    context.info(`${executeOneAsync.name} started processing message (${stringify(message)})`);
    return Promise.delay(ms)
      .then(
        () => {
          if (typeof callback === 'function') {
            callback(message, context);
          }
          if (!mustRejectWithError) {
            context.info(`${executeOneAsync.name} completed message (${stringify(message)})`);
            return message;
          } else {
            context.error(`${executeOneAsync.name} failed intentionally on message (${stringify(message)}) with error (${mustRejectWithError})`,
              mustRejectWithError);
            throw mustRejectWithError;
          }
        },
        err => {
          context.error(`${executeOneAsync.name} hit UNEXPECTED error`, err.stack);
          if (typeof callback === 'function') {
            callback(message, context);
          }
          if (!mustRejectWithError) {
            context.info(`${executeOneAsync.name} "completed" message (${stringify(message)})`);
            return message;
          } else {
            context.error(`${executeOneAsync.name} "failed" intentionally on message (${stringify(message)}) with error (${mustRejectWithError})`,
              mustRejectWithError.stack);
            throw mustRejectWithError;
          }
        }
      );
  }

  return executeOneAsync;
}

// function sampleExecuteOneSync(mustSucceed) {
//   function executeOneSync(message, context) {
//     if (mustSucceed) {
//       context.info(`${executeOneSync.name} completed message (${stringify(message)})`);
//       return message;
//     } else {
//       const errMsg = `${executeOneSync.name} failed intentionally on message (${stringify(message)})`;
//       const err = new Error(errMsg);
//       context.error(errMsg, err.stack);
//       throw err;
//     }
//   }
//
//   return executeOneSync;
// }

function sampleExecuteAllAsync(ms, mustRejectWithError, callback) {
  function executeAllAsync(messages, context) {
    context.info(`${executeAllAsync.name} started processing messages ${stringify(messages)}`);
    return Promise.delay(ms)
      .then(
        () => {
          if (typeof callback === 'function') {
            callback(message, context);
          }
          if (!mustRejectWithError) {
            context.info(`${executeAllAsync.name} completed messages ${stringify(messages)}`);
            return messages;
          } else {
            context.error(`${executeAllAsync.name} failed intentionally on messages ${stringify(messages)} with error (${mustRejectWithError})`,
              mustRejectWithError.stack);
            throw mustRejectWithError;
          }
        },
        err => {
          context.error(`${executeAllAsync.name} hit UNEXPECTED error`, err.stack);
          if (typeof callback === 'function') {
            callback(message, context);
          }
          if (!mustRejectWithError) {
            context.info(`${executeAllAsync.name} "completed" messages ${stringify(messages)}`);
            return messages;
          } else {
            context.error(`${executeAllAsync.name} "failed" intentionally on messages ${stringify(messages)} with error (${mustRejectWithError})`,
              mustRejectWithError.stack);
            throw mustRejectWithError;
          }
        }
      );
  }

  return executeAllAsync;
}

// function sampleExecuteAllSync(mustSucceed) {
//   function executeAllSync(messages, context) {
//     if (mustSucceed) {
//       context.info(`${executeAllSync.name} completed messages (${stringify(messages)})`);
//       return messages;
//     } else {
//       const errMsg = `${executeAllSync.name} failed intentionally on messages (${stringify(messages)})`;
//       const err = new Error(errMsg);
//       context.error(errMsg, err.stack);
//       throw err;
//     }
//   }
//
//   return executeAllSync;
// }

// =====================================================================================================================
// validateTaskDefinitions
// =====================================================================================================================

test('validateTaskDefinitions', t => {
  function check(processOneTaskDefs, processAllTaskDefs, mustPass) {
    const arg1 = stringify(Arrays.isArrayOfType(processOneTaskDefs, TaskDef) ? processOneTaskDefs.map(d => d.name) : processOneTaskDefs);
    const arg2 = stringify(Arrays.isArrayOfType(processAllTaskDefs, TaskDef) ? processAllTaskDefs.map(d => d.name) : processAllTaskDefs);
    const prefix = `validateTaskDefinitions(${arg1}, ${arg2})`;
    try {
      streamConsumer.validateTaskDefinitions(processOneTaskDefs, processAllTaskDefs, context);
      if (mustPass) {
        t.pass(`${prefix} should have passed`);
      } else {
        t.fail(`${prefix} should NOT have passed`);
      }
    } catch (err) {
      if (mustPass) {
        t.fail(`${prefix} should NOT have failed (${err})`);
      } else {
        t.pass(`${prefix} should have failed`);
      }
    }
  }

  const context = {};
  logging.configureDefaultLogging(context);

  // Create task definitions
  const taskDef1 = TaskDef.defineTask('Task1', execute1);
  const taskDef2 = TaskDef.defineTask('Task2', execute2);
  const taskDef3 = TaskDef.defineTask('Task3', execute3);
  const taskDef4 = TaskDef.defineTask('Task4', execute4);

  const taskDef1Copy = TaskDef.defineTask('Task1', execute1);
  const taskDef2Copy = TaskDef.defineTask('Task2', execute2);
  const taskDef3Copy = TaskDef.defineTask('Task3', execute1);
  const taskDef4Copy = TaskDef.defineTask('Task4', execute4);

  // Not enough task defs
  check(undefined, undefined, false);
  check(undefined, [], false);
  check([], undefined, false);
  check([], [], false);

  // Invalid task definitions (not TaskDef)
  check(['"TaskDef1 string"'], undefined, false);
  check(undefined, [{
    name: 'TaskDef2', execute: () => {
    }
  }], false);

  // Validate that real TaskDef task defintions are valid
  check([taskDef1], undefined, true);
  check(undefined, [taskDef2], true);
  check([taskDef1], [taskDef2], true);
  check([taskDef1, taskDef2], undefined, true);
  check(undefined, [taskDef3, taskDef4], true);
  check([taskDef1, taskDef2], [taskDef3], true);
  check([taskDef1], [taskDef3, taskDef4], true);
  check([taskDef1, taskDef2], [taskDef3, taskDef4], true);

  // Create subtask definitions
  const subTaskDef1 = taskDef1.defineSubTask('SubTask1');
  const subTaskDef2 = taskDef2.defineSubTask('SubTask2');
  const subTaskDef3 = taskDef3.defineSubTask('SubTask3');
  const subTaskDef4 = taskDef4.defineSubTask('SubTask4');

  // Re-validate same set as before (now that the previous TaskDefs have become complex ones (each with a sub-task)
  check([taskDef1], undefined, true);
  check(undefined, [taskDef2], true);
  check([taskDef1], [taskDef2], true);
  check([taskDef1, taskDef2], undefined, true);
  check(undefined, [taskDef3, taskDef4], true);
  check([taskDef1, taskDef2], [taskDef3], true);
  check([taskDef1], [taskDef3, taskDef4], true);
  check([taskDef1, taskDef2], [taskDef3, taskDef4], true);

  // Invalid subtask definitions
  check([subTaskDef1], undefined, false);
  check(undefined, [subTaskDef2], false);
  check([subTaskDef1], [subTaskDef2], false);
  check([subTaskDef1, subTaskDef2], undefined, false);
  check(undefined, [subTaskDef3, subTaskDef4], false);
  check([subTaskDef1, subTaskDef2], [subTaskDef3], false);
  check([subTaskDef1], [subTaskDef3, subTaskDef4], false);
  check([subTaskDef1, subTaskDef2], [subTaskDef3, subTaskDef4], false);

  // Some valid task definitions with some invalid subtask definitions
  check([taskDef1], [subTaskDef2], false);
  check([subTaskDef1], [taskDef2], false);
  check([taskDef1, subTaskDef2], [taskDef3], false);
  check([taskDef1], [taskDef3, subTaskDef4], false);
  check([taskDef1, taskDef2], [taskDef3, subTaskDef4], false);
  check([taskDef1, subTaskDef2], [taskDef3, taskDef4], false);

  // All task definitions' names within each list must be unique
  check([taskDef1, taskDef2, taskDef3, taskDef1], undefined, false);
  check([taskDef1, taskDef2, taskDef3, taskDef2], [], false);
  check([taskDef1, taskDef2, taskDef3, taskDef3], [taskDef4], false);

  check(undefined, [taskDef2, taskDef3, taskDef4, taskDef2], false);
  check([], [taskDef2, taskDef3, taskDef4, taskDef3], false);
  check([taskDef1], [taskDef2, taskDef3, taskDef4, taskDef4], false);

  check([taskDef1, taskDef2, taskDef1, taskDef2], [taskDef3, taskDef4, taskDef3, taskDef4], false);

  // Copies with same names, but different instances
  check([taskDef1, taskDef1Copy], [taskDef3, taskDef3Copy], false); // names non-unique within each list

  // Instances not unique within each list (will fail name uniqueness check)
  check([taskDef1, taskDef1], [taskDef3, taskDef3], false); // names non-unique within each list

  // Invalid, since both lists share task definitions!
  check([taskDef1], [taskDef2, taskDef1], false);
  check([taskDef1, taskDef3], [taskDef3], false); // names non-unique within each list
  check([taskDef1, taskDef4], [taskDef3, taskDef4], false); // names non-unique within each list

  // names unique within each list and instances "unique" too
  check([taskDef1, taskDef3Copy], [taskDef1Copy, taskDef3], true);
  check([taskDef1, taskDef2, taskDef3Copy, taskDef4Copy], [taskDef1Copy, taskDef2Copy, taskDef3, taskDef4], true);

  t.end();
});

// =====================================================================================================================
// processStreamEvent
// =====================================================================================================================

function checkMessagesTasksStates(t, messages, oneStateType, allStateType, context) {
  for (let i = 0; i < messages.length; ++i) {
    checkMessageTasksStates(t, messages[i], oneStateType, allStateType, context)
  }
}

function checkMessageTasksStates(t, message, oneStateType, allStateType, context) {
  if (oneStateType) {
    const oneTasksByName = streamConsumer.getProcessOneTasksByName(message, context);
    const ones = taskUtils.getTasksAndSubTasks(oneTasksByName);
    t.ok(ones.every(t => t.state instanceof oneStateType), `message ${message.id} every process one task state must be instance of ${oneStateType.name}`);
  }
  if (allStateType) {
    const allTasksByName = streamConsumer.getProcessAllTasksByName(message, context);
    const alls = taskUtils.getTasksAndSubTasks(allTasksByName);
    t.ok(alls.every(t => t.state instanceof allStateType), `message ${message.id} every process all task state must be instance of ${allStateType.name}`);
  }
}

// =====================================================================================================================
// processStreamEvent with successful message(s)
// =====================================================================================================================

test('processStreamEvent with 1 message that succeeds all tasks', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};

    const n = 1;

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const event = sampleKinesisEvent(streamName, undefined, sampleMessage(1), false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev1', maxTimeInMillis);

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    configureDefaults(t, context, undefined);

    // Setup the task definitions
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, undefined));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      streamConsumer.configureStreamConsumer(context, undefined, undefined, event, awsContext);
      const promise = streamConsumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promise.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev1', `context.stage must be dev1`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(results => {
          t.pass(`processStreamEvent must resolve`);
          const messages = results.messages;
          t.equal(messages.length, n, `processStreamEvent results must have ${n} messages`);
          checkMessagesTasksStates(t, messages, taskStates.CompletedState, taskStates.CompletedState, context);
          t.equal(messages[0].taskTracking.ones.Task1.attempts, 1, `Task1 attempts must be 1`);
          t.equal(messages[0].taskTracking.alls.Task2.attempts, 1, `Task2 attempts must be 1`);

          t.ok(results.processing.completed, `processStreamEvent processing must be completed`);
          t.notOk(results.processing.failed, `processStreamEvent processing must not be failed`);
          t.notOk(results.processing.timedOut, `processStreamEvent processing must not be timed-out`);

          t.equal(results.unusableRecords.length, 0, `processStreamEvent results must have ${0} unusable records`);
          t.equal(results.discardedUnusableRecords.length, 0, `processStreamEvent results must have ${0} discarded unusable records`);
          t.equal(results.handledIncompleteMessages.length, 0, `processStreamEvent results must have ${0} handled incomplete records`);
          t.equal(results.discardedRejectedMessages.length, 0, `processStreamEvent results must have ${0} discarded rejected messages`);

          t.end();
        })
        .catch(err => {
          t.fail(`processStreamEvent should NOT have failed (${stringify(err)})`, err.stack);
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err.stack);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});


test('processStreamEvent with 1 message that succeeds all tasks (despite broken Kinesis, i.e. no unusable/rejected/incomplete)', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};

    const n = 1;

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const event = sampleKinesisEvent(streamName, undefined, sampleMessage(1), false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev1', maxTimeInMillis);

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    configureDefaults(t, context, new Error('Disabling Kinesis'));

    // Setup the task definitions
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, undefined));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      streamConsumer.configureStreamConsumer(context, undefined, undefined, event, awsContext);
      const promise = streamConsumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promise.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev1', `context.stage must be dev1`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(results => {
          t.pass(`processStreamEvent must resolve`);
          const messages = results.messages;
          t.equal(messages.length, n, `processStreamEvent results must have ${n} messages`);
          checkMessagesTasksStates(t, messages, taskStates.CompletedState, taskStates.CompletedState, context);
          t.equal(messages[0].taskTracking.ones.Task1.attempts, 1, `Task1 attempts must be 1`);
          t.equal(messages[0].taskTracking.alls.Task2.attempts, 1, `Task2 attempts must be 1`);

          t.ok(results.processing.completed, `processStreamEvent processing must be completed`);
          t.notOk(results.processing.failed, `processStreamEvent processing must not be failed`);
          t.notOk(results.processing.timedOut, `processStreamEvent processing must not be timed-out`);

          t.equal(results.unusableRecords.length, 0, `processStreamEvent results must have ${0} unusable records`);
          t.equal(results.discardedUnusableRecords.length, 0, `processStreamEvent results must have ${0} discarded unusable records`);
          t.equal(results.handledIncompleteMessages.length, 0, `processStreamEvent results must have ${0} handled incomplete records`);
          t.equal(results.discardedRejectedMessages.length, 0, `processStreamEvent results must have ${0} discarded rejected messages`);

          t.end();
        })

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err.stack);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('processStreamEvent with 10 messages that succeed all tasks (despite broken Kinesis, i.e. no unusable/rejected/incomplete)', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};

    const n = 10;

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const records = [];
    for (let i = 0; i < n; ++i) {
      const eventSourceArn = samples.sampleKinesisEventSourceArn(region, streamName);
      const record = samples.sampleKinesisRecord(undefined, sampleMessage(i + 1), eventSourceArn, region);
      records.push(record);
    }
    const event = samples.sampleKinesisEventWithRecords(records);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev1', maxTimeInMillis);

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    //configureDefaults(t, context, undefined);
    configureDefaults(t, context, new Error('Disabling Kinesis'));

    // Setup the task definitions
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, undefined));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      streamConsumer.configureStreamConsumer(context, undefined, undefined, event, awsContext);
      const promise = streamConsumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promise.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev1', `context.stage must be dev1`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(results => {
          t.pass(`processStreamEvent must resolve`);
          const messages = results.messages;
          t.equal(messages.length, n, `processStreamEvent results must have ${n} messages`);
          checkMessagesTasksStates(t, messages, taskStates.CompletedState, taskStates.CompletedState, context);
          for (let i = 0; i < messages.length; ++i) {
            t.equal(messages[i].taskTracking.ones.Task1.attempts, 1, `message ${messages[i].id} Task1 attempts must be 1`);
            t.equal(messages[i].taskTracking.alls.Task2.attempts, 1, `message ${messages[i].id} Task2 attempts must be 1`);
          }
          t.ok(results.processing.completed, `processStreamEvent processing must be completed`);
          t.notOk(results.processing.failed, `processStreamEvent processing must not be failed`);
          t.notOk(results.processing.timedOut, `processStreamEvent processing must not be timed-out`);

          t.equal(results.unusableRecords.length, 0, `processStreamEvent results must have ${0} unusable records`);
          t.equal(results.discardedUnusableRecords.length, 0, `processStreamEvent results must have ${0} discarded unusable records`);
          t.equal(results.handledIncompleteMessages.length, 0, `processStreamEvent results must have ${0} handled incomplete records`);
          t.equal(results.discardedRejectedMessages.length, 0, `processStreamEvent results must have ${0} discarded rejected messages`);

          t.end();
        })
        .catch(err => {
          t.fail(`processStreamEvent should NOT have failed (${stringify(err)})`, err.stack);
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err.stack);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

// =====================================================================================================================
// processStreamEvent with unusable record(s)
// =====================================================================================================================

test('processStreamEvent with 1 unusable record', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const event = sampleKinesisEvent(streamName, undefined, undefined, false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev1', maxTimeInMillis);

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    configureDefaults(t, context, undefined);

    // Setup the task definitions
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, undefined));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      streamConsumer.configureStreamConsumer(context, undefined, undefined, event, awsContext);
      const promise = streamConsumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promise.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev1', `context.stage must be dev1`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(results => {
          const n = 0;
          t.pass(`processStreamEvent must resolve`);
          const messages = results.messages;
          t.equal(messages.length, n, `processStreamEvent results must have ${n} messages`);

          t.ok(results.processing.completed, `processStreamEvent processing must be completed`);
          t.notOk(results.processing.failed, `processStreamEvent processing must not be failed`);
          t.notOk(results.processing.timedOut, `processStreamEvent processing must not be timed-out`);

          t.equal(results.unusableRecords.length, 1, `processStreamEvent results must have ${1} unusable records`);
          t.equal(results.discardedUnusableRecords.length, 1, `processStreamEvent results must have ${1} discarded unusable records`);
          t.equal(results.handledIncompleteMessages.length, 0, `processStreamEvent results must have ${0} handled incomplete records`);
          t.equal(results.discardedRejectedMessages.length, 0, `processStreamEvent results must have ${0} discarded rejected messages`);

          t.end();
        })
        .catch(err => {
          t.fail(`processStreamEvent should NOT have failed (${stringify(err)})`, err.stack);
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err.stack);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('processStreamEvent with 1 unusable record, but if cannot discard must fail', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const event = sampleKinesisEvent(streamName, undefined, undefined, false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev1', maxTimeInMillis);

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    const fatalError = new Error('Disabling Kinesis');
    configureDefaults(t, context, fatalError);

    // Setup the task definitions
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, undefined));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      streamConsumer.configureStreamConsumer(context, undefined, undefined, event, awsContext);
      const promise = streamConsumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promise.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev1', `context.stage must be dev1`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(messages => {
          const n = messages.length;
          t.fail(`processStreamEvent must NOT resolve with ${n} message(s)`);
          t.end();
        })
        .catch(err => {
            t.pass(`processStreamEvent must reject with error (${stringify(err)})`);
            t.equal(err, fatalError, `processStreamEvent error must be ${fatalError}`);

            streamConsumer.awaitStreamConsumerResults(err.streamConsumerResults).then(results => {
              const messages = results.messages;
              t.equal(messages.length, 0, `processStreamEvent results must have ${0} messages`);
              t.equal(results.unusableRecords.length, 1, `processStreamEvent results must have ${1} unusable records`);

              t.ok(results.processing.completed, `processStreamEvent processing must be completed`);
              t.notOk(results.processing.failed, `processStreamEvent processing must not be failed`);
              t.notOk(results.processing.timedOut, `processStreamEvent processing must not be timed-out`);

              if (results.discardedUnusableRecords || !results.discardUnusableRecordsError) {
                t.fail(`discardUnusableRecords must fail`);
              }
              t.equal(results.discardUnusableRecordsError, fatalError, `discardUnusableRecords must fail with ${fatalError}`);

              if (!results.handledIncompleteMessages || results.handleIncompleteMessagesError) {
                t.fail(`handleIncompleteMessages must not fail with ${results.handleIncompleteMessagesError}`);
              }
              if (results.handledIncompleteMessages) {
                t.equal(results.handledIncompleteMessages.length, 0, `handleIncompleteMessages must have ${0} handled incomplete records`);
              }

              if (!results.discardedRejectedMessages || results.discardRejectedMessagesError) {
                t.fail(`discardRejectedMessages must not fail with ${results.discardRejectedMessagesError}`);
              }
              if (results.discardedRejectedMessages) {
                t.equal(results.discardedRejectedMessages.length, 0, `discardedRejectedMessages must have ${0} discarded rejected messages`);
              }

              t.end();
            });
          }
        );
    } catch
      (err) {
      t.fail(`processStreamEvent should NOT have failed on try-catch (${err})`, err.stack);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

// =====================================================================================================================
// processStreamEvent with failing processOne message(s)
// =====================================================================================================================

test('processStreamEvent with 1 message that fails its processOne task, resubmits', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const event = sampleKinesisEvent(streamName, undefined, sampleMessage(1), false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev1', maxTimeInMillis);

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    configureDefaults(t, context, undefined);

    // Setup the task definitions
    const processOneError = new Error('Failing process one task');
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, processOneError));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      streamConsumer.configureStreamConsumer(context, undefined, undefined, event, awsContext);
      const promise = streamConsumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promise.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev1', `context.stage must be dev1`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(results => {
          t.pass(`processStreamEvent must resolve`);
          const n = 1;
          const messages = results.messages;
          t.equal(messages.length, n, `processStreamEvent results must have ${n} messages`);
          checkMessagesTasksStates(t, messages, taskStates.Failed, taskStates.CompletedState, context);
          t.equal(messages[0].taskTracking.ones.Task1.attempts, 1, `Task1 attempts must be 1`);
          t.equal(messages[0].taskTracking.alls.Task2.attempts, 1, `Task2 attempts must be 1`);

          t.ok(results.processing.completed, `processStreamEvent processing must be completed`);
          t.notOk(results.processing.failed, `processStreamEvent processing must not be failed`);
          t.notOk(results.processing.timedOut, `processStreamEvent processing must not be timed-out`);

          t.equal(results.unusableRecords.length, 0, `processStreamEvent results must have ${0} unusable records`);
          t.equal(results.discardedUnusableRecords.length, 0, `processStreamEvent results must have ${0} discarded unusable records`);
          t.equal(results.handledIncompleteMessages.length, 1, `processStreamEvent results must have ${1} handled incomplete records`);
          t.equal(results.discardedRejectedMessages.length, 0, `processStreamEvent results must have ${0} discarded rejected messages`);

          t.end();
        })
        .catch(err => {
          t.fail(`processStreamEvent should NOT have failed (${stringify(err)})`, err.stack);
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err.stack);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('processStreamEvent with 1 message that fails its processOne task, but cannot resubmit must fail', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const event = sampleKinesisEvent(streamName, undefined, sampleMessage(1), false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev1', maxTimeInMillis);

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    const fatalError = new Error('Disabling Kinesis');
    configureDefaults(t, context, fatalError);

    // Setup the task definitions
    const processOneError = new Error('Failing process one task');
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, processOneError));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      streamConsumer.configureStreamConsumer(context, undefined, undefined, event, awsContext);
      const promise = streamConsumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promise.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev1', `context.stage must be dev1`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(messages => {
          const n = messages.length;
          t.fail(`processStreamEvent must NOT resolve with ${n} message(s)`);
          t.end();
        })
        .catch(err => {
          t.pass(`processStreamEvent must reject with error (${stringify(err)})`);
          t.equal(err, fatalError, `processStreamEvent error must be ${fatalError}`);

          streamConsumer.awaitStreamConsumerResults(err.streamConsumerResults).then(results => {
            const messages = results.messages;
            t.equal(messages.length, 1, `processStreamEvent results must have ${1} messages`);
            t.equal(results.unusableRecords.length, 0, `processStreamEvent results must have ${0} unusable records`);

            t.ok(results.processing.completed, `processStreamEvent processing must be completed`);
            t.notOk(results.processing.failed, `processStreamEvent processing must not be failed`);
            t.notOk(results.processing.timedOut, `processStreamEvent processing must not be timed-out`);

            if (!results.discardedUnusableRecords || results.discardUnusableRecordsError) {
              t.fail(`discardUnusableRecords must not fail with ${results.discardUnusableRecordsError}`);
            }
            if (results.discardedUnusableRecords) {
              t.equal(results.discardedUnusableRecords.length, 0, `discardUnusableRecords must have ${0} discarded unusable records`);
            }

            if (results.handledIncompleteMessages || !results.handleIncompleteMessagesError) {
              t.fail(`handleIncompleteMessages must fail with ${results.handleIncompleteMessagesError}`);
            }
            t.equal(results.handleIncompleteMessagesError, fatalError, `handleIncompleteMessages must fail with ${fatalError}`);

            if (!results.discardedRejectedMessages || results.discardRejectedMessagesError) {
              t.fail(`discardRejectedMessages must not fail with ${results.discardRejectedMessagesError}`);
            }
            if (results.discardedRejectedMessages) {
              t.equal(results.discardedRejectedMessages.length, 0, `discardedRejectedMessages must have ${0} discarded rejected messages`);
            }

            t.end();
          });
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err.stack);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

// =====================================================================================================================
// processStreamEvent with failing processAll message(s)
// =====================================================================================================================

test('processStreamEvent with 1 message that fails its processAll task, resubmits', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const event = sampleKinesisEvent(streamName, undefined, sampleMessage(1), false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev1', maxTimeInMillis);

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    configureDefaults(t, context, undefined);

    // Setup the task definitions
    const processAllError = new Error('Failing process all task');
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, undefined));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, processAllError));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      streamConsumer.configureStreamConsumer(context, undefined, undefined, event, awsContext);
      const promise = streamConsumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promise.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev1', `context.stage must be dev1`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(results => {
          t.pass(`processStreamEvent must resolve`);
          const n = 1;
          const messages = results.messages;
          t.equal(messages.length, n, `processStreamEvent results must have ${n} messages`);
          checkMessagesTasksStates(t, messages, taskStates.CompletedState, taskStates.FailedState, context);
          t.equal(messages[0].taskTracking.ones.Task1.attempts, 1, `Task1 attempts must be 1`);
          t.equal(messages[0].taskTracking.alls.Task2.attempts, 1, `Task2 attempts must be 1`);

          t.ok(results.processing.completed, `processStreamEvent processing must be completed`);
          t.notOk(results.processing.failed, `processStreamEvent processing must not be failed`);
          t.notOk(results.processing.timedOut, `processStreamEvent processing must not be timed-out`);

          t.equal(results.unusableRecords.length, 0, `processStreamEvent results must have ${0} unusable records`);
          t.equal(results.discardedUnusableRecords.length, 0, `processStreamEvent results must have ${0} discarded unusable records`);
          t.equal(results.handledIncompleteMessages.length, 1, `processStreamEvent results must have ${1} handled incomplete records`);
          t.equal(results.discardedRejectedMessages.length, 0, `processStreamEvent results must have ${0} discarded rejected messages`);

          t.end();
        })
        .catch(err => {
          t.fail(`processStreamEvent should NOT have failed (${stringify(err)})`, err.stack);
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err.stack);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('processStreamEvent with 1 message that fails its processAll task, but cannot resubmit must fail', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const event = sampleKinesisEvent(streamName, undefined, sampleMessage(1), false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev1', maxTimeInMillis);

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    const fatalError = new Error('Disabling Kinesis');
    configureDefaults(t, context, fatalError);

    // Setup the task definitions
    const processAllError = new Error('Failing process one task');
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, undefined));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, processAllError));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      streamConsumer.configureStreamConsumer(context, undefined, undefined, event, awsContext);
      const promise = streamConsumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promise.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev1', `context.stage must be dev1`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(messages => {
          const n = messages.length;
          t.fail(`processStreamEvent must NOT resolve with ${n} message(s)`);
          t.end();
        })
        .catch(err => {
          t.pass(`processStreamEvent must reject with error (${stringify(err)})`);
          t.equal(err, fatalError, `processStreamEvent error must be ${fatalError}`);

          streamConsumer.awaitStreamConsumerResults(err.streamConsumerResults).then(results => {
            const messages = results.messages;
            t.equal(messages.length, 1, `processStreamEvent results must have ${1} messages`);
            t.equal(results.unusableRecords.length, 0, `processStreamEvent results must have ${0} unusable records`);

            t.ok(results.processing.completed, `processStreamEvent processing must be completed`);
            t.notOk(results.processing.failed, `processStreamEvent processing must not be failed`);
            t.notOk(results.processing.timedOut, `processStreamEvent processing must not be timed-out`);

            if (!results.discardedUnusableRecords || results.discardUnusableRecordsError) {
              t.fail(`discardUnusableRecords must not fail with ${results.discardUnusableRecordsError}`);
            }
            if (results.discardedUnusableRecords) {
              t.equal(results.discardedUnusableRecords.length, 0, `discardUnusableRecords must have ${0} discarded unusable records`);
            }

            if (results.handledIncompleteMessages || !results.handleIncompleteMessagesError) {
              t.fail(`handleIncompleteMessages must fail with ${results.handleIncompleteMessagesError}`);
            }
            t.equal(results.handleIncompleteMessagesError, fatalError, `handleIncompleteMessages must fail with ${fatalError}`);

            if (!results.discardedRejectedMessages || results.discardRejectedMessagesError) {
              t.fail(`discardRejectedMessages must not fail with ${results.discardRejectedMessagesError}`);
            }
            if (results.discardedRejectedMessages) {
              t.equal(results.discardedRejectedMessages.length, 0, `discardedRejectedMessages must have ${0} discarded rejected messages`);
            }

            t.end();
          });
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err.stack);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

// =====================================================================================================================
// processStreamEvent with successful message(s) with inactive tasks, must discard abandoned message(s)
// =====================================================================================================================

test('processStreamEvent with 1 message that succeeds, but has 1 abandoned task - must discard rejected message', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const msg = sampleMessage(1);

    // Add some "history" to this message to give it a no-longer active task that will trigger abandonment of this task
    const taskX = Task.createTask(TaskDef.defineTask('TaskX', execute1));
    taskX.fail(new Error('Previously failed'));
    for (let i = 0; i < 100; ++i) {
      taskX.incrementAttempts();
    }
    const taskXLike = JSON.parse(JSON.stringify(taskX));

    msg.taskTracking = {ones: {'TaskX': taskXLike}};
    const m = JSON.parse(JSON.stringify(msg));
    t.ok(m, 'Message with tasks is parsable');
    const taskXRevived = m.taskTracking.ones.TaskX;
    t.ok(Task.isTaskLike(taskXRevived), `TaskX must be task-like (${stringify(taskXRevived)})`);
    t.deepEqual(taskXRevived, taskXLike, `TaskX revived must be original TaskX task-like (${stringify(taskXRevived)})`);
    // console.log(`##### TASK X  ${JSON.stringify(taskX)}`);
    // console.log(`##### REVIVED ${JSON.stringify(taskXRevived)}`);

    const event = sampleKinesisEvent(streamName, undefined, msg, false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev1', maxTimeInMillis);

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    configureDefaults(t, context, undefined);

    // Setup the task definitions
    //const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, undefined)); //.then(msg => taskUtils.getTask(msg.taskTracking.ones, 'Task1')
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = []; //[taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      streamConsumer.configureStreamConsumer(context, undefined, undefined, event, awsContext);
      const promise = streamConsumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promise.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev1', `context.stage must be dev1`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(results => {
          t.pass(`processStreamEvent must resolve`);
          const n = 1;
          const messages = results.messages;
          t.equal(messages.length, n, `processStreamEvent results must have ${n} messages`);
          checkMessagesTasksStates(t, messages, taskStates.Abandoned, taskStates.CompletedState, context);
          t.equal(messages[0].taskTracking.ones.TaskX.attempts, 100, `TaskX attempts must be 100`);
          t.equal(messages[0].taskTracking.alls.Task2.attempts, 1, `Task2 attempts must be 1`);

          t.ok(results.processing.completed, `processStreamEvent processing must be completed`);
          t.notOk(results.processing.failed, `processStreamEvent processing must not be failed`);
          t.notOk(results.processing.timedOut, `processStreamEvent processing must not be timed-out`);

          t.equal(results.unusableRecords.length, 0, `processStreamEvent results must have ${0} unusable records`);
          t.equal(results.discardedUnusableRecords.length, 0, `processStreamEvent results must have ${0} discarded unusable records`);
          t.equal(results.handledIncompleteMessages.length, 0, `processStreamEvent results must have ${0} handled incomplete records`);
          t.equal(results.discardedRejectedMessages.length, 1, `processStreamEvent results must have ${1} discarded rejected messages`);

          t.end();
        })
        .catch(err => {
          t.fail(`processStreamEvent should NOT have failed (${stringify(err)})`, err.stack);
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err.stack);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('processStreamEvent with 1 message that succeeds, but has 1 abandoned task - must fail if cannot discard rejected message', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const msg = sampleMessage(1);

    // Add some "history" to this message to give it a no-longer active task that will trigger abandonment of this task
    const taskX = Task.createTask(TaskDef.defineTask('TaskX', execute1));
    taskX.fail(new Error('Previously failed'));
    msg.taskTracking = {ones: {'TaskX': JSON.parse(JSON.stringify(taskX))}};

    const event = sampleKinesisEvent(streamName, undefined, msg, false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev1', maxTimeInMillis);

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    const fatalError = new Error('Disabling Kinesis');
    configureDefaults(t, context, fatalError);

    // Setup the task definitions
    //const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, undefined)); //.then(msg => taskUtils.getTask(msg.taskTracking.ones, 'Task1')
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = []; //[taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      streamConsumer.configureStreamConsumer(context, undefined, undefined, event, awsContext);
      const promise = streamConsumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promise.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev1', `context.stage must be dev1`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(messages => {
          const n = messages.length;
          t.fail(`processStreamEvent must NOT resolve with ${n} message(s)`);
          t.end();
        })
        .catch(err => {
          t.pass(`processStreamEvent must reject with error (${stringify(err)})`);
          t.equal(err, fatalError, `processStreamEvent error must be ${fatalError}`);

          streamConsumer.awaitStreamConsumerResults(err.streamConsumerResults).then(results => {
            const messages = results.messages;
            t.equal(messages.length, 1, `processStreamEvent results must have ${1} messages`);
            t.equal(results.unusableRecords.length, 0, `processStreamEvent results must have ${0} unusable records`);

            t.ok(results.processing.completed, `processStreamEvent processing must be completed`);
            t.notOk(results.processing.failed, `processStreamEvent processing must not be failed`);
            t.notOk(results.processing.timedOut, `processStreamEvent processing must not be timed-out`);

            if (!results.discardedUnusableRecords || results.discardUnusableRecordsError) {
              t.fail(`discardUnusableRecords must not fail with ${results.discardUnusableRecordsError}`);
            }
            if (results.discardedUnusableRecords) {
              t.equal(results.discardedUnusableRecords.length, 0, `discardUnusableRecords must have ${0} discarded unusable records`);
            }

            if (!results.handledIncompleteMessages || results.handleIncompleteMessagesError) {
              t.fail(`handleIncompleteMessages must not fail with ${results.handleIncompleteMessagesError}`);
            }
            if (results.handledIncompleteMessages) {
              t.equal(results.handledIncompleteMessages.length, 0, `handleIncompleteMessages must have ${0} handled incomplete messages`);
            }

            if (results.discardedRejectedMessages || !results.discardRejectedMessagesError) {
              t.fail(`discardRejectedMessages must fail with ${results.discardRejectedMessagesError}`);
            }
            t.equal(results.discardRejectedMessagesError, fatalError, `discardedRejectedMessages must fail with ${fatalError}`);

            t.end();
          });
        });
    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err.stack);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

// =====================================================================================================================
// processStreamEvent with rejecting message(s), must discard rejected message(s)
// =====================================================================================================================

test('processStreamEvent with 1 message that rejects - must discard rejected message', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const msg = sampleMessage(1);

    const event = sampleKinesisEvent(streamName, undefined, msg, false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev1', maxTimeInMillis);

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    configureDefaults(t, context, undefined);

    // Setup the task definitions
    const rejectError = new Error('Rejecting message');
    const executeOneAsync = sampleExecuteOneAsync(5, undefined, (msg, context) => {
      // trigger a rejection from inside
      context.info(`Triggering an internal reject`);
      msg.taskTracking.ones.Task1.reject('Forcing reject', rejectError, true);
    });
    const taskDef1 = TaskDef.defineTask('Task1', executeOneAsync);
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      streamConsumer.configureStreamConsumer(context, undefined, undefined, event, awsContext);
      const promise = streamConsumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promise.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev1', `context.stage must be dev1`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(results => {
          t.pass(`processStreamEvent must resolve`);
          const n = 1;
          const messages = results.messages;
          t.equal(messages.length, n, `processStreamEvent results must have ${n} messages`);
          checkMessagesTasksStates(t, messages, taskStates.Rejected, taskStates.CompletedState, context);
          t.equal(messages[0].taskTracking.ones.Task1.attempts, 1, `Task1 attempts must be 1`);
          t.equal(messages[0].taskTracking.alls.Task2.attempts, 1, `Task2 attempts must be 1`);

          t.ok(results.processing.completed, `processStreamEvent processing must be completed`);
          t.notOk(results.processing.failed, `processStreamEvent processing must not be failed`);
          t.notOk(results.processing.timedOut, `processStreamEvent processing must not be timed-out`);

          t.equal(results.unusableRecords.length, 0, `processStreamEvent results must have ${0} unusable records`);
          t.equal(results.discardedUnusableRecords.length, 0, `processStreamEvent results must have ${0} discarded unusable records`);
          t.equal(results.handledIncompleteMessages.length, 0, `processStreamEvent results must have ${0} handled incomplete records`);
          t.equal(results.discardedRejectedMessages.length, 1, `processStreamEvent results must have ${1} discarded rejected messages`);

          t.end();
        })
        .catch(err => {
          t.fail(`processStreamEvent should NOT have failed (${stringify(err)})`, err.stack);
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err.stack);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('processStreamEvent with 1 message that rejects, but cannot discard rejected message must fail', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const msg = sampleMessage(1);

    const event = sampleKinesisEvent(streamName, undefined, msg, false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev1', maxTimeInMillis);

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    const fatalError = new Error('Disabling Kinesis');
    configureDefaults(t, context, fatalError);

    // Setup the task definitions
    const rejectError = new Error('Rejecting message');
    const executeOneAsync = sampleExecuteOneAsync(5, undefined, (msg, context) => {
      // trigger a rejection from inside
      context.info(`Triggering an internal reject`);
      msg.taskTracking.ones.Task1.reject('Forcing reject', rejectError, true);
    });
    const taskDef1 = TaskDef.defineTask('Task1', executeOneAsync);
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      streamConsumer.configureStreamConsumer(context, undefined, undefined, event, awsContext);
      const promise = streamConsumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promise.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev1', `context.stage must be dev1`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(messages => {
          const n = messages.length;
          t.fail(`processStreamEvent must NOT resolve with ${n} message(s)`);
          t.end();
        })
        .catch(err => {
          t.pass(`processStreamEvent must reject with error (${stringify(err)})`);
          t.equal(err, fatalError, `processStreamEvent error must be ${fatalError}`);

          streamConsumer.awaitStreamConsumerResults(err.streamConsumerResults).then(results => {
            const messages = results.messages;
            t.equal(messages.length, 1, `processStreamEvent results must have ${1} messages`);
            t.equal(results.unusableRecords.length, 0, `processStreamEvent results must have ${0} unusable records`);

            t.ok(results.processing.completed, `processStreamEvent processing must be completed`);
            t.notOk(results.processing.failed, `processStreamEvent processing must not be failed`);
            t.notOk(results.processing.timedOut, `processStreamEvent processing must not be timed-out`);

            if (!results.discardedUnusableRecords || results.discardUnusableRecordsError) {
              t.fail(`discardUnusableRecords must not fail with ${results.discardUnusableRecordsError}`);
            }
            if (results.discardedUnusableRecords) {
              t.equal(results.discardedUnusableRecords.length, 0, `discardUnusableRecords must have ${0} discarded unusable records`);
            }

            if (!results.handledIncompleteMessages || results.handleIncompleteMessagesError) {
              t.fail(`handleIncompleteMessages must not fail with ${results.handleIncompleteMessagesError}`);
            }
            if (results.handledIncompleteMessages) {
              t.equal(results.handledIncompleteMessages.length, 0, `handleIncompleteMessages must have ${0} handled incomplete messages`);
            }

            if (results.discardedRejectedMessages || !results.discardRejectedMessagesError) {
              t.fail(`discardRejectedMessages must fail with ${results.discardRejectedMessagesError}`);
            }
            t.equal(results.discardRejectedMessagesError, fatalError, `discardedRejectedMessages must fail with ${fatalError}`);

            t.end();
          });
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err.stack);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

// =====================================================================================================================
// processStreamEvent with message(s) exceeding max number of attempts on all tasks, must discard Discarded message(s)
// =====================================================================================================================

test('processStreamEvent with 1 message that exceeds max number of attempts on all its tasks - must discard Discarded message', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    configureDefaults(t, context, undefined);

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const msg = sampleMessage(1);

    const maxNumberOfAttempts = streamProcessing.getMaxNumberOfAttempts(context);

    // Add some "history" to this message to give it a no-longer active task that will trigger abandonment of this task
    const task1Before = Task.createTask(TaskDef.defineTask('Task1', execute1));
    task1Before.fail(new Error('Previously failed Task1'));

    const task2Before = Task.createTask(TaskDef.defineTask('Task2', execute1));
    task2Before.fail(new Error('Previously failed Task2'));

    // Push both tasks number of attempts to the brink
    for (let a = 0; a < maxNumberOfAttempts - 1; ++a) {
      task1Before.incrementAttempts();
      task2Before.incrementAttempts();
    }
    t.equal(task1Before.attempts, maxNumberOfAttempts - 1, `BEFORE Task1 attempts must be ${maxNumberOfAttempts - 1}`);
    t.equal(task2Before.attempts, maxNumberOfAttempts - 1, `BEFORE Task2 attempts must be ${maxNumberOfAttempts - 1}`);

    const task1Like = JSON.parse(JSON.stringify(task1Before));
    const task2Like = JSON.parse(JSON.stringify(task2Before));

    msg.taskTracking = {ones: {'Task1': task1Like}, alls: {'Task2': task2Like}};

    const m = JSON.parse(JSON.stringify(msg));
    t.ok(m, 'Message with tasks is parsable');

    const task1Revived = m.taskTracking.ones.Task1;
    t.ok(Task.isTaskLike(task1Revived), `Task1 must be task-like (${stringify(task1Revived)})`);
    t.deepEqual(task1Revived, task1Like, `Task1 revived must be original Task1 task-like (${stringify(task1Revived)})`);

    const task2Revived = m.taskTracking.alls.Task2;
    t.ok(Task.isTaskLike(task2Revived), `Task2 must be task-like (${stringify(task2Revived)})`);
    t.deepEqual(task2Revived, task2Like, `Task2 revived must be original Task2 task-like (${stringify(task2Revived)})`);

    t.equal(task1Revived.attempts, maxNumberOfAttempts - 1, `REVIVED Task1 attempts must be ${maxNumberOfAttempts - 1}`);
    t.equal(task2Revived.attempts, maxNumberOfAttempts - 1, `REVIVED Task2 attempts must be ${maxNumberOfAttempts - 1}`);


    const event = sampleKinesisEvent(streamName, undefined, msg, false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev1', maxTimeInMillis);

    // Setup the task definitions
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, new Error('Final failure')));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, new Error('Final failure')));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      streamConsumer.configureStreamConsumer(context, undefined, undefined, event, awsContext);
      const promise = streamConsumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promise.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev1', `context.stage must be dev1`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(results => {
          t.pass(`processStreamEvent must resolve`);
          const n = 1;
          const messages = results.messages;
          t.equal(messages.length, n, `processStreamEvent results must have ${n} messages`);
          checkMessagesTasksStates(t, messages, taskStates.Discarded, taskStates.Discarded, context);
          t.equal(messages[0].taskTracking.ones.Task1.attempts, maxNumberOfAttempts, `Task1 attempts must be ${maxNumberOfAttempts}`);
          t.equal(messages[0].taskTracking.alls.Task2.attempts, maxNumberOfAttempts, `Task2 attempts must be ${maxNumberOfAttempts}`);

          t.ok(results.processing.completed, `processStreamEvent processing must be completed`);
          t.notOk(results.processing.failed, `processStreamEvent processing must not be failed`);
          t.notOk(results.processing.timedOut, `processStreamEvent processing must not be timed-out`);

          t.equal(results.unusableRecords.length, 0, `processStreamEvent results must have ${0} unusable records`);
          t.equal(results.discardedUnusableRecords.length, 0, `processStreamEvent results must have ${0} discarded unusable records`);
          t.equal(results.handledIncompleteMessages.length, 0, `processStreamEvent results must have ${0} handled incomplete records`);
          t.equal(results.discardedRejectedMessages.length, 1, `processStreamEvent results must have ${1} discarded rejected messages`);

          t.end();
        })
        .catch(err => {
          t.fail(`processStreamEvent should NOT have failed (${stringify(err)})`, err.stack);
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err.stack);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('processStreamEvent with 1 message that exceeds max number of attempts on all its tasks, but cannot discard message must fail', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    const fatalError = new Error('Disabling Kinesis');
    configureDefaults(t, context, fatalError);

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const msg = sampleMessage(1);

    const maxNumberOfAttempts = streamProcessing.getMaxNumberOfAttempts(context);

    // Add some "history" to this message to give it a no-longer active task that will trigger abandonment of this task
    const task1Before = Task.createTask(TaskDef.defineTask('Task1', execute1));
    task1Before.fail(new Error('Previously failed Task1'));

    const task2Before = Task.createTask(TaskDef.defineTask('Task2', execute1));
    task2Before.fail(new Error('Previously failed Task2'));

    // Push both tasks number of attempts to the brink
    for (let a = 0; a < maxNumberOfAttempts - 1; ++a) {
      task1Before.incrementAttempts();
      task2Before.incrementAttempts();
    }

    const task1Like = JSON.parse(JSON.stringify(task1Before));
    const task2Like = JSON.parse(JSON.stringify(task2Before));

    msg.taskTracking = {ones: {'Task1': task1Like}, alls: {'Task2': task2Like}};

    const m = JSON.parse(JSON.stringify(msg));
    t.ok(m, 'Message with tasks is parsable');

    const task1Revived = m.taskTracking.ones.Task1;
    t.ok(Task.isTaskLike(task1Revived), `Task1 must be task-like (${stringify(task1Revived)})`);
    t.deepEqual(task1Revived, task1Like, `Task1 revived must be original Task1 task-like (${stringify(task1Revived)})`);

    const task2Revived = m.taskTracking.alls.Task2;
    t.ok(Task.isTaskLike(task2Revived), `Task2 must be task-like (${stringify(task2Revived)})`);
    t.deepEqual(task2Revived, task2Like, `Task2 revived must be original Task2 task-like (${stringify(task2Revived)})`);


    const event = sampleKinesisEvent(streamName, undefined, msg, false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev1', maxTimeInMillis);

    // Setup the task definitions
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, new Error('Final failure')));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, new Error('Final failure')));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      streamConsumer.configureStreamConsumer(context, undefined, undefined, event, awsContext);
      const promise = streamConsumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promise.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev1', `context.stage must be dev1`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(results => {
          const n = results.messages.length;
          t.fail(`processStreamEvent must NOT resolve with ${n} message(s)`);
          t.end();
        })
        .catch(err => {
          t.pass(`processStreamEvent must reject with error (${stringify(err)})`);
          t.equal(err, fatalError, `processStreamEvent error must be ${fatalError}`);

          streamConsumer.awaitStreamConsumerResults(err.streamConsumerResults).then(results => {
            const messages = results.messages;
            t.equal(messages.length, 1, `processStreamEvent results must have ${1} messages`);
            t.equal(results.unusableRecords.length, 0, `processStreamEvent results must have ${0} unusable records`);

            t.ok(results.processing.completed, `processStreamEvent processing must be completed`);
            t.notOk(results.processing.failed, `processStreamEvent processing must not be failed`);
            t.notOk(results.processing.timedOut, `processStreamEvent processing must not be timed-out`);

            if (!results.discardedUnusableRecords || results.discardUnusableRecordsError) {
              t.fail(`discardUnusableRecords must not fail with ${results.discardUnusableRecordsError}`);
            }
            if (results.discardedUnusableRecords) {
              t.equal(results.discardedUnusableRecords.length, 0, `discardUnusableRecords must have ${0} discarded unusable records`);
            }

            if (!results.handledIncompleteMessages || results.handleIncompleteMessagesError) {
              t.fail(`handleIncompleteMessages must not fail with ${results.handleIncompleteMessagesError}`);
            }
            if (results.handledIncompleteMessages) {
              t.equal(results.handledIncompleteMessages.length, 0, `handleIncompleteMessages must have ${0} handled incomplete messages`);
            }

            if (results.discardedRejectedMessages || !results.discardRejectedMessagesError) {
              t.fail(`discardRejectedMessages must fail with ${results.discardRejectedMessagesError}`);
            }
            t.equal(results.discardRejectedMessagesError, fatalError, `discardedRejectedMessages must fail with ${fatalError}`);

            t.end();
          });
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err.stack);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('processStreamEvent with 1 message that only exceeds max number of attempts on 1 of its 2 its tasks, must not discard message yet', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    configureDefaults(t, context, undefined);

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const msg = sampleMessage(1);

    const maxNumberOfAttempts = streamProcessing.getMaxNumberOfAttempts(context);

    // Add some "history" to this message to give it a no-longer active task that will trigger abandonment of this task
    const task1Before = Task.createTask(TaskDef.defineTask('Task1', execute1));
    task1Before.fail(new Error('Previously failed Task1'));

    const task2Before = Task.createTask(TaskDef.defineTask('Task2', execute1));
    task2Before.fail(new Error('Previously failed Task2'));

    // Push 1st task's number of attempts to max - 2 (i.e. won't exceed this round)
    for (let a = 0; a < maxNumberOfAttempts - 2; ++a) {
      task1Before.incrementAttempts();
    }
    // Push 2nd task's number of attempts to max - 1 (will exceed this round
    for (let a = 0; a < maxNumberOfAttempts - 1; ++a) {
      task2Before.incrementAttempts();
    }

    const task1Like = JSON.parse(JSON.stringify(task1Before));
    const task2Like = JSON.parse(JSON.stringify(task2Before));

    msg.taskTracking = {ones: {'Task1': task1Like}, alls: {'Task2': task2Like}};

    const m = JSON.parse(JSON.stringify(msg));
    t.ok(m, 'Message with tasks is parsable');

    const task1Revived = m.taskTracking.ones.Task1;
    t.ok(Task.isTaskLike(task1Revived), `Task1 must be task-like (${stringify(task1Revived)})`);
    t.deepEqual(task1Revived, task1Like, `Task1 revived must be original Task1 task-like (${stringify(task1Revived)})`);

    const task2Revived = m.taskTracking.alls.Task2;
    t.ok(Task.isTaskLike(task2Revived), `Task2 must be task-like (${stringify(task2Revived)})`);
    t.deepEqual(task2Revived, task2Like, `Task2 revived must be original Task2 task-like (${stringify(task2Revived)})`);


    const event = sampleKinesisEvent(streamName, undefined, msg, false);

    // Generate a sample AWS context
    const maxTimeInMillis = 1000;
    const awsContext = sampleAwsContext('1.0.1', 'dev1', maxTimeInMillis);

    // Setup the task definitions
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(5, new Error('Final failure')));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(5, new Error('Final failure')));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      streamConsumer.configureStreamConsumer(context, undefined, undefined, event, awsContext);
      const promise = streamConsumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promise.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev1', `context.stage must be dev1`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(results => {
          t.pass(`processStreamEvent must resolve`);
          const n = 1;
          const messages = results.messages;
          t.equal(messages.length, n, `processStreamEvent results must have ${n} messages`);
          checkMessagesTasksStates(t, messages, taskStates.Failed, taskStates.Failed, context);
          t.equal(messages[0].taskTracking.ones.Task1.attempts, maxNumberOfAttempts - 1, `Task1 attempts must be ${maxNumberOfAttempts - 1}`);
          t.equal(messages[0].taskTracking.alls.Task2.attempts, maxNumberOfAttempts, `Task1 attempts must be ${maxNumberOfAttempts}`);

          t.ok(results.processing.completed, `processStreamEvent processing must be completed`);
          t.notOk(results.processing.failed, `processStreamEvent processing must not be failed`);
          t.notOk(results.processing.timedOut, `processStreamEvent processing must not be timed-out`);

          t.equal(results.unusableRecords.length, 0, `processStreamEvent results must have ${0} unusable records`);
          t.equal(results.discardedUnusableRecords.length, 0, `processStreamEvent results must have ${0} discarded unusable records`);
          t.equal(results.handledIncompleteMessages.length, 1, `processStreamEvent results must have ${1} handled incomplete records`);
          t.equal(results.discardedRejectedMessages.length, 0, `processStreamEvent results must have ${0} discarded rejected messages`);

          t.end();
        })
        .catch(err => {
          t.fail(`processStreamEvent should NOT have failed (${stringify(err)})`, err.stack);
          t.end(err);
        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err.stack);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

// =====================================================================================================================
// processStreamEvent with 1 message and triggered timeout promise, must resubmit incomplete message
// =====================================================================================================================

test('processStreamEvent with 1 message and triggered timeout promise, must resubmit incomplete message', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    configureDefaults(t, context, undefined);

    const n = 1;

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const event = sampleKinesisEvent(streamName, undefined, sampleMessage(1), false);

    // Generate a sample AWS context
    const maxTimeInMillis = 10;
    const awsContext = sampleAwsContext('1.0.1', 'dev1', maxTimeInMillis);

    // Setup the task definitions
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(15, undefined));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(15, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      streamConsumer.configureStreamConsumer(context, undefined, undefined, event, awsContext);
      const promise = streamConsumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promise.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev1', `context.stage must be dev1`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(results => {
          t.pass(`processStreamEvent must resolve`);
          const n = 1;
          const messages = results.messages;
          t.equal(messages.length, n, `processStreamEvent results must have ${n} messages`);
          checkMessagesTasksStates(t, messages, taskStates.TimedOut, taskStates.TimedOut, context);
          t.equal(messages[0].taskTracking.ones.Task1.attempts, 1, `Task1 attempts must be 1`);
          t.equal(messages[0].taskTracking.alls.Task2.attempts, 1, `Task2 attempts must be 1`);

          t.notOk(results.processing.completed, `processStreamEvent processing must not be completed`);
          t.notOk(results.processing.failed, `processStreamEvent processing must not be failed`);
          t.ok(results.processing.timedOut, `processStreamEvent processing must be timed-out`);

          t.equal(results.unusableRecords.length, 0, `processStreamEvent results must have ${0} unusable records`);
          t.equal(results.discardedUnusableRecords.length, 0, `processStreamEvent results must have ${0} discarded unusable records`);
          t.equal(results.handledIncompleteMessages.length, 1, `processStreamEvent results must have ${1} handled incomplete records`);
          t.equal(results.discardedRejectedMessages.length, 0, `processStreamEvent results must have ${0} discarded rejected messages`);

          t.end();
        })

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err.stack);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('processStreamEvent with 1 message and triggered timeout promise, must fail if it cannot resubmit incomplete messages', t => {
  try {
    // Simulate a region in AWS_REGION for testing (if none already exists)
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};

    // Simulate ideal conditions - everything meant to be configured beforehand has been configured
    const fatalError = new Error('Disabling Kinesis');
    configureDefaults(t, context, fatalError);

    const n = 1;

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const event = sampleKinesisEvent(streamName, undefined, sampleMessage(1), false);

    // Generate a sample AWS context
    const maxTimeInMillis = 10;
    const awsContext = sampleAwsContext('1.0.1', 'dev1', maxTimeInMillis);

    // Setup the task definitions
    const taskDef1 = TaskDef.defineTask('Task1', sampleExecuteOneAsync(15, undefined));
    const taskDef2 = TaskDef.defineTask('Task2', sampleExecuteAllAsync(15, undefined));
    const processOneTaskDefs = [taskDef1];
    const processAllTaskDefs = [taskDef2];

    // Process the event
    try {
      streamConsumer.configureStreamConsumer(context, undefined, undefined, event, awsContext);
      const promise = streamConsumer.processStreamEvent(event, processOneTaskDefs, processAllTaskDefs, context);

      if (Promise.isPromise(promise)) {
        t.pass(`processStreamEvent returned a promise`);
      } else {
        t.fail(`processStreamEvent should have returned a promise`);
      }

      t.equal(context.region, region, `context.region must be ${region}`);
      t.equal(context.stage, 'dev1', `context.stage must be dev1`);
      t.equal(context.awsContext, awsContext, `context.awsContext must be given awsContext`);

      promise
        .then(messages => {
          const n = messages.length;
          t.fail(`processStreamEvent must NOT resolve with ${n} message(s)`);
          t.end();
        })
        .catch(err => {
          t.pass(`processStreamEvent must reject with error (${stringify(err)})`);
          t.equal(err, fatalError, `processStreamEvent error must be ${fatalError}`);

          streamConsumer.awaitStreamConsumerResults(err.streamConsumerResults).then(results => {
            const messages = results.messages;
            t.equal(messages.length, 1, `processStreamEvent results must have ${1} messages`);
            t.equal(results.unusableRecords.length, 0, `processStreamEvent results must have ${0} unusable records`);

            t.notOk(results.processing.completed, `processStreamEvent processing must not be completed`);
            t.notOk(results.processing.failed, `processStreamEvent processing must not be failed`);
            t.ok(results.processing.timedOut, `processStreamEvent processing must be timed-out`);

            if (!results.discardedUnusableRecords || results.discardUnusableRecordsError) {
              t.fail(`discardUnusableRecords must not fail with ${results.discardUnusableRecordsError}`);
            }
            if (results.discardedUnusableRecords) {
              t.equal(results.discardedUnusableRecords.length, 0, `discardUnusableRecords must have ${0} discarded unusable records`);
            }

            if (results.handledIncompleteMessages || !results.handleIncompleteMessagesError) {
              t.fail(`handleIncompleteMessages must fail with ${results.handleIncompleteMessagesError}`);
            }
            t.equal(results.handleIncompleteMessagesError, fatalError, `handleIncompleteMessages must fail with ${fatalError}`);

            if (!results.discardedRejectedMessages || results.discardRejectedMessagesError) {
              t.fail(`discardRejectedMessages must not fail with ${results.discardRejectedMessagesError}`);
            }
            if (results.discardedRejectedMessages) {
              t.equal(results.discardedRejectedMessages.length, 0, `discardedRejectedMessages must have ${0} discarded rejected messages`);
            }

            t.end();
          });

        });

    } catch (err) {
      t.fail(`processStreamEvent should NOT have failed in try-catch (${stringify(err)})`, err.stack);
      t.end(err);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});
