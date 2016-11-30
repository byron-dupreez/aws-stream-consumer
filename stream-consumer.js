'use strict';

// AWS core utilities
const regions = require('aws-core-utils/regions');
const stages = require('aws-core-utils/stages');
// const arns = require('aws-core-utils/arns');
const streamEvents = require('aws-core-utils/stream-events');

const Strings = require('core-functions/strings');
//const isBlank = Strings.isBlank;
const isNotBlank = Strings.isNotBlank;
// const trim = Strings.trim;
const stringify = Strings.stringify;

const Arrays = require('core-functions/arrays');
require('core-functions/promises');

// Tasks, task definitions, task states & task utilities
const states = require('task-utils/task-states');
const taskDefs = require('task-utils/task-defs');
const TaskDef = taskDefs.TaskDef;
const Tasks = require('task-utils/tasks');
const Task = Tasks.Task;
const taskUtils = require('task-utils/task-utils');

// Dependencies
const logging = require('logging-utils');
//const stageHandling = require('aws-core-utils/stages');
const streamProcessing = require('./stream-processing');
//const kinesisCache = require('aws-core-utils/kinesis-cache');

/**
 * Utilities and functions to be used to robustly consume messages from an AWS Kinesis or DynamoDB event stream.
 * @module aws-stream-consumer/stream-consumer
 * @author Byron du Preez
 */
module.exports = {
  // Configuration
  isStreamConsumerConfigured: isStreamConsumerConfigured,
  configureStreamConsumer: configureStreamConsumer,
  configureRegionStageAndAwsContext: configureRegionStageAndAwsContext,

  // Processing
  processStreamEvent: processStreamEvent,

  validateTaskDefinitions: validateTaskDefinitions,

  getTaskTracking: getTaskTracking,
  getProcessOneTasksByName: getProcessOneTasksByName,
  getProcessOneTask: getProcessOneTask,
  getProcessAllTasksByName: getProcessAllTasksByName,
  getProcessAllTask: getProcessAllTask,
  setRecord: setRecord,

  awaitStreamProcessingPartialResults: awaitStreamProcessingPartialResults,
  awaitAndLogStreamProcessingPartialResults: awaitAndLogStreamProcessingPartialResults,

  FOR_TESTING_ONLY: {
    logStreamEvent: logStreamEvent,
    processStreamEventRecords: processStreamEventRecords,
    processStreamEventRecord: processStreamEventRecord,
    extractMessageFromStreamEventRecord: extractMessageFromStreamEventRecord,
    executeProcessOneTasks: executeProcessOneTasks,
    executeProcessOneTask: executeProcessOneTask,
    executeProcessAllTasks: executeProcessAllTasks,
    executeProcessAllTask: executeProcessAllTask,
    createTimeoutPromise: createTimeoutPromise,
    createCompletedPromise: createCompletedPromise,
    discardAnyUnusableRecords: discardAnyUnusableRecords,
    finaliseMessageProcessing: finaliseMessageProcessing,
    discardIncompleteTasksIfMaxAttemptsExceeded: discardIncompleteTasksIfMaxAttemptsExceeded,
    handleAnyIncompleteMessages: handleAnyIncompleteMessages,
    isMessageIncomplete: isMessageIncomplete,
    discardAnyRejectedMessages: discardAnyRejectedMessages,
    isMessageFinalisedButRejected: isMessageFinalisedButRejected
  }
};

// =====================================================================================================================
// Consumer configuration - configures the runtime settings for a stream consumer on a given context from a given AWS event and AWS context
// =====================================================================================================================

/**
 * @typedef {Object} StreamConsumerSettings - configuration settings
 * @property {LoggingSettings|undefined} [loggingSettings] - optional logging settings to use to configure logging
 * @property {StageHandlingSettings|undefined} [stageHandlingSettings] - optional stage handling settings to use to configure stage handling
 * @property {StreamProcessingSettings|undefined} [streamProcessingSettings] - optional stream processing settings to use to configure stream processing
 */

/**
 * @typedef {Object} StreamConsumerOptions - configuration options to use if no corresponding settings are provided
 * @property {LoggingOptions|undefined} [loggingOptions] - optional logging options to use to configure logging
 * @property {StageHandlingOptions|undefined} [stageHandlingOptions] - optional stage handling options to use to configure stage handling
 * @property {StreamProcessingOptions|undefined} [streamProcessingOptions] - optional stream processing options to use to configure stream processing
 */

/**
 * Returns true if the stream consumer's dependencies and runtime settings have been configured on the given context;
 * otherwise returns false.
 * @param {Object} context - the context to check
 * @returns {boolean} true if configured; false otherwise
 */
function isStreamConsumerConfigured(context) {
  return !!context && logging.isLoggingConfigured(context) && stages.isStageHandlingConfigured(context) &&
    context.region && context.stage && context.awsContext && streamProcessing.isStreamProcessingConfigured(context) &&
    context.streamConsumer && typeof context.streamConsumer === 'object';
}

/**
 * Configures the dependencies and runtime settings for the stream consumer on the given context from the given settings
 * and/or options, the given AWS event and the given AWS context in preparation for processing of a batch of Kinesis or
 * DynamoDB stream records. Any error thrown must subsequently trigger a replay of all the records in the current batch
 * until the Lambda can be fixed.
 *
 * @param {Object} context - the context onto which to configure a stream consumer's runtime settings
 * @param {StreamConsumerSettings|undefined} [settings] - optional configuration settings to use to configure dependencies
 * @param {StreamConsumerOptions|undefined} [options] - configuration options to use to configure dependencies if no corresponding settings are provided
 * @param {Object} event - the AWS event, which was passed to your lambda
 * @param {Object} awsContext - the AWS context, which was passed to your lambda
 * @return {Object} the context object configured with a stream consumer's runtime settings
 * @throws {Error} an error if the region and/or stage cannot be resolved
 */
function configureStreamConsumer(context, settings, options, event, awsContext) {
  // Configure stream processing (plus logging, stage handling & kinesis) if not configured yet
  streamProcessing.configureStreamProcessing(context, settings ? settings.streamProcessingSettings : undefined,
    options ? options.streamProcessingOptions : undefined, settings, options, false);

  // Configure region, stage & AWS context
  configureRegionStageAndAwsContext(context, event, awsContext);

  // Set up a streamConsumer object on the context onto which to track some of the overall flow state
  if (!context.streamConsumer) {
    context.streamConsumer = {};
  }
}

/**
 * Configures the given context with the current region, the resolved stage and the given AWS context.
 * @param {Object} context - the context to configure
 * @param {Object} event - the AWS event, which was passed to your lambda
 * @param {Object} awsContext - the AWS context, which was passed to your lambda
 */
function configureRegionStageAndAwsContext(context, event, awsContext) {
  // Configure context.awsContext with the given AWS context, if not already configured
  if (!context.awsContext) {
    context.awsContext = awsContext;
  }
  // Configure context.region to the AWS region, if it is not already configured
  regions.configureRegion(context, true);

  // Resolve the current stage (e.g. dev, qa, prod, ...) if possible and configure context.stage with it, if it is not
  // already configured
  stages.configureStage(context, event, awsContext, true);

  context.info(`Using region (${context.region}) and stage (${context.stage})`);
  return context;
}

// =====================================================================================================================
// Process stream event
// =====================================================================================================================

/**
 * @typedef {Object} StreamProcessingResults - the stream processing results, which are returned when stream processing
 * completes successfully
 * @property {Object[]} messages - a list of zero or more successfully extracted message objects
 * @property {Object[]} unusableRecords - a list of zero or more unusable records
 * @property {boolean} processingCompleted - whether or not message processing completed successfully or not
 * @property {boolean} processingFailed - whether or not message processing failed or not
 * @property {boolean} processingTimedOut - whether or not message processing timed out or not
 * @property {Object[]|undefined} [handledIncompleteMessages] - an optional list of zero or more successfully handled incomplete messages
 * @property {Object[]|undefined} [discardedUnusableRecords] - an optional list of zero or more successfully discarded unusable records
 * @property {Object[]|undefined} [discardedRejectedMessages] - an optional list of zero or more successfully discarded rejected messages
 * @property {Error|undefined} [handleIncompleteMessagesError] - an optional error with which handle incomplete records failed
 * @property {Error|undefined} [discardUnusableRecordsError] - an optional error with which discard unusable records failed
 * @property {Error|undefined} [discardRejectedMessagesError] - an optional error with which discard rejected messages failed
 */

/**
 * @typedef {Object} StreamProcessingPartialResults - the stream processing partial results, which are attached to the
 * final error when stream processing fails
 * @property {Object[]} messages - a list of zero or more successfully extracted message objects
 * @property {Object[]} unusableRecords - a list of zero or more unusable records
 * @property {boolean} processingCompleted - whether or not message processing completed successfully or not
 * @property {boolean} processingFailed - whether or not message processing failed or not
 * @property {boolean} processingTimedOut - whether or not message processing timed out or not
 * @property {Promise.<Object[]|Error>} handleIncompleteMessagesPromise - a promise of either a resolved list of zero or more successfully handled incomplete records or a rejected error
 * @property {Promise.<Object[]|Error>} discardUnusableRecordsPromise - a promise of either a resolved list of zero or more successfully discarded unusable records or a rejected error
 * @property {Promise.<Object[]|Error>} discardRejectedMessagesPromise - a promise of either a resolved list of zero or more successfully discarded rejected messages or a rejected error
 */

/**
 * @typedef {Error} StreamProcessingError - the final error returned via a rejected promise when stream processing fails
 * @property {StreamProcessingPartialResults} streamProcessingPartialResults - the partial stream processing results
 * available at the time of the final error
 */

/**
 * Processes the given (Kinesis or DynamoDB) stream event using the given AWS context and context by applying each of
 * the tasks defined by the task definitions in the given processOneTaskDefs and processAllTaskDefs to each message
 * extracted from the event.
 *
 * @param {Object} event - the AWS stream event
 * @param {TaskDef[]|undefined} [processOneTaskDefsOrNone] - an "optional" list of "processOne" task definitions that
 * will be used to generate the tasks to be executed on each message independently
 * @param {TaskDef[]|undefined} [processAllTaskDefsOrNone] - an "optional" list of "processAll" task definitions that
 * will be used to generate the tasks to be executed on all of the event's messages collectively
 * @param {Object} context - the configured context to use
 * @returns {Promise.<StreamProcessingResults|StreamProcessingError>} a resolved promise with the full stream processing
 * results or a rejected promise with an error with partial stream processing results
 */
function processStreamEvent(event, processOneTaskDefsOrNone, processAllTaskDefsOrNone, context) {
  // Ensure that the stream consumer is fully configured before proceeding, and if not, trigger a replay of all the
  // records until it can be fixed
  if (!isStreamConsumerConfigured(context)) {
    const errMsg = `FATAL - Your stream consumer MUST be configured before invoking processStreamEvents (see stream-consumer#configureStreamConsumer & stream-processing). Fix your Lambda and redeploy ASAP, since this issue is blocking all of your stream's shards!`;
    (context.error ? context.error : console.error)(errMsg);
    return Promise.reject(new Error(errMsg));
  }

  try {
    // Check if the Lambda as configured will be unusable or useless, and if so, trigger a replay of all the records until
    // it can be fixed
    validateTaskDefinitions(processOneTaskDefsOrNone, processAllTaskDefsOrNone, context);

  } catch (err) {
    return Promise.reject(err);
  }

  // Clean up any undefined or null task definition lists
  const processOneTaskDefs = processOneTaskDefsOrNone ? processOneTaskDefsOrNone : [];
  const processAllTaskDefs = processAllTaskDefsOrNone ? processAllTaskDefsOrNone : [];

  if (context.debugEnabled) logStreamEvent(event, "Processing stream event", false, context);

  const records = event.Records;
  if (!records || records.length <= 0) {
    logStreamEvent(event, "Missing Records on stream event", true, context);
    return Promise.resolve([]);
  }

  // Convert all of the parsable Kinesis event's records back into their original message object forms; skipping &
  // logging all unparseable records
  const results = processStreamEventRecords(records, processOneTaskDefs, processAllTaskDefs, context);
  const messages = results[0];
  const processOneTasksPromise = results[1];
  const processAllTasksPromise = results[2];
  const unusableRecords = results[3];

  const processedPromise = Promise.all([processOneTasksPromise, processAllTasksPromise]);

  // Discard all unusable records
  const discardUnusableRecordsPromise = discardAnyUnusableRecords(unusableRecords, records, context);

  // Set a timeout to trigger when a configurable percentage of the remaining time in millis is reached, which will give
  // us a bit of time to finalise at least some of the message processing before we run out of time to complete
  // everything in this invocation
  const cancellable = {};
  const timeoutPromise = createTimeoutPromise(cancellable, context);

  // Build a completed promise that will only continue once the processedPromise and discardUnusableRecordsPromise promises have complete
  const completedPromise = createCompletedPromise(processedPromise, messages, cancellable, context);

  const completedVsTimeoutPromise = Promise.race([completedPromise, timeoutPromise]);

  // Whichever finishes first, finalise message processing as best as possible, e.g. by handling any incomplete
  // messages (e.g. by ideally avoiding replaying all of them)
  return completedVsTimeoutPromise
    .then(results => finaliseMessageProcessing(messages, unusableRecords, discardUnusableRecordsPromise, context))
    .catch(err => {
      context.error(`Stream processing failed with error (${err})`, err.stack);
      return Promise.reject(err);
    });
}

/**
 * Validates the given processOneTaskDefs and processAllTaskDefs and raises an appropriate error if these task
 * definitions are invalid (and effectively make this Lambda unusable or useless). Any error thrown must subsequently
 * trigger a replay of all the records in this batch until the Lambda can be fixed.
 *
 * @param {TaskDef[]|undefined} processOneTaskDefs - an "optional" list of "processOne" task definitions that will be
 * used to generate the tasks to be executed on each message independently
 * @param {TaskDef[]|undefined} processAllTaskDefs - an "optional" list of "processAll" task definitions that will be
 * used to generate the tasks to be executed on all of the event's messages collectively
 * @param {Object} context the context
 * @throws {Error} a validation failure Error (if this Lambda is unusable or useless)
 */
function validateTaskDefinitions(processOneTaskDefs, processAllTaskDefs, context) {
  function validateTaskDefs(taskDefs, name) {
    if (taskDefs) {
      // Must be an array of executable TaskDef instances with valid execute functions
      if (!Arrays.isArrayOfType(taskDefs, TaskDef) || !taskDefs.every(t => t && t.isExecutable() && typeof t.execute === 'function')) {
        // This Lambda is unusable, so trigger an exception to put all records back until it can be fixed!
        const errMsg = `FATAL - ${name} must be an array of executable TaskDef instances with valid execute functions! Fix your Lambda and redeploy ASAP, since this issue is blocking all of your stream's shards!`;
        context.error(errMsg);
        throw new Error(errMsg);
      }
      // All of the task definitions must have unique names
      const taskNames = taskDefs.map(d => d.name);
      if (!Arrays.isDistinct(taskNames)) {
        // This Lambda is unusable, so trigger an exception to put all records back until it can be fixed!
        const errMsg = `FATAL - ${name} must have no duplicate task names ${stringify(taskNames)}! Fix your Lambda and redeploy ASAP, since this issue is blocking all of your stream's shards!`;
        context.error(errMsg);
        throw new Error(errMsg);
      }
    }
  }

  // Check that processOneTasks (if defined) is an array of executable TaskDef instances with valid execute functions
  validateTaskDefs(processOneTaskDefs, 'processOneTaskDefs');

  // Check that processAllTasks (if defined) is an array of executable TaskDef instances with valid execute functions
  validateTaskDefs(processAllTaskDefs, 'processAllTaskDefs');

  // Check that at least one task is defined across both processOneTasks and processAllTasks - otherwise no progress can be made at all!
  if ((!processOneTaskDefs || processOneTaskDefs.length <= 0) && (!processAllTaskDefs || processAllTaskDefs.length <= 0)) {
    // This Lambda is useless, so trigger an exception to put all records back until it can be fixed!
    const errMsg = `FATAL - There must be at least one task definition in either of processOneTasks or processAllTasks! Fix your Lambda and redeploy ASAP, since this issue is blocking all of your stream's shards!`;
    context.error(errMsg);
    throw new Error(errMsg);
  }

  // Check that all task definitions across both processOneTasks and processOneTasks are distinct (i.e. any given task
  // definition must NOT appear in both lists)
  const allTaskDefs = (processOneTaskDefs ? processOneTaskDefs : []).concat(processAllTaskDefs ? processAllTaskDefs : []);
  if (!Arrays.isDistinct(allTaskDefs)) {
    // This Lambda is useless, so trigger an exception to put all records back until it can be fixed!
    const errMsg = `FATAL - Any given task definition must NOT exist in both processOneTasks and processAllTasks! Fix your Lambda and redeploy ASAP, since this issue is blocking all of your stream's shards!`;
    context.error(errMsg);
    throw new Error(errMsg);
  }
}

function logStreamEvent(event, prefix, asError, context) {
  try {
    const text = (prefix ? prefix + " - " : "") + JSON.stringify(event, null, 2);
    if (asError)
      context.error(text);
    else
      context.debug(text);
  } catch (err) {
    context.error(`Failed to log stream event`, err.stack);
  }
}

/**
 * Extracts a message from each of the given stream event records and then starts execution of all the tasks defined by
 * the given processOne task definitions against each message. When the entire batch of messages has been extracted from
 * all of the event's records, then starts execution of all the tasks defined by the given processAll task definitions
 * against the entire batch of messages. Finally returns an array containing: an array of zero or more successfully
 * extracted message objects; a second array of zero or more processOne task promises; a third array of zero or more
 * processAll task promises; and a fourth array of zero or more unusable, unparseable records.
 *
 * @param {Array.<Object>} records - an AWS stream event's records
 * @param {TaskDef[]} processOneTaskDefs - a list of zero or more "processOne" task definitions that will be used to
 * generate the tasks to be executed on each successfully extracted message independently
 * @param {TaskDef[]} processAllTaskDefs - a list of zero or more "processAll" task definitions that will be used to
 * generate the tasks to be executed on all of the event's messages collectively
 * @param {Object} context - the context
 * @return {[Object[],Promise,Promise,Object[]]} an array containing: an array of zero or more successfully extracted
 * message objects; a promise that will complete when all of the processOne task promises complete (if any); a promise
 * that will complete when all of the processAll task promises complete (if any); and an array of zero or more unusable,
 * unparseable records.
 */
function processStreamEventRecords(records, processOneTaskDefs, processAllTaskDefs, context) {
  if (!records || records.length <= 0) {
    context.error(`Stream event records required`);
    // Records not provided, so just return an empty array of empty arrays
    return [[], Promise.resolve([]), Promise.resolve([]), []];
  }

  // Convert all of the Kinesis event's records back into their original message object forms and kick off all
  // processOne tasks on each message extracted
  let msgAndPromiseOrUnusableList = records.map(record => processStreamEventRecord(record, processOneTaskDefs, context));

  // Collect all of the defined (successfully extracted) messages
  const messages = msgAndPromiseOrUnusableList.map(msgAndPromiseOrUnusable => msgAndPromiseOrUnusable[0])
    .filter(m => !!m);

  // Collect all of the defined processOne tasks promises
  const processOneTasksPromises = msgAndPromiseOrUnusableList.map(msgAndPromiseOrUnusable => msgAndPromiseOrUnusable[1])
    .filter(ps => !!ps);

  // Create a single promise that will wait for all of the processOne tasks' promises to complete
  const processOneTasksPromise = processOneTasksPromises.length > 0 ?
    Promise.all(processOneTasksPromises) : Promise.resolve([]);

  // Collect all of the defined unusable records
  const unusableRecords = msgAndPromiseOrUnusableList.map(msgAndPromiseOrUnusable => msgAndPromiseOrUnusable[2])
    .filter(ur => !!ur);

  // Start execution of all of the processAll tasks on the entire batch of messages extracted from the event's records
  const processAllTasksPromiseOrUndefined = executeProcessAllTasks(messages, processAllTaskDefs, context);

  const processAllTasksPromise = processAllTasksPromiseOrUndefined ?
    processAllTasksPromiseOrUndefined : Promise.resolve([]);

  return [messages, processOneTasksPromise, processAllTasksPromise, unusableRecords];
}


/**
 * First attempts to extract the original message object from the given stream event record and, if successful, creates
 * and starts tasks for the extracted message from the given processOneTaskDefs, each of which must have an execute
 * method that accepts a message and the given context as arguments and ideally returns a Promise.
 *
 * When this is done, returns an array containing: firstly the message object (if successfully extracted) or undefined
 * (if not); secondly a promise that will complete when all of the message's processOne tasks promises have completed
 * (or undefined if none); and lastly the unusable record (if unparseable) or undefined (if not).
 *
 * Any and all errors encountered along the way are logged, but no errors are allowed to escape from this function.
 *
 * An empty array of processOneTaskDefs implies that the caller does not need to process each message independently and
 * it will simply be skipped and its result returned as undefined.
 *
 * @param {Object} record - an AWS stream event record
 * @param {TaskDef[]} processOneTaskDefs - a list of zero or more "processOne" task definitions that will be used to
 * generate the tasks to be executed on each successfully extracted message independently
 * @param {Object} context - the context
 * @return {Array.<Object|undefined>} an array containing: the parsed message or undefined; a promise that will complete
 * when all of the message's processOne tasks promises have completed (or undefined if none); and the unusable record
 * (or undefined if none)
 */
function processStreamEventRecord(record, processOneTaskDefs, context) {
  try {
    if (streamProcessing.isKinesisStreamType(context)) {
      streamEvents.validateKinesisStreamEventRecord(record);
    } else if (streamProcessing.isDynamoDBStreamType(context)) {
      streamEvents.validateDynamoDBStreamEventRecord(record);
    } else {
      streamEvents.validateStreamEventRecord(record);
    }
  } catch (err) {
    context.error(err.message, err.stack);
    // Record is not a valid Kinesis or DynamoDB stream event record, so return no message, no promises and the unusable record
    return [undefined, undefined, record];
  }

  // Extract a message from the stream event record
  const message = extractMessageFromStreamEventRecord(record, context);

  // Check whether we successfully extracted a message or not
  if (!message) {
    // The record was unparseable, so return no message, no promises and the unusable record (to be discarded)
    return [undefined, undefined, record];
  }

  // Give the message a link to the record it came from
  setRecord(message, record, context);

  // Execute all of the incomplete processOne tasks on the given message
  const processOneTasksPromise = executeProcessOneTasks(message, processOneTaskDefs, context);

  return [message, processOneTasksPromise, undefined];
}

/**
 * Attempts to extract the message from the given stream event record using the configured extractMessageFromRecord
 * function on the given context, which was configured by {@linkcode configureStreamProcessing}. Logs any errors
 * encountered and the returns the extracted message (if defined and successfully extracted); otherwise undefined.
 *
 * @param {Object} record - the stream event record from which to extract a message
 * @param {Object} context - the context
 * @returns {Object|undefined} - the extracted message (if successful); otherwise undefined
 */
function extractMessageFromStreamEventRecord(record, context) {
  // Get the configured extractMessageFromRecord function to be used to do the actual extraction
  const extractMessageFromRecord = streamProcessing.getExtractMessageFromRecordFunction(context);

  if (extractMessageFromRecord) {
    try {
      const msg = extractMessageFromRecord(record, context);
      if (!msg || typeof msg !== 'object') {
        const fnName = isNotBlank(extractMessageFromRecord.name) ? extractMessageFromRecord.name : 'extract message from record';
        context.error(`The message extracted from stream event record using the configured ${fnName} function MUST be an object (${stringify(msg)}) - discarding record (${stringify(record)})`);
        return undefined;
      }
      return msg;

    } catch (err) {
      // NB: Do NOT throw an error in this case, since an un-parsable record will most likely remain an un-parsable record
      // forever and throwing an error would result in an "infinite" loop back to Kinesis until the record eventually expires
      const fnName = isNotBlank(extractMessageFromRecord.name) ? extractMessageFromRecord.name : 'extractMessageFromRecord';
      context.error(`Failed to extract message from record using the configured ${fnName} function - error (${stringify(err)} - discarding record (${stringify(record)})`, err.stack);
      return undefined;
    }
  } else {
    context.error(`Cannot extract a message from record without a valid, configured extractMessageFromRecord function - discarding record (${stringify(record)})`);
    return undefined;
  }
}

function getProcessOneTasksByName(message, context) {
  const taskTracking = getTaskTracking(message, context);
  if (!taskTracking.ones) {
    taskTracking.ones = {};
  }
  return taskTracking.ones;
}

function getProcessAllTasksByName(messageOrMessages, context) {
  const taskTracking = getTaskTracking(messageOrMessages, context);
  if (!taskTracking.alls) {
    taskTracking.alls = {};
  }
  return taskTracking.alls;
}

function getProcessOneTask(message, taskName, context) {
  const tasksByName = getProcessOneTasksByName(message, context);
  return taskUtils.getTask(tasksByName, taskName);
}

function getProcessAllTask(messageOrMessages, taskName, context) {
  const tasksByName = getProcessAllTasksByName(messageOrMessages, context);
  return taskUtils.getTask(tasksByName, taskName);
}

function setRecord(message, record, context) {
  const taskTracking = getTaskTracking(message, context);
  Object.defineProperty(taskTracking, 'record', {value: record, writable: true, configurable: true, enumerable: false});
}

function getTaskTracking(target, context) {
  const taskTrackingName = context.streamProcessing.taskTrackingName;
  let taskTracking = target[taskTrackingName];
  if (!taskTracking) {
    taskTracking = {};
    if (Array.isArray(target)) {
      Object.defineProperty(target, taskTrackingName, {value: taskTracking, writable: true, enumerable: false});
    } else {
      target[taskTrackingName] = taskTracking;
    }
  }
  return taskTracking;
}

/**
 * First replaces all of the old processOne task-like objects on the given message with new tasks created from the given
 * active processOne task definitions and updates these new tasks with status-related information from the old ones (if
 * any); and then starts execution of all of the incomplete processOne tasks on the message and collects their promises.
 *
 * @param {Object} message - the message to be processed
 * @param {TaskDef[]} processOneTaskDefs - a list of zero or more "processOne" task definitions that will be used to
 * generate the tasks to be executed independently on the given message
 * @param {Object} context - the context
 * @returns {Promise|undefined} a promise that will complete with all of the messages's processOne tasks' results when all of
 * these tasks' promises (if any) have completed
 */
function executeProcessOneTasks(message, processOneTaskDefs, context) {
  if (!message) {
    return undefined;
  }
  // Replaces all of the old processOne task-like objects with new tasks created from the given processOneTaskDefs and
  // updates these new tasks with information from the old ones
  const processOneTasksByName = getProcessOneTasksByName(message, context);
  const newTasksAndAbandonedTasks = taskUtils.replaceTasksWithNewTasksUpdatedFromOld(processOneTasksByName, processOneTaskDefs);
  const newTasks = newTasksAndAbandonedTasks[0];
  //const abandonedTasks = newTasksAndAbandonedTasks[1];

  const incompleteTasks = newTasks.filter(task => !task.isFullyFinalised());

  // Check whether the caller provided processOneTaskDefs to use or not
  if (processOneTaskDefs.length <= 0 || incompleteTasks.length <= 0) {
    // Either we don't have any incomplete messages left to process or the caller did not provide any processOneTaskDefs,
    // In the latter case, we assume that the caller did not need anything to be executed independently per message
    return undefined;
  }
  // Start executing each of the new processOneTasks on the message and collect their promises
  const processOneTasksPromises = incompleteTasks.map(task => executeProcessOneTask(task, message, context)).filter(p => !!p);

  return processOneTasksPromises.length > 0 ? Promise.all(processOneTasksPromises) : undefined;
}

/**
 * Starts asynchronous execution of the given processOne task's execute function with the given message and context as
 * arguments and returns a promise to return the task's result (if successful) or undefined (if not).
 *
 * Any and all errors encountered along the way are logged, but no errors are allowed to escape from this function.
 *
 * @param {Task} task - the processOne task to be executed (or none)
 * @param {Object} message - the message to pass as the first argument to the given task's execute function
 * @param {Object} context - the context to pass as the second argument to the given task's execute function
 * @return {Promise|undefined} a promise to return the task's result (if successful) or undefined (if not)
 */
function executeProcessOneTask(task, message, context) {
  if (!task || !message) {
    // Caller did not need/provide a task or no message, so nothing more to do
    return undefined;
  }
  // In preparation for the processing that is about to begin, reset all of the message's non-completed processMessage
  // tasks' states to Unstarted and clear the message's non-completed tasks' results
  task.reset();

  // Execute the given task and convert its result into a Promise of a result (if its not already one)
  // let promise = undefined;
  const startMs = Date.now();
  // try {
  //   // Asynchronously kick off independent processing of the task on the message
  //   const result = task.execute(message, context);
  //   promise = Promise.allOrOne(result);
  // } catch (err) {
  //   promise = Promise.reject(err);
  // }

  // Asynchronously kick off independent processing of the task on the message
  return task.execute(message, context).then(
    result => {
      context.info(`Task (${task.name}) success took ${Date.now() - startMs} ms`);
      context.trace(`Finished executing task (${task.name}) - state (${stringify(task.state)}) on message (${stringify(message)})`);
      return result;
    },
    err => {
      context.info(`Task (${task.name}) failure took ${Date.now() - startMs} ms`);
      context.error(`Failed to execute task (${task.name}) - state (${stringify(task.state)}) on message - error (${stringify(err)})`, err.stack);
      return undefined;
    });
}

/**
 * First replaces all of the old processAll task-like objects on the given messages with new tasks created from the
 * given active processAll task definitions and updates these new tasks with status-related information from the old
 * ones (if any); and then starts execution of EACH of the processAll tasks on the subset of messages for which a
 * particular task is still incomplete and collects their promises.
 *
 * @param {Object} messages - the entire batch of messages to be processed
 * @param {TaskDef[]} processAllTaskDefs - a list of zero or more "processAll" task definitions that will be used to
 * generate the tasks to be executed on all of the event's messages collectively
 * @param {Object} context - the context
 * @returns {Promise|undefined} a promise that will complete with all of the processAll tasks' results when these tasks' promises
 * (if any) have completed
 */
function executeProcessAllTasks(messages, processAllTaskDefs, context) {
  if (!messages || messages.length <= 0) {
    return undefined;
  }
  // Replaces all of the old processAll task-like objects on each message with new tasks created from the given
  // processAllTaskDefs and updates these new tasks with information from the old ones
  const messagesWithIncompleteTasks = messages.map(message => {
    const processAllTasksByName = getProcessAllTasksByName(message, context);
    const newTasksAndAbandonedTasks = taskUtils.replaceTasksWithNewTasksUpdatedFromOld(processAllTasksByName, processAllTaskDefs);
    const newTasks = newTasksAndAbandonedTasks[0];
    //const abandonedTasks = newTasksAndAbandonedTasks[1];

    const incompleteTasks = newTasks.filter(task => !task.isFullyFinalised());
    return [message, incompleteTasks];
  });

  // Check whether the caller provided processAllTaskDefs to use or not
  if (processAllTaskDefs.length <= 0) {
    return undefined;
  }

  // Create task tracking on the given messages array to keep track of the master tasks
  const masterTasksByName = getProcessAllTasksByName(messages, context);

  // Start executing each of the defined processAll tasks on all of the messages for which a particular processAll task
  // is still incomplete
  const promises = processAllTaskDefs.map(taskDef => {
    const taskName = taskDef.name;

    // Collect all of the messages that still have the current task in their list of incomplete tasks
    const incompleteMessages = messagesWithIncompleteTasks.map(messageAndIncompleteTasks => {
      const message = messageAndIncompleteTasks[0];
      const incompleteTasks = messageAndIncompleteTasks[1];
      // Check if the current message has an incomplete task with the current task definition's task name
      const incompleteTask = incompleteTasks.find(t => t.name === taskDef.name);
      return incompleteTask ? message : undefined;
    }).filter(m => !!m); // eliminate all of the undefined messages, which do NOT have the current task as an incomplete task

    // If there are any incomplete messages that still need to be processed with the current task definition, then
    // create a task and start executing it with the non-empty list of incomplete messages
    if (incompleteMessages.length > 0) {
      // Collect all the "slave" tasks with the current task name from the incomplete messages
      const slaveTasks = incompleteMessages.map(message => {
        const processAllTasksByName = getProcessAllTasksByName(message, context);
        return taskUtils.getTask(processAllTasksByName, taskName);
      });

      // Create a master task over all of the incomplete messages' slave tasks
      const masterTask = Task.createMasterTask(taskDef, slaveTasks);
      taskUtils.setTask(masterTasksByName, masterTask.name, masterTask);

      // Execute the master task, which will trigger subsequent state updates to its slave tasks
      return executeProcessAllTask(masterTask, incompleteMessages, context);
    } else {
      // The current task has been completed on all of the messages, so return its promise as undefined
      if (context.debugEnabled) context.debug(`Skipping execution of fully finalised task (${taskName})${taskDef.subTaskDefs.length > 0 ? ` and its sub-tasks ${stringify(taskDef.subTaskDefs.map(d => d.name))}` : ''} with no incomplete messages out of all ${messages.length} messages`);
      return undefined;
    }
  }).filter(p => !!p); // drop all undefined promises, which were tasks with no incomplete messages

  return promises.length > 0 ? Promise.all(promises) : undefined;
}

/**
 * Starts asynchronous execution of the given processAll task's execute function with the given messages and context as
 * arguments and returns a promise to return the task's result (if successful) or undefined (if not).
 *
 * Any and all errors encountered along the way are logged, but no errors are allowed to escape from this function.
 *
 * @param {Task} task - the processAll task to be executed (or none)
 * @param {Object[]} messages - the messages to pass as the first argument to the given task's execute function
 * @param {Object} context - the context to pass as the second argument to the given task's execute function
 * @return {Promise} a promise to return the task's result (if successful) or undefined (if not)
 */
function executeProcessAllTask(task, messages, context) {
  if (!task || !messages || messages.length <= 0) {
    // Caller did not need/provide a task or no messages, so nothing more to do
    return undefined;
  }
  // In preparation for the processing that is about to begin, reset all of the message's non-finalised processMessage
  // tasks' states to Unstarted and clear the message's non-finalised tasks' results
  task.reset();

  // Execute the given task and convert its result into a Promise of a result (if its not already one)
  // let promise = undefined;
  const startMs = Date.now();
  // try {
  //   // Asynchronously kick off independent processing of the task on the messages
  //   const result = task.execute(messages, context);
  //   promise = Promise.allOrOne(result);
  // } catch (err) {
  //   promise = Promise.reject(err);
  // }

  // Asynchronously kick off independent processing of the task on the messages
  return task.execute(messages, context).then(
    result => {
      context.info(`Task (${task.name}) success took ${Date.now() - startMs} ms`);
      context.trace(`Finished executing task (${task.name}) - state (${stringify(task.state)}) on messages (${stringify(messages)})`);
      return result;
    },
    err => {
      context.info(`Task ${task.name} failure took ${Date.now() - startMs} ms`);
      context.error(`Failed to execute task (${task.name}) - state (${stringify(task.state)}) on ${messages.length} messages - error (${stringify(err)})`, err.stack);
      return undefined;
    });
}

/**
 * An override task execute factory function that on invocation will return a task execute function that wraps the given
 * task's original execute function and supplements and alters its execution behaviour as follows:
 * - If the task is already fully finalised, then does nothing (other than logging a warning); otherwise:
 *   - First increments the number of attempts on the task (and recursively on all of its sub-tasks).
 *   - Next executes the task's actual, original execute function on the task and then based on the outcome:
 *     - If the execute function completes successfully, updates the task's result with the result obtained and also
 *       sets its state to Succeeded, but ONLY if the task is still in an Unstarted state.
 *     - If the execute function throws an exception or returns a rejected promise, sets its state to Failed with the
 *       error encountered, but ONLY if the task is not already in a rejected or failed state.
 *     - Returns the result wrapped in a Promise.
 *
 * @param {Task} task - the task to be executed
 * @param {Function} execute - the task's original execute function (provided by its task definition)
 * @returns {Function} a wrapper execute function, which will invoke the task's original execute function
 */
function taskExecutePromiseFactory(task, execute) {
  /**
   * Returns an execute function that must accept either one message or all messages as its first argument and a context
   * as its second argument, will execute the task's original execute function and return a resolved or rejected Promise
   * containing the result obtained or error encountered.
   * @returns {Promise.<*>} a promise containing the result obtained or error encountered
   */
  function executeUpdateStateAndReturnPromise() {
    // Use the context in the second argument for logging if available
    const context = arguments.length > 1 ? arguments[1] : undefined;
    const logger = context ? context : console;

    if (!task.isFullyFinalised()) {
      // First increment the number of attempts on this task (and all of its sub-tasks recursively), since its starting
      // to execute
      task.incrementAttempts(true);
      task.updateLastExecutedAt(new Date(), true);

      // Then execute the actual execute function
      try {
        // Execute the task's function
        const result = execute.apply(task, arguments);

        // If the result is a promise or array of promises or a non-promise, reduce it to a single promise
        const promise = Promise.allOrOne(result);

        return promise
          .then(
            result => {
              // If this task is still in an unstarted state after its execute function completes then complete it
              Task.completeTaskIfStillUnstarted(task, result, logger);
              return result;
            },
            err => {
              // If this task is not already in a failed or rejected state after its execute function fails then fail it
              Task.failTaskIfNotRejectedNorFailed(task, err, logger);
              return Promise.reject(err);
            }
          );
      } catch (err) {
        // If this task is not already in a failed or rejected state after its execute function fails then fail it
        Task.failTaskIfNotRejectedNorFailed(task, err, logger);
        return Promise.reject(err);
      }
    } else {
      logger.warn(`Attempted to execute a fully finalised task (${task.name}) - ${stringify(task)}`);
      return task.completed ? Promise.allOrOne(task.result) : Promise.resolve(undefined);
    }
  }

  return executeUpdateStateAndReturnPromise;
}
// Replace the default task execute factory with the above factory
if (Task.taskExecuteFactory === Task.defaultTaskExecuteFactory) {
  Task.taskExecuteFactory = taskExecutePromiseFactory;
}

/**
 * Creates a promise that will timeout when the configured percentage or 90% (if not configured) of the remaining time
 * in millis is reached, which will give us hopefully enough time to finalise at least some of our message processing
 * before we run out of time to complete everything in this invocation.
 *
 * @param {Object|undefined|null} [cancellable] - an arbitrary object onto which a cancelTimeout method will be installed
 * @param {Object} context - the context
 * @param {Object} context.awsContext - the context's AWS context
 * @return {Promise.<boolean>} a promise to return true if the timeout is triggered or false if not
 */
function createTimeoutPromise(cancellable, context) {
  const remainingTimeInMillis = context.awsContext.getRemainingTimeInMillis();

  // Resolve the configured percentage of remaining time at which to timeout or use the default
  const timeoutAtPercentageOfRemainingTime = context.streamProcessing.timeoutAtPercentageOfRemainingTime;

  let timeoutMs = Number.parseInt(remainingTimeInMillis * timeoutAtPercentageOfRemainingTime);
  return Promise.delay(timeoutMs, cancellable)
    .then(() => {
      context.streamConsumer.processingTimedOut = true;
      context.info('Premature timeout kicking in ...');
      return true;
    })
    .catch(err => {
      // timeout was cancelled externally, so return normally
      context.streamConsumer.processingTimedOut = false;
      return false;
    });
}

/**
 * Build a completed promise that will only continue once the given processedPromise and discardUnusableRecordsPromise
 * promises have complete.
 * @param {Promise} processedPromise - the promise that all processing tasks have completed
 * @param {Object[]} messages - the messages being processed
 * @param {Object} cancellable - a cancellable object that enables cancellation of the timeout promise on completion
 * @param {Object} context - the context
 * @returns {Promise.<Object[]>} a promise to return the messages when processing finishes successfully
 */
function createCompletedPromise(processedPromise, messages, cancellable, context) {
  context.streamConsumer.processingCompleted = false;
  context.streamConsumer.processingFailed = false;
  return processedPromise
    .then(results => {
      const timedOut = cancellable.cancelTimeout();
      const m = messages.length;
      const ms = m !== 1 ? 's' : '';
      if (timedOut) {
        context.warn(`Timed out before finished processing ${m} message${ms}`);
      } else {
        context.info(`Processed ${m} message${ms}`);
        context.streamConsumer.processingCompleted = true;
      }
      return messages;
    })
    .catch(err => {
      const timedOut = cancellable.cancelTimeout();
      const m = messages.length;
      const ms = m !== 1 ? 's' : '';
      if (timedOut) {
        context.warn(`Timed out before failed to finish processing ${m} message${ms} - error (${err})`, err.stack);
      } else {
        context.info(`Failed to finish processing ${m} message${ms} - error (${err})`, err.stack);
        context.streamConsumer.processingFailed = true;
      }
      throw err;
    });
}

/**
 * Attempts to discard all of the given unusable records using the configured discardUnusableRecords function (see
 * {@linkcode stream-processing-config#configureStreamProcessing}).
 * @param {Object[]} unusableRecords - the list of unusable records
 * @param {Object[]} records - the list of all records
 * @param {Object} context - the context
 * @returns {Promise.<*>} a promise that will complete when the configured discardUnusableRecords function completes
 */
function discardAnyUnusableRecords(unusableRecords, records, context) {
  const r = records.length;
  const rs = `${r} record${r !== 1 ? 's' : ''}`;
  const u = unusableRecords.length;

  if (u <= 0) {
    // No records need to be discarded
    context.info(`No unusable records to discard out of ${rs}`);
    return Promise.resolve([]);
  }
  const usOfRs = `${u} unusable record${u !== 1 ? 's' : ''} of ${rs}`;

  // Get the configured discardUnusableRecords function to be used to do the actual discarding
  const discardUnusableRecords = streamProcessing.getDiscardUnusableRecordsFunction(context);

  if (discardUnusableRecords) {
    // Trigger the configured discardUnusableRecords function to do the actual discarding
    return Promise.try(() => Promise.allOrOne(discardUnusableRecords(unusableRecords, context)))
      .then(results => {
        context.info(`Discarded ${usOfRs} - results (${stringify(results)}`);
        return unusableRecords;
      })
      .catch(err => {
        // If discard fails, then no choice left, but to throw an exception back to Lambda to force a replay of the batch of records (BAD!) :(
        const fnName = isNotBlank(discardUnusableRecords.name) ? discardUnusableRecords.name : 'discardUnusableRecords';
        context.error(`Failed to discard ${usOfRs} using the configured ${fnName} function - error (${stringify(err)}) - forced to trigger a replay`, err.stack);
        throw err;
      });
  } else {
    const errMsg = `Cannot discard ${usOfRs} without a valid, configured discardUnusableRecords function - forced to trigger a replay!`;
    context.error(errMsg);
    return Promise.reject(new Error(errMsg));
  }
}

/**
 * Freezes all of the given messages' tasks to prevent any further changes to its tasks (e.g. from the other promise
 * that lost the timeout race).
 * @param {Object[]} messages - all of the messages
 * @param {Object} context - the context to use
 */
function freezeAllTasks(messages, context) {
  context.debug(`FREEZING all tasks on ${messages.length} message(s)`);

  // First freeze all master tasks (if any)
  const processAllMasterTasksByName = getProcessAllTasksByName(messages, context);
  const masterTasks = taskUtils.getTasks(processAllMasterTasksByName);
  masterTasks.forEach(task => task.freeze());

  messages.forEach(m => {
    // Freeze all processOne tasks
    const processOneTasksByName = getProcessOneTasksByName(m, context);
    const processOneTasks = taskUtils.getTasks(processOneTasksByName);
    processOneTasks.forEach(task => task.freeze());

    // Freeze all processAll tasks if not already frozen via master tasks
    const processAllTasksByName = getProcessAllTasksByName(m, context);
    const processAllTasks = taskUtils.getTasks(processAllTasksByName);
    processAllTasks.forEach(task => {
      if (!task.isFrozen()) {
        task.freeze()
      }
    });
  });
}

/**
 * Attempts to finalise message processing (either after all processing completed successfully or after the configured
 * timeout expired to indicate this Lambda is running out of time) by first marking messages' incomplete tasks that have
 * exceeded the allowed number of attempts as discarded; then freezing all messages' tasks and then by handling all
 * still incomplete messages and discarding any rejected messages using the configured functions for both (see
 * {@linkcode stream-processing-config#configureStreamProcessing})
 * @param {Object[]} messages - the messages to be finalised (if any)
 * @param {Object[]} unusableRecords - the unusable records encountered (if any)
 * @param {Promise} discardUnusableRecordsPromise - the promise that all unusable records have been discarded
 * @param {Object} context - the context
 * @returns {Promise.<StreamProcessingResults|StreamProcessingError>} a resolved promise with the full stream processing
 * results or a rejected promise with an error with partial stream processing results
 */
function finaliseMessageProcessing(messages, unusableRecords, discardUnusableRecordsPromise, context) {
  // Discard incomplete tasks on each message if ALL of the message's tasks have exceeded the maximum number of allowed attempts
  messages.forEach(message => discardIncompleteTasksIfMaxAttemptsExceeded(message, context));

  // Freeze all of the messages' tasks to prevent any further changes from the other promise that lost the timeout race
  freezeAllTasks(messages, context);

  // Save the task tracking states of all of the messages
  //TODO
  const saveMessagesTaskTrackingStatePromise = saveAllMessagesTaskTrackingState(messages, context);

  // Resubmit any still incomplete messages
  const handleIncompleteMessagesPromise = handleAnyIncompleteMessages(messages, context);

  // Discard any finalised messages that contain at least one rejected task
  const discardRejectedMessagesPromise = discardAnyRejectedMessages(messages, context);

  const streamProcessingResults = {
    messages: messages,
    unusableRecords: unusableRecords,
    processingCompleted: isProcessingCompleted(context),
    processingFailed: isProcessingFailed(context),
    processingTimedOut: isProcessingTimedOut(context)
  };

  return Promise.all([discardUnusableRecordsPromise, handleIncompleteMessagesPromise, discardRejectedMessagesPromise])
    .then(results => {
      const discardedUnusableRecords = results[0];
      const handledIncompleteMessages = results[1];
      const discardedRejectedMessages = results[2];

      streamProcessingResults.handledIncompleteMessages = handledIncompleteMessages;
      streamProcessingResults.discardedUnusableRecords = discardedUnusableRecords;
      streamProcessingResults.discardedRejectedMessages = discardedRejectedMessages;

      return streamProcessingResults;
    })
    .catch(err => {
      err.streamProcessingPartialResults = streamProcessingResults;
      streamProcessingResults.handleIncompleteMessagesPromise = handleIncompleteMessagesPromise;
      streamProcessingResults.discardUnusableRecordsPromise = discardUnusableRecordsPromise;
      streamProcessingResults.discardRejectedMessagesPromise = discardRejectedMessagesPromise;

      return Promise.reject(err);
    });
}

/**
 * If the given error has stream processing partial results, then returns a promise that will wait for all of the stream
 * processing partial results' promises to complete and then return the finalised results; otherwise just returns a
 * promise that will return undefined
 * @param {Error} error - the error with which stream processing ended
 * @param {StreamProcessingPartialResults} [error.streamProcessingPartialResults] - the optional stream processing
 * partial results attached to the error
 * @returns {Promise.<StreamProcessingResults|undefined>} a promise of the finalised stream processing results (if any)
 * or undefined (if none)
 */
function awaitStreamProcessingPartialResults(error) {
  if (error.streamProcessingPartialResults) {
    const partialResults = error.streamProcessingPartialResults;
    const results = {
      messages: partialResults.messages,
      unusableRecords: partialResults.unusableRecords,
      processingCompleted: partialResults.processingCompleted,
      processingFailed: partialResults.processingFailed,
      processingTimedOut: partialResults.processingTimedOut
    };

    return Promise.every(partialResults.handleIncompleteMessagesPromise, partialResults.discardUnusableRecordsPromise, partialResults.discardRejectedMessagesPromise)
      .then(resultsOrErrors => {
        results.discardedUnusableRecords = resultsOrErrors[0].result;
        results.discardUnusableRecordsError = resultsOrErrors[0].error;
        results.handledIncompleteMessages = resultsOrErrors[1].result;
        results.handleIncompleteMessagesError = resultsOrErrors[1].error;
        results.discardedRejectedMessages = resultsOrErrors[2].result;
        results.discardRejectedMessagesError = resultsOrErrors[2].error;
        return results;
      });

  } else {
    return Promise.resolve(undefined);
  }
}

/**
 * Awaits and then logs any partial stream processing results on the given error using the given context.
 * @param {Error} error - the error with which stream processing ended
 * @param {StreamProcessingPartialResults} [error.streamProcessingPartialResults] - the optional stream processing
 * partial results attached to the error
 * @param {Object} context - the context to use
 * @returns {Promise.<StreamProcessingResults|undefined>} a promise of the finalised stream processing results (if any)
 * or undefined (if none)
 */
function awaitAndLogStreamProcessingPartialResults(error, context) {
  return awaitStreamProcessingPartialResults(error).then(results => {
    context.log(results ? JSON.stringify(results) : 'No stream processing partial results available');
    return results;
  });
}

function isProcessingCompleted(context) {
  return context && context.streamConsumer && context.streamConsumer.processingCompleted;
}

function isProcessingFailed(context) {
  return context && context.streamConsumer && context.streamConsumer.processingFailed;
}

function isProcessingTimedOut(context) {
  return context && context.streamConsumer && context.streamConsumer.processingTimedOut;
}

function discardIncompleteTasksIfMaxAttemptsExceeded(message, context) {
  // Get the message's processOne tasks
  const processOneTasksByName = getProcessOneTasksByName(message, context);
  //const processOneTasks = taskUtils.getTasks(processOneTasksByName);

  // Get the message's processAll tasks
  const processAllTasksByName = getProcessAllTasksByName(message, context);
  //const processAllTasks = taskUtils.getTasks(processAllTasksByName);

  const allTasksAndSubTasks = taskUtils.getTasksAndSubTasks(processOneTasksByName)
    .concat(taskUtils.getTasksAndSubTasks(processAllTasksByName));

  // Check if the number of attempts at each of the messages's tasks have all exceeded the maximum number of attempts allowed
  const maxNumberOfAttempts = context.streamProcessing.maxNumberOfAttempts;

  const incompleteTasks = allTasksAndSubTasks.filter(t => !t.finalised);
  if (incompleteTasks.length <= 0) {
    return false;
  }
  const maxAttemptsExceeded = incompleteTasks.every(t => t.attempts >= maxNumberOfAttempts);
  if (maxAttemptsExceeded) {
    // Mark all of the incomplete tasks as discarded
    incompleteTasks.forEach(t => {
      const reason = `The number of attempts (${t.attempts}) has ${t.attempts > maxNumberOfAttempts ? 'exceeded' : 'reached'} the maximum number of attempts allowed (${maxNumberOfAttempts})`;
      t.discard(reason, undefined, false);
    });
  }
  return maxAttemptsExceeded;
}

function saveAllMessagesTaskTrackingState(messages, context) {
  const m = messages.length;
  const ms = `${m} message${m !== 1 ? 's' : ''}`;

  if (m <= 0) {
    context.info(`No task tracking state to save, since ${ms}!`);
    return Promise.resolve([]);
  }

  // Get the configured saveTaskTrackingState function to be used to do the actual saving
  const saveTaskTrackingState = streamProcessing.getSaveTaskTrackingStateFunction(context);

  if (saveTaskTrackingState) {
    // Trigger the configured saveMessagesTaskTrackingState function to do the actual saving
    return Promise.try(() => Promise.allOrOne(saveTaskTrackingState(messages, context)))
      .then(results => {
        context.info(`Saved task tracking state of ${ms} - results (${stringify(results)}`);
        return messages;
      })
      .catch(err => {
        const fnName = isNotBlank(saveTaskTrackingState.name) ? saveTaskTrackingState.name : 'saveMessagesTaskTrackingState';
        context.error(`Failed to save task tracking state of ${ms} using the configured ${fnName} function - error (${stringify(err)}`, err.stack);
        throw err;
      });
  } else {
    const errMsg = `Cannot save task tracking state of ${ms} without a valid, configured saveMessagesTaskTrackingState function!`;
    context.error(errMsg);
    return Promise.reject(new Error(errMsg));
  }
}

/**
 * First finds all of the incomplete messages in the given list of all messages being processed and then attempts to
 * handle all of these incomplete messages using the configured handleIncompleteMessages function (see {@linkcode
 * stream-processing-config#configureStreamProcessing}).
 * @param {Object[]} messages - all of the messages being processed
 * @param {Object} context - the context
 * @returns {Promise.<*>} a promise that will complete when the configured handleIncompleteMessages function completes
 */
function handleAnyIncompleteMessages(messages, context) {
  const m = messages.length;
  const ms = `${m} message${m !== 1 ? 's' : ''}`;

  if (m <= 0) {
    context.info(`No incomplete messages to handle, since ${ms}!`);
    return Promise.resolve([]);
  }
  // Collect all of the messages that have not been completed yet
  const incompleteMessages = messages.filter(m => isMessageIncomplete(m, context));

  const i = incompleteMessages.length;
  const is = `${i} incomplete message${i !== 1 ? 's' : ''}`;
  const isOfMs = `${is} of ${ms}`;

  if (i <= 0) {
    // All messages have completed, so nothing needs to be bounced back to Kinesis and we are finally done
    context.info(`No incomplete messages to handle out of ${ms}`);
    return Promise.resolve([]);
  }

  // Get the configured handleIncompleteMessages function to be used to do the actual handling of the incomplete messages
  const handleIncompleteMessages = streamProcessing.getHandleIncompleteMessagesFunction(context);

  if (handleIncompleteMessages) {
    // Trigger the configured handleIncompleteMessages function to do the actual handling of the incomplete messages
    return Promise.try(() => Promise.allOrOne(handleIncompleteMessages(messages, incompleteMessages, context)))
      .then(results => {
        context.info(`Handled ${isOfMs} - results (${stringify(results)}`);
        return incompleteMessages;
      })
      .catch(err => {
        // If handle fails, then no choice left, but to throw an exception back to Lambda to force a replay of the batch of messages (BAD!) :(
        const fnName = isNotBlank(handleIncompleteMessages.name) ? handleIncompleteMessages.name : 'handleIncompleteMessages';
        context.error(`Failed to handle ${isOfMs} using the configured ${fnName} function - error (${stringify(err)} - forced to trigger a replay`, err.stack);
        throw err;
      });
  } else {
    const errMsg = `FATAL - Cannot handle ${isOfMs} without a valid, configured handleIncompleteMessages function - forced to trigger a replay! Fix your Lambda ASAP!`;
    context.error(errMsg);
    return Promise.reject(new Error(errMsg));
  }
}

function isMessageIncomplete(message, context) {
  // Check all processOneTasks are fully finalised
  const processOneTasksByName = getProcessOneTasksByName(message, context);
  const processOneTasks = taskUtils.getTasks(processOneTasksByName);
  if (!processOneTasks.every(t => t.isFullyFinalised())) {
    return true;
  }
  // Check all processAllTasks are fully finalised
  const processAllTasksByName = getProcessAllTasksByName(message, context);
  const processAllTasks = taskUtils.getTasks(processAllTasksByName);
  return !processAllTasks.every(t => t.isFullyFinalised());
}

/**
 * First finds all of the finalised, but rejected messages in the given list of all messages being processed and then
 * attempts to discard all of these rejected messages using the configured discardRejectedMessages function (see
 * {@linkcode stream-processing-config#configureStreamProcessing}).
 * @param {Object[]} messages - all of the messages being processed
 * @param {Object} context - the context
 * @returns {Promise.<*>} a promise that will complete when the configured discardRejectedMessages function completes
 */
function discardAnyRejectedMessages(messages, context) {
  const m = messages.length;
  const ms = `${m} message${m !== 1 ? 's' : ''}`;

  if (m <= 0) {
    context.info(`No rejected messages to discard, since ${ms}!`);
    return Promise.resolve([]);
  }
  // Collect all messages that have been fully finalised, but also contain at least one rejected task
  const rejectedMessages = messages.filter(message => isMessageFinalisedButRejected(message, context));

  const r = rejectedMessages.length;
  const rs = `${r} rejected message${r !== 1 ? 's' : ''}`;
  const rsOfMs = `${rs} of ${ms}`;

  if (rejectedMessages.length <= 0) {
    // No messages need to be discarded
    context.info(`No rejected messages to discard out of ${ms}`);
    return Promise.resolve([]);
  }

  // Get the configured discardRejectedMessages function to be used to do the actual discarding
  const discardRejectedMessages = streamProcessing.getDiscardRejectedMessagesFunction(context);

  if (discardRejectedMessages) {
    // Trigger the configured discardRejectedMessages function to do the actual discarding
    return Promise.try(() => Promise.allOrOne(discardRejectedMessages(rejectedMessages, context)))
      .then(results => {
        context.info(`Discarded ${rsOfMs} - results (${stringify(results)}`);
        return rejectedMessages;
      })
      .catch(err => {
        // If discard fails, then no choice left, but to throw an exception back to Lambda to force a replay of the batch of messages (BAD!) :(
        const fnName = isNotBlank(discardRejectedMessages.name) ? discardRejectedMessages.name : 'discardRejectedMessages';
        context.error(`Failed to discard ${rsOfMs} using the configured ${fnName} function - error (${stringify(err)}) - forced to trigger a replay`, err.stack);
        throw err;
      });
  } else {
    const errMsg = `Cannot discard ${rsOfMs} without a valid, configured discardRejectedMessages function - forced to trigger a replay!`;
    context.error(errMsg);
    return Promise.reject(new Error(errMsg));
  }
}

function isMessageFinalisedButRejected(message, context) {
  // Get all of the message's processOneTasks
  const processOneTasksByName = getProcessOneTasksByName(message, context);
  // Get all of the message's processAllTasks
  const processAllTasksByName = getProcessAllTasksByName(message, context);

  const allTasksAndSubTasks = taskUtils.getTasksAndSubTasks(processOneTasksByName)
    .concat(taskUtils.getTasksAndSubTasks(processAllTasksByName));

  return allTasksAndSubTasks.every(t => t.finalised) && allTasksAndSubTasks.some(t => t.rejected);
}
