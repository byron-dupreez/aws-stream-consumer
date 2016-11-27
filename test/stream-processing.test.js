'use strict';

/**
 * Unit tests for aws-stream-consumer/stream-processing.js
 * @author Byron du Preez
 */

const test = require("tape");

// The test subject
const streamProcessing = require('../stream-processing');

const isStreamProcessingConfigured = streamProcessing.isStreamProcessingConfigured;

const configureStreamProcessing = streamProcessing.configureStreamProcessing;

const getDefaultKinesisStreamProcessingSettings = streamProcessing.getDefaultKinesisStreamProcessingSettings;
const configureDefaultKinesisStreamProcessing = streamProcessing.configureDefaultKinesisStreamProcessing;

const getDefaultDynamoDBStreamProcessingSettings = streamProcessing.getDefaultDynamoDBStreamProcessingSettings;
const configureDefaultDynamoDBStreamProcessing = streamProcessing.configureDefaultDynamoDBStreamProcessing;

const configureStreamProcessingWithSettings = streamProcessing.FOR_TESTING_ONLY.configureStreamProcessingWithSettings;

const getStreamProcessingSetting = streamProcessing.getStreamProcessingSetting;
const getStreamProcessingFunction = streamProcessing.getStreamProcessingFunction;

// Convenience accessors for specific stream processing functions
const getExtractMessageFromRecordFunction = streamProcessing.getExtractMessageFromRecordFunction;
const getLoadTaskTrackingStateFunction = streamProcessing.getLoadTaskTrackingStateFunction;
const getSaveTaskTrackingStateFunction = streamProcessing.getSaveTaskTrackingStateFunction;
const getDiscardUnusableRecordsFunction = streamProcessing.getDiscardUnusableRecordsFunction;
const getDiscardRejectedMessagesFunction = streamProcessing.getDiscardRejectedMessagesFunction;
const getHandleIncompleteMessagesFunction = streamProcessing.getHandleIncompleteMessagesFunction;

// Default extractMessageFromRecord functions
const extractJsonMessageFromKinesisRecord = streamProcessing.DEFAULTS.extractJsonMessageFromKinesisRecord;
const useStreamEventRecordAsMessage = streamProcessing.DEFAULTS.useStreamEventRecordAsMessage;

// Default loadTaskTrackingState functions
const skipLoadTaskTrackingState = streamProcessing.DEFAULTS.skipLoadTaskTrackingState;
const loadTaskTrackingStateFromDynamoDB = streamProcessing.DEFAULTS.loadTaskTrackingStateFromDynamoDB;

// Default saveTaskTrackingState functions
const skipSaveTaskTrackingState = streamProcessing.DEFAULTS.skipSaveTaskTrackingState;
const saveTaskTrackingStateToDynamoDB = streamProcessing.DEFAULTS.saveTaskTrackingStateToDynamoDB;

// Default handleIncompleteMessages functions
const resubmitIncompleteMessagesToKinesis = streamProcessing.DEFAULTS.resubmitIncompleteMessagesToKinesis;
const replayAllMessagesIfIncomplete = streamProcessing.DEFAULTS.replayAllMessagesIfIncomplete;

// Default discardUnusableRecords functions
const discardUnusableRecordsToDRQ = streamProcessing.DEFAULTS.discardUnusableRecordsToDRQ;
// Default discardRejectedMessages functions
const discardRejectedMessagesToDMQ = streamProcessing.DEFAULTS.discardRejectedMessagesToDMQ;

// Generic settings names
const STREAM_TYPE_SETTING = streamProcessing.STREAM_TYPE_SETTING;
const TASK_TRACKING_NAME_SETTING = streamProcessing.TASK_TRACKING_NAME_SETTING;
const TIMEOUT_AT_PERCENTAGE_OF_REMAINING_TIME_SETTING = streamProcessing.TIMEOUT_AT_PERCENTAGE_OF_REMAINING_TIME_SETTING;
const MAX_NUMBER_OF_ATTEMPTS_SETTING = streamProcessing.MAX_NUMBER_OF_ATTEMPTS_SETTING;

// Processing function settings names
const EXTRACT_MESSAGE_FROM_RECORD_SETTING = streamProcessing.EXTRACT_MESSAGE_FROM_RECORD_SETTING;
const LOAD_TASK_TRACKING_STATE_SETTING = streamProcessing.LOAD_TASK_TRACKING_STATE_SETTING;
const SAVE_TASK_TRACKING_STATE_SETTING = streamProcessing.SAVE_TASK_TRACKING_STATE_SETTING;
const HANDLE_INCOMPLETE_MESSAGES_SETTING = streamProcessing.HANDLE_INCOMPLETE_MESSAGES_SETTING;
const DISCARD_UNUSABLE_RECORDS_SETTING = streamProcessing.DISCARD_UNUSABLE_RECORDS_SETTING;
const DISCARD_REJECTED_MESSAGES_SETTING = streamProcessing.DISCARD_REJECTED_MESSAGES_SETTING;

// Specialised settings names
const TASK_TRACKING_TABLE_NAME_SETTING = streamProcessing.TASK_TRACKING_TABLE_NAME_SETTING;
const DEAD_RECORD_QUEUE_NAME_SETTING = streamProcessing.DEAD_RECORD_QUEUE_NAME_SETTING;
const DEAD_MESSAGE_QUEUE_NAME_SETTING = streamProcessing.DEAD_MESSAGE_QUEUE_NAME_SETTING;

// Valid stream types
const KINESIS_STREAM_TYPE = streamProcessing.KINESIS_STREAM_TYPE;
const DYNAMODB_STREAM_TYPE = streamProcessing.DYNAMODB_STREAM_TYPE;

// External dependencies
const logging = require("logging-utils");
const base64 = require("core-functions/base64");
const regions = require("aws-core-utils/regions");
const stages = require("aws-core-utils/stages");
const kinesisCache = require("aws-core-utils/kinesis-cache");
const dynamoDBDocClientCache = require("aws-core-utils/dynamodb-doc-client-cache");

// Testing dependencies
const testing = require("./testing");
// const okNotOk = testing.okNotOk;
// const checkOkNotOk = testing.checkOkNotOk;
// const checkMethodOkNotOk = testing.checkMethodOkNotOk;
const equal = testing.equal;
// const checkEqual = testing.checkEqual;
// const checkMethodEqual = testing.checkMethodEqual;

const samples = require("./samples");

function dummyKinesis(t, prefix, error) {
  return {
    putRecord(request) {
      return {
        promise() {
          return new Promise((resolve, reject) => {
            t.pass(`${prefix} simulated putRecord to Kinesis`);
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

function sampleMessage() {
  return {
    name: 'Sample Message',
    dob: new Date().toISOString(),
    num: 123,
    address: {
      lat: 123.456,
      lon: -67.890
    },
    tags: ['a', 'b']
  };
}

function toOptions(streamType, taskTrackingName, timeoutAtPercentageOfRemainingTime, maxNumberOfAttempts,
  taskTrackingTableName, deadRecordQueueName, deadMessageQueueName, kinesisOptions, dynamoDBDocClientOptions) {

  return {
    // generic settings
    streamType: streamType,
    taskTrackingName: taskTrackingName,
    timeoutAtPercentageOfRemainingTime: timeoutAtPercentageOfRemainingTime,
    maxNumberOfAttempts: maxNumberOfAttempts,
    // specialised settings needed by default implementations
    taskTrackingTableName: taskTrackingTableName,
    deadRecordQueueName: deadRecordQueueName,
    deadMessageQueueName: deadMessageQueueName,
    // Kinesis & DynamoDB DocumentClient constructor options
    kinesisOptions: kinesisOptions,
    dynamoDBDocClientOptions: dynamoDBDocClientOptions
  };
}


function toSettings(streamType, taskTrackingName, timeoutAtPercentageOfRemainingTime, maxNumberOfAttempts,
  extractMessageFromRecord, loadTaskTrackingState, saveTaskTrackingState, handleIncompleteMessages,
  discardUnusableRecords, discardRejectedMessages, taskTrackingTableName, deadRecordQueueName, deadMessageQueueName,
  kinesisOptions, dynamoDBDocClientOptions) {

  return {
    // generic settings
    streamType: streamType,
    taskTrackingName: taskTrackingName,
    timeoutAtPercentageOfRemainingTime: timeoutAtPercentageOfRemainingTime,
    maxNumberOfAttempts: maxNumberOfAttempts,
    // functions
    extractMessageFromRecord: extractMessageFromRecord,
    loadTaskTrackingState: loadTaskTrackingState,
    saveTaskTrackingState: saveTaskTrackingState,
    handleIncompleteMessages: handleIncompleteMessages,
    discardUnusableRecords: discardUnusableRecords,
    discardRejectedMessages: discardRejectedMessages,
    // specialised settings needed by default implementations
    taskTrackingTableName: taskTrackingTableName,
    deadRecordQueueName: deadRecordQueueName,
    deadMessageQueueName: deadMessageQueueName,
    // Kinesis & DynamoDB DocumentClient constructor options
    kinesisOptions: kinesisOptions,
    dynamoDBDocClientOptions: dynamoDBDocClientOptions
  };
}

function checkSettings(t, context, before, mustChange, expectedSettings) {
  t.ok(isStreamProcessingConfigured(context), `Default stream processing must be configured now`);

  const after = context.streamProcessing;
  equal(t, after.streamType, mustChange ? expectedSettings.streamType : before.streamType, 'streamType');
  equal(t, after.taskTrackingName, mustChange ? expectedSettings.taskTrackingName : before.taskTrackingName, 'taskTrackingName');
  equal(t, after.timeoutAtPercentageOfRemainingTime, mustChange ? expectedSettings.timeoutAtPercentageOfRemainingTime : before.timeoutAtPercentageOfRemainingTime, 'timeoutAtPercentageOfRemainingTime');
  equal(t, after.maxNumberOfAttempts, mustChange ? expectedSettings.maxNumberOfAttempts : before.maxNumberOfAttempts, 'maxNumberOfAttempts');
  equal(t, after.extractMessageFromRecord, mustChange ? expectedSettings.extractMessageFromRecord : before.extractMessageFromRecord, 'extractMessageFromRecord');
  equal(t, after.loadTaskTrackingState, mustChange ? expectedSettings.loadTaskTrackingState : before.loadTaskTrackingState, 'loadTaskTrackingState');
  equal(t, after.saveTaskTrackingState, mustChange ? expectedSettings.saveTaskTrackingState : before.saveTaskTrackingState, 'saveTaskTrackingState');
  equal(t, after.handleIncompleteMessages, mustChange ? expectedSettings.handleIncompleteMessages : before.handleIncompleteMessages, 'handleIncompleteMessages');
  equal(t, after.discardUnusableRecords, mustChange ? expectedSettings.discardUnusableRecords : before.discardUnusableRecords, 'discardUnusableRecords');
  equal(t, after.discardRejectedMessages, mustChange ? expectedSettings.discardRejectedMessages : before.discardRejectedMessages, 'discardRejectedMessages');
  equal(t, after.taskTrackingTableName, mustChange ? expectedSettings.taskTrackingTableName : before.taskTrackingTableName, 'taskTrackingTableName');
  equal(t, after.deadRecordQueueName, mustChange ? expectedSettings.deadRecordQueueName : before.deadRecordQueueName, 'deadRecordQueueName');
  equal(t, after.deadMessageQueueName, mustChange ? expectedSettings.deadMessageQueueName : before.deadMessageQueueName, 'deadMessageQueueName');
  equal(t, after.kinesisOptions, mustChange ? expectedSettings.kinesisOptions : before.kinesisOptions, 'kinesisOptions');
  equal(t, after.dynamoDBDocClientOptions, mustChange ? expectedSettings.dynamoDBDocClientOptions : before.dynamoDBDocClientOptions, 'dynamoDBDocClientOptions');

  // Check Kinesis instance is also configured
  const region = regions.getRegion();
  if (expectedSettings.kinesisOptions) {
    t.ok(context.kinesis, 'context.kinesis must be configured');
    equal(t, context.kinesis.config.region, region, 'context.kinesis.config.region');
    equal(t, context.kinesis.config.maxRetries, expectedSettings.kinesisOptions.maxRetries, 'context.kinesis.config.maxRetries');
  }
  // Check DynamoDB DocumentClient instance is also configured
  if (expectedSettings.dynamoDBDocClientOptions) {
    // Check DynamoDB.DocumentClient is also configured
    t.ok(context.dynamoDBDocClient, 'context.dynamoDBDocClient must be configured');
    equal(t, context.dynamoDBDocClient.service.config.region, region, 'context.dynamoDBDocClient.config.region');
    equal(t, context.dynamoDBDocClient.service.config.maxRetries, expectedSettings.dynamoDBDocClientOptions.maxRetries, 'context.dynamoDBDocClient.config.maxRetries');
  }
}

function deleteCachedInstances() {
  const region = regions.getRegion();
  kinesisCache.deleteKinesis(region);
  dynamoDBDocClientCache.deleteDynamoDBDocClient(region);
}

function checkConfigureStreamProcessingWithSettings(t, context, settings, otherSettings, otherOptions, forceConfiguration, expectedSettings) {
  const before = context.streamProcessing;
  const mustChange = forceConfiguration || !isStreamProcessingConfigured(context);

  const c = configureStreamProcessingWithSettings(context, settings, otherSettings, otherOptions, forceConfiguration);

  t.ok(c === context, `Context returned must be given context`);
  checkSettings(t, context, before, mustChange, expectedSettings);
}

function checkConfigureStreamProcessing(t, context, settings, options, otherSettings, otherOptions, forceConfiguration, expectedSettings) {
  const before = context.streamProcessing;
  const mustChange = forceConfiguration || !isStreamProcessingConfigured(context);

  const c = configureStreamProcessing(context, settings, options, otherSettings, otherOptions, forceConfiguration);

  t.ok(c === context, `Context returned must be given context`);
  checkSettings(t, context, before, mustChange, expectedSettings);
}

// =====================================================================================================================
// isStreamProcessingConfigured
// =====================================================================================================================

test('isStreamProcessingConfigured', t => {
  const context = {};
  t.notOk(isStreamProcessingConfigured(context), `Stream processing must NOT be configured yet`);

  configureDefaultKinesisStreamProcessing(context, undefined, undefined, undefined, true);

  t.ok(isStreamProcessingConfigured(context), `Stream processing must be configured now`);

  t.end();
});

// =====================================================================================================================
// configureStreamProcessingWithSettings
// =====================================================================================================================

test('configureStreamProcessingWithSettings', t => {
  // Set up region
  regions.ONLY_FOR_TESTING.setRegionIfNotSet('us-west-1');
  // Remove any cached entries before configuring
  deleteCachedInstances();

  function check(context, settings, otherSettings, otherOptions, forceConfiguration, expectedSettings) {
    return checkConfigureStreamProcessingWithSettings(t, context, settings, otherSettings, otherOptions, forceConfiguration, expectedSettings);
  }

  const context = {};

  t.notOk(isStreamProcessingConfigured(context), `Stream processing must NOT be configured yet`);

  // Dummy extract message from record functions
  const extractMessageFromRecord1 = (record, context) => record;
  const extractMessageFromRecord2 = (record, context) => record;

  const loadTaskTrackingState1 = (messages, context) => messages;
  const loadTaskTrackingState2 = (messages, context) => messages;

  const saveTaskTrackingState1 = (messages, context) => messages;
  const saveTaskTrackingState2 = (messages, context) => messages;

  const handleIncompleteMessages1 = (incompleteMessages, streamName, context) => incompleteMessages;
  const handleIncompleteMessages2 = (incompleteMessages, streamName, context) => incompleteMessages;

  const discardUnusableRecords1 = (unusableRecords, context) => unusableRecords;
  const discardUnusableRecords2 = (unusableRecords, context) => unusableRecords;

  const discardRejectedMessages1 = (rejectedMessages, context) => rejectedMessages;
  const discardRejectedMessages2 = (rejectedMessages, context) => rejectedMessages;

  // Configure for the first time
  const settings1 = toSettings(DYNAMODB_STREAM_TYPE, 'myTasks', 0.75, 2, extractMessageFromRecord1, loadTaskTrackingState1, saveTaskTrackingState1, handleIncompleteMessages1, discardUnusableRecords1, discardRejectedMessages1, 'taskTrackingTableName1', 'DRQ1', 'DMQ1', {}, {});
  check(context, settings1, undefined, require('../dynamodb-options.json'), false, settings1);

  // Don't force a different configuration
  const settings2 = toSettings(KINESIS_STREAM_TYPE, 'myTasks_NoOverride', 0.81, 77, extractMessageFromRecord2, loadTaskTrackingState2, saveTaskTrackingState2, handleIncompleteMessages2, discardUnusableRecords2, discardRejectedMessages2, 'taskTrackingTableName2', 'DRQ2', 'DMQ2', {}, undefined);
  check(context, settings2, undefined, require('../kinesis-options.json'), false, settings1);

  // Force a new configuration
  context.kinesis = undefined;
  context.dynamoDBDocClient = undefined;
  deleteCachedInstances();
  const settings3 = toSettings(KINESIS_STREAM_TYPE, 'myTasks_Override', 0.91, 7, extractMessageFromRecord2, loadTaskTrackingState2, saveTaskTrackingState2, handleIncompleteMessages2, discardUnusableRecords2, discardRejectedMessages2, 'taskTrackingTableName3', 'DRQ3', 'DMQ3', {}, undefined);
  check(context, settings3, undefined, require('../kinesis-options.json'), true, settings3);

  t.end();
});

// =====================================================================================================================
// configureStreamProcessing with settings
// =====================================================================================================================

test('configureStreamProcessing with settings', t => {
  // Set up region
  regions.ONLY_FOR_TESTING.setRegionIfNotSet('us-west-1');
  // Remove any cached entries before configuring
  deleteCachedInstances();

  function check(context, settings, options, otherSettings, otherOptions, forceConfiguration, expectedSettings) {
    return checkConfigureStreamProcessing(t, context, settings, options, otherSettings, otherOptions, forceConfiguration, expectedSettings);
  }

  const context = {};

  t.notOk(isStreamProcessingConfigured(context), `Stream processing must NOT be configured yet`);

  // Dummy extract message from record functions
  const extractMessageFromRecord1 = (record, context) => record;
  const extractMessageFromRecord2 = (record, context) => record;

  const loadTaskTrackingState1 = (messages, context) => messages;
  const loadTaskTrackingState2 = (messages, context) => messages;

  const saveTaskTrackingState1 = (messages, context) => messages;
  const saveTaskTrackingState2 = (messages, context) => messages;

  const handleIncompleteMessages1 = (incompleteMessages, streamName, context) => incompleteMessages;
  const handleIncompleteMessages2 = (incompleteMessages, streamName, context) => incompleteMessages;

  const discardUnusableRecords1 = (unusableRecords, context) => unusableRecords;
  const discardUnusableRecords2 = (unusableRecords, context) => unusableRecords;

  const discardRejectedMessages1 = (rejectedMessages, context) => rejectedMessages;
  const discardRejectedMessages2 = (rejectedMessages, context) => rejectedMessages;

  // Configure for the first time
  const settings1 = toSettings(DYNAMODB_STREAM_TYPE, 'myTasks', 0.75, 2, extractMessageFromRecord1, loadTaskTrackingState1, saveTaskTrackingState1, handleIncompleteMessages1, discardUnusableRecords1, discardRejectedMessages1, 'taskTrackingTableName1', 'DRQ1', 'DMQ1', {}, {});
  check(context, settings1, undefined, undefined, require('../dynamodb-options.json'), false, settings1);

  // Don't force a different configuration
  const settings2 = toSettings(KINESIS_STREAM_TYPE, 'myTasks_NoOverride', 0.81, 77, extractMessageFromRecord2, loadTaskTrackingState2, saveTaskTrackingState2, handleIncompleteMessages2, discardUnusableRecords2, discardRejectedMessages2, 'taskTrackingTableName2', 'DRQ2', 'DMQ2', {}, {});
  check(context, settings2, undefined, undefined, require('../kinesis-options.json'), false, settings1);

  // Force a new configuration
  context.kinesis = undefined;
  context.dynamoDBDocClient = undefined;
  deleteCachedInstances();
  const settings3 = toSettings(KINESIS_STREAM_TYPE, 'myTasks_Override', 0.91, 7, extractMessageFromRecord2, loadTaskTrackingState2, saveTaskTrackingState2, handleIncompleteMessages2, discardUnusableRecords2, discardRejectedMessages2, 'taskTrackingTableName3', 'DRQ3', 'DMQ3', {}, {});
  check(context, settings3, undefined, undefined, require('../kinesis-options.json'), true, settings3);

  t.end();
});

// =====================================================================================================================
// configureStreamProcessing with options
// =====================================================================================================================

test('configureStreamProcessing with options', t => {
  // Set up region
  regions.ONLY_FOR_TESTING.setRegionIfNotSet('us-west-1');
  const region = regions.getRegion();
  // Remove any cached entries before configuring
  deleteCachedInstances();

  function check(context, settings, options, otherSettings, otherOptions, forceConfiguration, expectedSettings) {
    return checkConfigureStreamProcessing(t, context, settings, options, otherSettings, otherOptions, forceConfiguration, expectedSettings);
  }

  const context = {};

  t.notOk(isStreamProcessingConfigured(context), `Stream processing must NOT be configured yet`);

  // Configure for the first time
  const options1 = toOptions(DYNAMODB_STREAM_TYPE, 'myTasks', 0.75, 2, 'taskTrackingTableName1', 'DRQ1', 'DMQ1', {}, {});
  const expectedSettings1 = toSettings(DYNAMODB_STREAM_TYPE, 'myTasks', 0.75, 2,
    useStreamEventRecordAsMessage, loadTaskTrackingStateFromDynamoDB, saveTaskTrackingStateToDynamoDB,
    replayAllMessagesIfIncomplete, discardUnusableRecordsToDRQ, discardRejectedMessagesToDMQ,
    'taskTrackingTableName1', 'DRQ1', 'DMQ1', options1.kinesisOptions, options1.dynamoDBDocClientOptions);
  check(context, undefined, options1, undefined, require('../dynamodb-options.json'), false, expectedSettings1);

  // Don't force a different configuration
  const options2 = toOptions(KINESIS_STREAM_TYPE, 'myTasks_NoOverride', 0.81, 77, 'taskTrackingTableName2', 'DRQ2', 'DMQ2', {}, {});
  //const expectedSettings2 = toSettings(KINESIS_STREAM_TYPE, 'myTasks_NoOverride', 0.81, 77, extractJsonMessageFromKinesisRecord, skipLoadTaskTrackingState, skipSaveTaskTrackingState, resubmitIncompleteMessagesToKinesis, discardUnusableRecordsToDRQ, discardRejectedMessagesToDMQ, 'taskTrackingTableName2', 'DRQ2', 'DMQ2', {}, {});
  check(context, undefined, options2, undefined, require('../kinesis-options.json'), false, expectedSettings1);

  // Force a new configuration
  context.kinesis = undefined;
  context.dynamoDBDocClient = undefined;
  deleteCachedInstances();
  const options3 = toOptions(KINESIS_STREAM_TYPE, 'myTasks_Override', 0.91, 7, 'taskTrackingTableName2', 'DRQ3', 'DMQ2', {}, {});
  const expectedSettings3 = toSettings(KINESIS_STREAM_TYPE, 'myTasks_Override', 0.91, 7,
    extractJsonMessageFromKinesisRecord, skipLoadTaskTrackingState, skipSaveTaskTrackingState,
    resubmitIncompleteMessagesToKinesis, discardUnusableRecordsToDRQ, discardRejectedMessagesToDMQ,
    'taskTrackingTableName2', 'DRQ3', 'DMQ2', options3.kinesisOptions, options3.dynamoDBDocClientOptions);
  check(context, undefined, options3, undefined, require('../kinesis-options.json'), true, expectedSettings3);

  t.end();
});

// =====================================================================================================================
// configureDefaultKinesisStreamProcessing
// =====================================================================================================================

test('configureDefaultKinesisStreamProcessing', t => {
  // Set up region
  regions.ONLY_FOR_TESTING.setRegionIfNotSet('us-west-1');
  // Remove any cached entries before configuring
  deleteCachedInstances();

  function checkConfigureDefaultKinesisStreamProcessing(context, options, otherSettings, otherOptions, forceConfiguration, expectedSettings) {
    const before = context.streamProcessing;
    const mustChange = forceConfiguration || !isStreamProcessingConfigured(context);

    const c = configureDefaultKinesisStreamProcessing(context, options, otherSettings, otherOptions, forceConfiguration);

    t.ok(c === context, `Context returned must be given context`);
    checkSettings(t, context, before, mustChange, expectedSettings);
  }

  const context = {};

  // Dummy extract message from record functions
  const extractMessageFromRecord1 = (record, context) => record;
  const loadTaskTrackingState1 = (messages, context) => messages;
  const saveTaskTrackingState1 = (messages, context) => messages;
  const handleIncompleteMessages1 = (messages, context) => messages;
  const discardUnusableRecords1 = (records, context) => records;
  const discardRejectedMessages1 = (rejectedMessages, messages, context) => messages;

  t.notOk(isStreamProcessingConfigured(context), `Default stream processing must NOT be configured yet`);

  // Configure defaults for the first time
  const otherOptions = require('../kinesis-options.json');
  const options = otherOptions.streamProcessingOptions;

  const expectedSettings = toSettings(options.streamType, options.taskTrackingName, options.timeoutAtPercentageOfRemainingTime,
    options.maxNumberOfAttempts, extractJsonMessageFromKinesisRecord, skipLoadTaskTrackingState, skipSaveTaskTrackingState,
    resubmitIncompleteMessagesToKinesis, discardUnusableRecordsToDRQ, discardRejectedMessagesToDMQ, undefined,
    options.deadRecordQueueName, options.deadMessageQueueName, options.kinesisOptions, undefined);

  checkConfigureDefaultKinesisStreamProcessing(context, options, undefined, undefined, false, expectedSettings);

  // Force a totally different configuration to overwrite the default Kinesis configuration
  context.kinesis = undefined;
  context.dynamoDBDocClient = undefined;
  deleteCachedInstances();
  const dynamoDBSettings = toSettings(DYNAMODB_STREAM_TYPE, 'myTasks_Override', 0.91, 7,
    extractMessageFromRecord1, loadTaskTrackingState1, saveTaskTrackingState1, handleIncompleteMessages1, discardUnusableRecords1, discardRejectedMessages1,
    'taskTrackingTableName', 'DRQ', 'DMQ', {}, {});
  checkConfigureStreamProcessingWithSettings(t, context, dynamoDBSettings, undefined, require('../dynamodb-options.json'), true, dynamoDBSettings);

  // Don't force the default configuration back again
  checkConfigureDefaultKinesisStreamProcessing(context, options, undefined, otherOptions, false, dynamoDBSettings);

  // Force the default configuration back again
  context.kinesis = undefined;
  context.dynamoDBDocClient = undefined;
  deleteCachedInstances();
  checkConfigureDefaultKinesisStreamProcessing(context, options, undefined, otherOptions, true, expectedSettings);

  t.end();
});

// =====================================================================================================================
// configureDefaultDynamoDBStreamProcessing
// =====================================================================================================================

test('configureDefaultDynamoDBStreamProcessing', t => {
  // Set up region
  regions.ONLY_FOR_TESTING.setRegionIfNotSet('us-west-1');
    // Remove any cached entries before configuring
    deleteCachedInstances();

  function checkConfigureDefaultDynamoDBStreamProcessing(context, options, otherSettings, otherOptions, forceConfiguration, expectedSettings) {
    const before = context.streamProcessing;
    const mustChange = forceConfiguration || !isStreamProcessingConfigured(context);

    const c = configureDefaultDynamoDBStreamProcessing(context, options, otherSettings, otherOptions, forceConfiguration);

    t.ok(c === context, `Context returned must be given context`);
    checkSettings(t, context, before, mustChange, expectedSettings);
  }

  const context = {};

  // Dummy extract message from record functions
  const extractMessageFromRecord1 = (record, context) => record;
  const loadTaskTrackingState1 = (messages, context) => messages;
  const saveTaskTrackingState1 = (messages, context) => messages;
  const handleIncompleteMessages1 = (messages, context) => messages;
  const discardUnusableRecords1 = (records, context) => records;
  const discardRejectedMessages1 = (rejectedMessages, messages, context) => messages;

  t.notOk(isStreamProcessingConfigured(context), `Default stream processing must NOT be configured yet`);

  // Configure defaults for the first time
  const otherOptions = require('../dynamodb-options.json');
  const options = otherOptions.streamProcessingOptions;

  const expectedSettings = toSettings(options.streamType, options.taskTrackingName, options.timeoutAtPercentageOfRemainingTime,
    options.maxNumberOfAttempts, useStreamEventRecordAsMessage, loadTaskTrackingStateFromDynamoDB, saveTaskTrackingStateToDynamoDB,
    replayAllMessagesIfIncomplete, discardUnusableRecordsToDRQ, discardRejectedMessagesToDMQ, options.taskTrackingTableName,
    options.deadRecordQueueName, options.deadMessageQueueName, options.kinesisOptions, options.dynamoDBDocClientOptions);

  checkConfigureDefaultDynamoDBStreamProcessing(context, options, undefined, undefined, false, expectedSettings);

  // Force a totally different configuration to overwrite the default DynamoDB configuration
  context.kinesis = undefined;
  context.dynamoDBDocClient = undefined;
  deleteCachedInstances();
  const kinesisSettings = toSettings(KINESIS_STREAM_TYPE, 'myTasks_Override', 0.91, 7,
    extractMessageFromRecord1, loadTaskTrackingState1, saveTaskTrackingState1, handleIncompleteMessages1, discardUnusableRecords1, discardRejectedMessages1,
    'taskTrackingTableName', 'DRQ', 'DMQ', {}, undefined);
  checkConfigureStreamProcessingWithSettings(t, context, kinesisSettings, undefined, require('../kinesis-options.json'), true, kinesisSettings);

  // Don't force the default configuration back again
  checkConfigureDefaultDynamoDBStreamProcessing(context, options, undefined, otherOptions, false, kinesisSettings);

  // Force the default configuration back again
  context.kinesis = undefined;
  context.dynamoDBDocClient = undefined;
  deleteCachedInstances();
  checkConfigureDefaultDynamoDBStreamProcessing(context, options, undefined, otherOptions, true, expectedSettings);

  t.end();
});

// =====================================================================================================================
// getStreamProcessingSetting and getStreamProcessingFunction
// =====================================================================================================================

test('getStreamProcessingSetting and getStreamProcessingFunction', t => {
  const context = {};
  const options = require('../kinesis-options.json');

  // Configure default stream processing settings
  configureDefaultKinesisStreamProcessing(context);

  const defaultSettings = getDefaultKinesisStreamProcessingSettings(options.streamProcessingOptions);

  equal(t, getStreamProcessingSetting(context, STREAM_TYPE_SETTING), defaultSettings.streamType, 'streamType setting');
  equal(t, getStreamProcessingSetting(context, TASK_TRACKING_NAME_SETTING), defaultSettings.taskTrackingName, 'taskTrackingName setting');
  equal(t, getStreamProcessingSetting(context, TIMEOUT_AT_PERCENTAGE_OF_REMAINING_TIME_SETTING), defaultSettings.timeoutAtPercentageOfRemainingTime, 'timeoutAtPercentageOfRemainingTime setting');
  equal(t, getStreamProcessingSetting(context, MAX_NUMBER_OF_ATTEMPTS_SETTING), defaultSettings.maxNumberOfAttempts, 'maxNumberOfAttempts setting');

  equal(t, getStreamProcessingFunction(context, EXTRACT_MESSAGE_FROM_RECORD_SETTING), extractJsonMessageFromKinesisRecord, 'extractMessageFromRecord function');
  equal(t, getStreamProcessingFunction(context, DISCARD_UNUSABLE_RECORDS_SETTING), discardUnusableRecordsToDRQ, 'discardUnusableRecords function');
  equal(t, getStreamProcessingFunction(context, DISCARD_REJECTED_MESSAGES_SETTING), discardRejectedMessagesToDMQ, 'discardRejectedMessages function');
  equal(t, getStreamProcessingFunction(context, HANDLE_INCOMPLETE_MESSAGES_SETTING), resubmitIncompleteMessagesToKinesis, 'handleIncompleteMessages function');

  equal(t, getStreamProcessingSetting(context, DEAD_RECORD_QUEUE_NAME_SETTING), defaultSettings.deadRecordQueueName, 'deadRecordQueueName setting');
  equal(t, getStreamProcessingSetting(context, DEAD_MESSAGE_QUEUE_NAME_SETTING), defaultSettings.deadMessageQueueName, 'deadMessageQueueName setting');

  equal(t, getExtractMessageFromRecordFunction(context), extractJsonMessageFromKinesisRecord, 'extractMessageFromRecord function');
  equal(t, getDiscardUnusableRecordsFunction(context), discardUnusableRecordsToDRQ, 'discardUnusableRecords function');
  equal(t, getDiscardRejectedMessagesFunction(context), discardRejectedMessagesToDMQ, 'discardRejectedMessages function');
  equal(t, getHandleIncompleteMessagesFunction(context), resubmitIncompleteMessagesToKinesis, 'handleIncompleteMessages function');

  t.end();
});

// =====================================================================================================================
// extractJsonMessageFromKinesisRecord
// =====================================================================================================================

test('extractJsonMessageFromKinesisRecord', t => {
  const context = {};
  logging.configureDefaultLogging(context);
  configureDefaultKinesisStreamProcessing(context, undefined, undefined, undefined, true);

  const eventSourceARN = samples.sampleKinesisEventSourceArn('eventSourceArnRegion', 'TestStream_DEV');
  const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');
  const msg = sampleMessage();

  // Parse the non-JSON message and expect an error
  t.throws(() => extractJsonMessageFromKinesisRecord(record, context), SyntaxError, `parsing a non-JSON message must throw an error`);

  record.kinesis.data = base64.toBase64(msg);

  const message = extractJsonMessageFromKinesisRecord(record, context);

  t.ok(message, 'JSON message must be extracted');
  t.deepEqual(message, msg, 'JSON message must match original');

  t.end();
});

// =====================================================================================================================
// extractJsonMessageFromKinesisRecord
// =====================================================================================================================

test('useStreamEventRecordAsMessage', t => {
  const context = {};
  configureDefaultDynamoDBStreamProcessing(context, undefined, undefined, require('../dynamodb-options.json'), true);

  const record0 = {};

  // Use an undefined record and expect an error
  t.throws(() => useStreamEventRecordAsMessage(undefined, context), Error, `using an undefined record must throw an error`);
  t.throws(() => useStreamEventRecordAsMessage(null, context), Error, `using an undefined record must throw an error`);

  const message0 = useStreamEventRecordAsMessage(record0, context);

  t.ok(message0, 'message0 must exist');
  t.deepEqual(message0, record0, 'message0 must match record0');

  const eventSourceARN = samples.sampleDynamoDBEventSourceArn('eventSourceArnRegion', 'TestTable_DEV');
  const record1 = samples.awsDynamoDBUpdateSampleEvent(eventSourceARN).Records[0];

  const message1 = useStreamEventRecordAsMessage(record1, context);

  t.ok(message1, 'message1 must exist');
  t.deepEqual(message1, record1, 'message1 must match record1');


  t.end();
});

// // =====================================================================================================================
// // discardUnusableRecordsToDRQ
// // =====================================================================================================================
//
// test('discardUnusableRecordsToDRQ with 0 records', t => {
//   const context = {
//     kinesis: dummyKinesis(t, 'discardUnusableRecordsToDRQ', undefined)
//   };
//   logging.configureDefaultLogging(context);
//
//   const eventSourceARN = samples.sampleEventSourceArn('eventSourceArnRegion', 'TestStream_DEV');
//   const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');
//
//   const awsContext = samples.sampleAwsContext('functionName', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'functionName', '1.0.1'));
//   const event = samples.sampleKinesisEventWithRecords([record]);
//   stages.configureDefaultStageHandling(context, false);
//   stages.configureStage(context, event, awsContext, true);
//
//   configureDefaultKinesisStreamProcessing(context, undefined, undefined, undefined, true);
//
//   t.plan(1);
//   discardUnusableRecordsToDRQ([], context)
//     .then(results => {
//       t.equal(results.length, 0, `discardUnusableRecordsToDRQ results (${results.length}) must be 0`);
//     })
//     .catch(err => {
//       t.fail(`discardUnusableRecordsToDRQ expected no failure - error: ${err.stack}`);
//     });
// });
//
// test('discardUnusableRecordsToDRQ with 1 record', t => {
//   const context = {
//     kinesis: dummyKinesis(t, 'discardUnusableRecordsToDRQ', undefined)
//   };
//   logging.configureDefaultLogging(context);
//
//   const eventSourceARN = samples.sampleEventSourceArn('eventSourceArnRegion', 'TestStream_DEV');
//   const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');
//
//   const awsContext = samples.sampleAwsContext('functionName', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'functionName', '1.0.1'));
//   const event = samples.sampleKinesisEventWithRecords([record]);
//   stages.configureDefaultStageHandling(context, false);
//   stages.configureStage(context, event, awsContext, true);
//
//   configureDefaultKinesisStreamProcessing(context, undefined, undefined, undefined, true);
//
//   t.plan(2);
//   discardUnusableRecordsToDRQ([record], context)
//     .then(results => {
//       t.equal(results.length, 1, `discardUnusableRecordsToDRQ results (${results.length}) must be 1`);
//     })
//     .catch(err => {
//       t.fail(`discardUnusableRecordsToDRQ expected no failure - error: ${err.stack}`);
//     });
// });
//
// test('discardUnusableRecordsToDRQ with 2 records', t => {
//   const context = {
//     kinesis: dummyKinesis(t, 'discardUnusableRecordsToDRQ', undefined)
//   };
//   logging.configureDefaultLogging(context);
//
//   const eventSourceARN = samples.sampleEventSourceArn('eventSourceArnRegion', 'TestStream_DEV');
//   const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');
//   const record2 = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');
//
//   const awsContext = samples.sampleAwsContext('functionName', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'functionName', '1.0.1'));
//   const records = [record, record2];
//   const event = samples.sampleKinesisEventWithRecords(records);
//   stages.configureDefaultStageHandling(context, false);
//   stages.configureStage(context, event, awsContext, true);
//
//   configureDefaultKinesisStreamProcessing(context, undefined, undefined, undefined, true);
//
//   t.plan(3);
//   discardUnusableRecordsToDRQ(records, context)
//     .then(results => {
//       t.equal(results.length, 2, `discardUnusableRecordsToDRQ results (${results.length}) must be 2`);
//     })
//     .catch(err => {
//       t.fail(`discardUnusableRecordsToDRQ expected no failure - error: ${err.stack}`);
//     });
// });
//
// test('discardUnusableRecordsToDRQ with 1 record and failure', t => {
//   const error = new Error('Planned failure');
//   const context = {
//     kinesis: dummyKinesis(t, 'discardUnusableRecordsToDRQ', error)
//   };
//   logging.configureDefaultLogging(context);
//
//   const eventSourceARN = samples.sampleEventSourceArn('eventSourceArnRegion', 'TestStream_DEV');
//   const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');
//
//   const awsContext = samples.sampleAwsContext('functionName', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'functionName', '1.0.1'));
//   const event = samples.sampleKinesisEventWithRecords([record]);
//   stages.configureDefaultStageHandling(context, false);
//   stages.configureStage(context, event, awsContext, true);
//
//   configureDefaultKinesisStreamProcessing(context, undefined, undefined, undefined, true);
//
//   t.plan(2);
//   discardUnusableRecordsToDRQ([record], context)
//     .then(() => {
//       t.fail(`discardUnusableRecordsToDRQ expected a failure`);
//     })
//     .catch(err => {
//       t.equal(err, error, `discardUnusableRecordsToDRQ error (${err}) must be ${error}`);
//     });
// });
//
// // =====================================================================================================================
// // discardRejectedMessagesToDMQ
// // =====================================================================================================================
//
// test('discardRejectedMessagesToDMQ with 0 messages', t => {
//   const context = {
//     kinesis: dummyKinesis(t, 'discardRejectedMessagesToDMQ', undefined)
//   };
//   logging.configureDefaultLogging(context);
//
//   const eventSourceARN = samples.sampleEventSourceArn('eventSourceArnRegion', 'TestStream_DEV');
//   const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');
//
//   const awsContext = samples.sampleAwsContext('functionName', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'functionName', '1.0.1'));
//   const event = samples.sampleKinesisEventWithRecords([record]);
//   stages.configureDefaultStageHandling(context, false);
//   stages.configureStage(context, event, awsContext, true);
//
//   configureDefaultKinesisStreamProcessing(context, undefined, undefined, undefined, true);
//
//   t.plan(1);
//   discardRejectedMessagesToDMQ([], context)
//     .then(results => {
//       t.equal(results.length, 0, `discardRejectedMessagesToDMQ results (${results.length}) must be 0`);
//     })
//     .catch(err => {
//       t.fail(`discardRejectedMessagesToDMQ expected no failure - error: ${err.stack}`);
//     });
// });
//
// test('discardRejectedMessagesToDMQ with 1 message', t => {
//   const context = {
//     kinesis: dummyKinesis(t, 'discardRejectedMessagesToDMQ', undefined)
//   };
//   logging.configureDefaultLogging(context);
//
//   const eventSourceARN = samples.sampleEventSourceArn('eventSourceArnRegion', 'TestStream_DEV');
//   const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');
//   const message = sampleMessage();
//   message.taskTracking = {record: record};
//
//   const awsContext = samples.sampleAwsContext('functionName', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'functionName', '1.0.1'));
//   const event = samples.sampleKinesisEventWithRecords([record]);
//   stages.configureDefaultStageHandling(context, false);
//   stages.configureStage(context, event, awsContext, true);
//
//   configureDefaultKinesisStreamProcessing(context, undefined, undefined, undefined, true);
//
//   t.plan(2);
//   discardRejectedMessagesToDMQ([message], context)
//     .then(results => {
//       t.equal(results.length, 1, `discardRejectedMessagesToDMQ results (${results.length}) must be 1`);
//     })
//     .catch(err => {
//       t.fail(`discardRejectedMessagesToDMQ expected no failure - error: ${err.stack}`);
//     });
// });
//
// test('discardRejectedMessagesToDMQ with 2 messages', t => {
//   const context = {
//     kinesis: dummyKinesis(t, 'discardRejectedMessagesToDMQ', undefined)
//   };
//   logging.configureDefaultLogging(context);
//
//   const eventSourceARN = samples.sampleEventSourceArn('eventSourceArnRegion', 'TestStream_DEV');
//   const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');
//   const message = sampleMessage();
//   message.taskTracking = {record: record};
//
//   const record2 = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');
//   const message2 = sampleMessage();
//   message2.taskTracking = {record: record2};
//
//   const awsContext = samples.sampleAwsContext('functionName', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'functionName', '1.0.1'));
//   const records = [record, record2];
//   const messages = [message, message2];
//   const event = samples.sampleKinesisEventWithRecords(records);
//   stages.configureDefaultStageHandling(context, false);
//   stages.configureStage(context, event, awsContext, true);
//
//   configureDefaultKinesisStreamProcessing(context, undefined, undefined, undefined, true);
//
//   t.plan(3);
//   discardRejectedMessagesToDMQ(messages, context)
//     .then(results => {
//       t.equal(results.length, 2, `discardRejectedMessagesToDMQ results (${results.length}) must be 2`);
//     })
//     .catch(err => {
//       t.fail(`discardRejectedMessagesToDMQ expected no failure - error: ${err.stack}`);
//     });
// });
//
// test('discardRejectedMessagesToDMQ with 1 record and failure', t => {
//   const error = new Error('Planned failure');
//   const context = {
//     kinesis: dummyKinesis(t, 'discardRejectedMessagesToDMQ', error)
//   };
//   logging.configureDefaultLogging(context);
//
//   const eventSourceARN = samples.sampleEventSourceArn('eventSourceArnRegion', 'TestStream_DEV');
//   const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');
//   const message = sampleMessage();
//   message.taskTracking = {record: record};
//
//   const awsContext = samples.sampleAwsContext('functionName', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'functionName', '1.0.1'));
//   const event = samples.sampleKinesisEventWithRecords([record]);
//   stages.configureDefaultStageHandling(context, false);
//   stages.configureStage(context, event, awsContext, true);
//
//   configureDefaultKinesisStreamProcessing(context, undefined, undefined, undefined, true);
//
//   t.plan(2);
//   discardRejectedMessagesToDMQ([message], context)
//     .then(() => {
//       t.fail(`discardRejectedMessagesToDMQ expected a failure`);
//     })
//     .catch(err => {
//       t.equal(err, error, `discardRejectedMessagesToDMQ error (${err}) must be ${error}`);
//     });
// });
//
//
// // =====================================================================================================================
// // resubmitIncompleteMessagesToKinesis
// // =====================================================================================================================
//
// test('resubmitIncompleteMessagesToKinesis with 0 messages', t => {
//   const context = {
//     kinesis: dummyKinesis(t, 'resubmitIncompleteMessagesToKinesis', undefined)
//   };
//   logging.configureDefaultLogging(context);
//
//   const streamName = 'TestStream_DEV';
//   const eventSourceARN = samples.sampleEventSourceArn('eventSourceArnRegion', streamName);
//   const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');
//
//   const awsContext = samples.sampleAwsContext('functionName', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'functionName', '1.0.1'));
//   const event = samples.sampleKinesisEventWithRecords([record]);
//   stages.configureDefaultStageHandling(context, false);
//   stages.configureStage(context, event, awsContext, true);
//
//   configureDefaultKinesisStreamProcessing(context, undefined, undefined, undefined, true);
//
//   t.plan(1);
//   resubmitIncompleteMessagesToKinesis([], [], context)
//     .then(results => {
//       t.equal(results.length, 0, `resubmitIncompleteMessagesToKinesis results (${results.length}) must be 0`);
//     })
//     .catch(err => {
//       t.fail(`resubmitIncompleteMessagesToKinesis expected no failure - error: ${err.stack}`);
//     });
// });
//
// test('resubmitIncompleteMessagesToKinesis with 1 message', t => {
//   const context = {
//     kinesis: dummyKinesis(t, 'resubmitIncompleteMessagesToKinesis', undefined)
//   };
//   logging.configureDefaultLogging(context);
//
//   const streamName = 'TestStream_DEV';
//   const eventSourceARN = samples.sampleEventSourceArn('eventSourceArnRegion', streamName);
//   const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');
//   const message = sampleMessage();
//   message.taskTracking = {record: record};
//
//   const awsContext = samples.sampleAwsContext('functionName', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'functionName', '1.0.1'));
//   const event = samples.sampleKinesisEventWithRecords([record]);
//   stages.configureDefaultStageHandling(context, false);
//   stages.configureStage(context, event, awsContext, true);
//
//   configureDefaultKinesisStreamProcessing(context, undefined, undefined, undefined, true);
//
//   t.plan(2);
//   resubmitIncompleteMessagesToKinesis([message], [message], context)
//     .then(results => {
//       t.equal(results.length, 1, `resubmitIncompleteMessagesToKinesis results (${results.length}) must be 1`);
//     })
//     .catch(err => {
//       t.fail(`resubmitIncompleteMessagesToKinesis expected no failure - error: ${err.stack}`);
//     });
// });
//
// test('resubmitIncompleteMessagesToKinesis with 2 messages', t => {
//   const context = {
//     kinesis: dummyKinesis(t, 'resubmitIncompleteMessagesToKinesis', undefined)
//   };
//   logging.configureDefaultLogging(context);
//
//   const streamName = 'TestStream_DEV';
//   const eventSourceARN = samples.sampleEventSourceArn('eventSourceArnRegion', streamName);
//   const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');
//   const message = sampleMessage();
//   message.taskTracking = {record: record};
//
//   const record2 = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');
//   const message2 = sampleMessage();
//   message2.taskTracking = {record: record2};
//
//   const awsContext = samples.sampleAwsContext('functionName', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'functionName', '1.0.1'));
//   const records = [record, record2];
//   const messages = [message, message2];
//   const event = samples.sampleKinesisEventWithRecords(records);
//   stages.configureDefaultStageHandling(context, false);
//   stages.configureStage(context, event, awsContext, true);
//
//   configureDefaultKinesisStreamProcessing(context, undefined, undefined, undefined, true);
//
//   t.plan(3);
//   resubmitIncompleteMessagesToKinesis(messages, messages, context)
//     .then(results => {
//       t.equal(results.length, 2, `resubmitIncompleteMessagesToKinesis results (${results.length}) must be 2`);
//     })
//     .catch(err => {
//       t.fail(`resubmitIncompleteMessagesToKinesis expected no failure - error: ${err.stack}`);
//     });
// });
//
// test('resubmitIncompleteMessagesToKinesis with 1 record and failure', t => {
//   const error = new Error('Planned failure');
//   const context = {
//     kinesis: dummyKinesis(t, 'resubmitIncompleteMessagesToKinesis', error)
//   };
//   logging.configureDefaultLogging(context);
//
//   const streamName = 'TestStream_DEV';
//   const eventSourceARN = samples.sampleEventSourceArn('eventSourceArnRegion', streamName);
//   const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');
//   const message = sampleMessage();
//   message.taskTracking = {record: record};
//
//   const awsContext = samples.sampleAwsContext('functionName', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'functionName', '1.0.1'));
//   const event = samples.sampleKinesisEventWithRecords([record]);
//   stages.configureDefaultStageHandling(context, false);
//   stages.configureStage(context, event, awsContext, true);
//
//   configureDefaultKinesisStreamProcessing(context, undefined, undefined, undefined, true);
//
//   t.plan(2);
//   resubmitIncompleteMessagesToKinesis([message], [message], context)
//     .then(results => {
//       t.fail(`resubmitIncompleteMessagesToKinesis expected a failure`);
//     })
//     .catch(err => {
//       t.equal(err, error, `resubmitIncompleteMessagesToKinesis error (${err}) must be ${error}`);
//     });
// });
