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

const configureStreamProcessingWithSettings = streamProcessing.configureStreamProcessingWithSettings;

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

const samples = require("./samples");
const strings = require("core-functions/strings");
const stringify = strings.stringify;

function sampleAwsEvent(streamName, partitionKey, data, omitEventSourceARN) {
  const region = process.env.AWS_REGION;
  const eventSourceArn = omitEventSourceARN ? undefined : samples.sampleKinesisEventSourceArn(region, streamName);
  return samples.sampleKinesisEventWithSampleRecord(partitionKey, data, eventSourceArn, region);
}

function sampleAwsContext(functionVersion, functionAlias) {
  const region = process.env.AWS_REGION;
  const functionName = 'sampleFunctionName';
  const invokedFunctionArn = samples.sampleInvokedFunctionArn(region, functionName, functionAlias);
  return samples.sampleAwsContext(functionName, functionVersion, invokedFunctionArn);
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
  taskTrackingTableName, deadRecordQueueName, deadMessageQueueName) {

  return {
    // generic settings
    streamType: streamType,
    taskTrackingName: taskTrackingName,
    timeoutAtPercentageOfRemainingTime: timeoutAtPercentageOfRemainingTime,
    maxNumberOfAttempts: maxNumberOfAttempts,
    // specialised settings needed by default implementations
    taskTrackingTableName: taskTrackingTableName,
    deadRecordQueueName: deadRecordQueueName,
    deadMessageQueueName: deadMessageQueueName
  };
}


function toSettings(streamType, taskTrackingName, timeoutAtPercentageOfRemainingTime, maxNumberOfAttempts,
  extractMessageFromRecord, loadTaskTrackingState, saveTaskTrackingState, handleIncompleteMessages,
  discardUnusableRecords, discardRejectedMessages, taskTrackingTableName, deadRecordQueueName, deadMessageQueueName) {

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
    deadMessageQueueName: deadMessageQueueName
  };
}
function toSettingsWithFunctionsOnly(extractMessageFromRecord, loadTaskTrackingState, saveTaskTrackingState,
  handleIncompleteMessages, discardUnusableRecords, discardRejectedMessages) {

  return {
    // functions
    extractMessageFromRecord: extractMessageFromRecord,
    loadTaskTrackingState: loadTaskTrackingState,
    saveTaskTrackingState: saveTaskTrackingState,
    handleIncompleteMessages: handleIncompleteMessages,
    discardUnusableRecords: discardUnusableRecords,
    discardRejectedMessages: discardRejectedMessages
  };
}

function checkSettings(t, context, before, mustChange, expectedSettings) {
  t.ok(isStreamProcessingConfigured(context), `Default stream processing must be configured now`);

  const after = context.streamProcessing;

  const expectedStreamType = mustChange ? expectedSettings.streamType : before.streamType;
  const expectedTaskTrackingName = mustChange ? expectedSettings.taskTrackingName : before.taskTrackingName;
  const expectedTimeoutAtPercentageOfRemainingTime = mustChange ? expectedSettings.timeoutAtPercentageOfRemainingTime : before.timeoutAtPercentageOfRemainingTime;
  const expectedMaxNumberOfAttempts = mustChange ? expectedSettings.maxNumberOfAttempts : before.maxNumberOfAttempts;
  const expectedExtractMessageFromRecord = mustChange ? expectedSettings.extractMessageFromRecord : before.extractMessageFromRecord;
  const expectedLoadTaskTrackingState = mustChange ? expectedSettings.loadTaskTrackingState : before.loadTaskTrackingState;
  const expectedSaveTaskTrackingState = mustChange ? expectedSettings.saveTaskTrackingState : before.saveTaskTrackingState;
  const expectedHandleIncompleteMessages = mustChange ? expectedSettings.handleIncompleteMessages : before.handleIncompleteMessages;
  const expectedDiscardUnusableRecords = mustChange ? expectedSettings.discardUnusableRecords : before.discardUnusableRecords;
  const expectedDiscardRejectedMessages = mustChange ? expectedSettings.discardRejectedMessages : before.discardRejectedMessages;
  const expectedTaskTrackingTableName = mustChange ? expectedSettings.taskTrackingTableName : before.taskTrackingTableName;
  const expectedDeadRecordQueueName = mustChange ? expectedSettings.deadRecordQueueName : before.deadRecordQueueName;
  const expectedDeadMessageQueueName = mustChange ? expectedSettings.deadMessageQueueName : before.deadMessageQueueName;

  t.equal(after.streamType, expectedStreamType, `streamType must be ${expectedStreamType}`);
  t.equal(after.taskTrackingName, expectedTaskTrackingName, `taskTrackingName must be ${expectedTaskTrackingName}`);
  t.equal(after.timeoutAtPercentageOfRemainingTime, expectedTimeoutAtPercentageOfRemainingTime, `timeoutAtPercentageOfRemainingTime must be ${expectedTimeoutAtPercentageOfRemainingTime}`);
  t.equal(after.maxNumberOfAttempts, expectedMaxNumberOfAttempts, `maxNumberOfAttempts must be ${expectedMaxNumberOfAttempts}`);
  t.equal(after.extractMessageFromRecord, expectedExtractMessageFromRecord, `extractMessageFromRecord must be ${stringify(expectedExtractMessageFromRecord)}`);
  t.equal(after.loadTaskTrackingState, expectedLoadTaskTrackingState, `loadTaskTrackingState must be ${stringify(expectedLoadTaskTrackingState)}`);
  t.equal(after.saveTaskTrackingState, expectedSaveTaskTrackingState, `saveTaskTrackingState must be ${stringify(expectedSaveTaskTrackingState)}`);
  t.equal(after.handleIncompleteMessages, expectedHandleIncompleteMessages, `handleIncompleteMessages must be ${stringify(expectedHandleIncompleteMessages)}`);
  t.equal(after.discardUnusableRecords, expectedDiscardUnusableRecords, `discardUnusableRecords must be ${stringify(expectedDiscardUnusableRecords)}`);
  t.equal(after.discardRejectedMessages, expectedDiscardRejectedMessages, `discardRejectedMessages must be ${stringify(expectedDiscardRejectedMessages)}`);
  t.equal(after.taskTrackingTableName, expectedTaskTrackingTableName, `taskTrackingTableName must be ${expectedTaskTrackingTableName}`);
  t.equal(after.deadRecordQueueName, expectedDeadRecordQueueName, `deadRecordQueueName must be ${expectedDeadRecordQueueName}`);
  t.equal(after.deadMessageQueueName, expectedDeadMessageQueueName, `deadMessageQueueName must be ${expectedDeadMessageQueueName}`);
}

function checkDependencies(t, context, stdSettings, stdOptions, event, awsContext, expectedStage) {
  t.ok(logging.isLoggingConfigured(context), `logging must be configured`);
  t.ok(stages.isStageHandlingConfigured(context), `stage handling must be configured`);
  t.ok(context.custom && typeof context.custom === 'object', `context.custom must be configured`);

  const kinesisOptions = stdSettings && stdSettings.kinesisOptions ? stdSettings.kinesisOptions :
    stdOptions && stdOptions.kinesisOptions ? stdOptions.kinesisOptions : undefined;

  const dynamoDBDocClientOptions = stdSettings && stdSettings.dynamoDBDocClientOptions ? stdSettings.dynamoDBDocClientOptions :
    stdOptions && stdOptions.dynamoDBDocClientOptions ? stdOptions.dynamoDBDocClientOptions : undefined;

  // Check Kinesis instance is also configured
  const region = regions.getRegion();
  if (kinesisOptions) {
    t.ok(context.kinesis && typeof context.kinesis === 'object', 'context.kinesis must be configured');
    t.equal(context.kinesis.config.region, region, `context.kinesis.config.region (${context.kinesis.config.region}) must be ${region}`);
    t.equal(context.kinesis.config.maxRetries, kinesisOptions.maxRetries, `context.kinesis.config.maxRetries (${context.kinesis.config.maxRetries}) must be ${kinesisOptions.maxRetries}`);
  } else {
    t.notOk(context.kinesis, 'context.kinesis must not be configured');
  }

  // Check DynamoDB DocumentClient instance is also configured
  if (dynamoDBDocClientOptions) {
    // Check DynamoDB.DocumentClient is also configured
    t.ok(context.dynamoDBDocClient && typeof context.dynamoDBDocClient === 'object', 'context.dynamoDBDocClient must be configured');
    t.equal(context.dynamoDBDocClient.service.config.region, region, `context.dynamoDBDocClient.service.config.region (${context.dynamoDBDocClient.service.config.region}) must be ${region}`);
    t.equal(context.dynamoDBDocClient.service.config.maxRetries, dynamoDBDocClientOptions.maxRetries,
      `context.dynamoDBDocClient.service.config.maxRetries (${context.dynamoDBDocClient.service.config.maxRetries}) must be ${dynamoDBDocClientOptions.maxRetries}`);
  } else {
    t.notOk(context.dynamoDBDocClient, 'context.dynamoDBDocClient must not be configured');
  }

  if (event && awsContext) {
    t.equal(context.region, region, `context.region must be ${region}`);
    t.equal(context.stage, expectedStage, `context.stage must be ${expectedStage}`);
    t.equal(context.awsContext, awsContext, 'context.awsContext must be given AWS context');
  }
}

function checkConfigureStreamProcessingWithSettings(t, context, settings, stdSettings, stdOptions, event, awsContext, forceConfiguration, expectedSettings, expectedStage) {
  const before = context.streamProcessing;
  const mustChange = forceConfiguration || !isStreamProcessingConfigured(context);

  const c = configureStreamProcessingWithSettings(context, settings, stdSettings, stdOptions, event, awsContext, forceConfiguration);

  t.ok(c === context, `Context returned must be given context`);
  checkSettings(t, context, before, mustChange, expectedSettings);
  checkDependencies(t, context, stdSettings, stdOptions, event, awsContext, expectedStage);
}

function checkConfigureStreamProcessing(t, context, settings, options, stdSettings, stdOptions, event, awsContext, forceConfiguration, expectedSettings, expectedStage) {
  const before = context.streamProcessing;
  const mustChange = forceConfiguration || !isStreamProcessingConfigured(context);

  const c = configureStreamProcessing(context, settings, options, stdSettings, stdOptions, event, awsContext, forceConfiguration);

  t.ok(c === context, `Context returned must be given context`);
  checkSettings(t, context, before, mustChange, expectedSettings);
  checkDependencies(t, context, stdSettings, stdOptions, event, awsContext, expectedStage);
}

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
  const region = regions.getRegion();
  kinesisCache.deleteKinesis(region);
  dynamoDBDocClientCache.deleteDynamoDBDocClient(region);
}

// =====================================================================================================================
// isStreamProcessingConfigured
// =====================================================================================================================

test('isStreamProcessingConfigured', t => {
  const context = {};
  t.notOk(isStreamProcessingConfigured(context), `Stream processing must NOT be configured yet`);

  configureDefaultKinesisStreamProcessing(context, undefined, undefined, undefined, undefined, undefined, true);

  t.ok(isStreamProcessingConfigured(context), `Stream processing must be configured now`);

  t.end();
});

// =====================================================================================================================
// configureStreamProcessingWithSettings without event & awsContext
// =====================================================================================================================

test('configureStreamProcessingWithSettings without event & awsContext', t => {
  function check(context, settings, stdSettings, stdOptions, forceConfiguration, expectedSettings) {
    return checkConfigureStreamProcessingWithSettings(t, context, settings, stdSettings, stdOptions, undefined, undefined,
      forceConfiguration, expectedSettings, undefined);
  }

  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

    const context = {};

    t.notOk(isStreamProcessingConfigured(context), `Stream processing must NOT be configured yet`);

    // Dummy extract message from record functions
    const extractMessageFromRecord1 = (record, context) => record;
    const extractMessageFromRecord2 = (record, context) => record;

    const loadTaskTrackingState1 = (messages, context) => messages;
    const loadTaskTrackingState2 = (messages, context) => messages;

    const saveTaskTrackingState1 = (messages, context) => messages;
    const saveTaskTrackingState2 = (messages, context) => messages;

    const handleIncompleteMessages1 = (messages, incompleteMessages, context) => incompleteMessages;
    const handleIncompleteMessages2 = (messages, incompleteMessages, context) => incompleteMessages;

    const discardUnusableRecords1 = (unusableRecords, context) => unusableRecords;
    const discardUnusableRecords2 = (unusableRecords, context) => unusableRecords;

    const discardRejectedMessages1 = (rejectedMessages, context) => rejectedMessages;
    const discardRejectedMessages2 = (rejectedMessages, context) => rejectedMessages;

    // Configure for the first time
    const settings1 = toSettings(DYNAMODB_STREAM_TYPE, 'myTasks1', 0.75, 2, extractMessageFromRecord1, loadTaskTrackingState1, saveTaskTrackingState1, handleIncompleteMessages1, discardUnusableRecords1, discardRejectedMessages1, 'taskTrackingTableName1', 'DRQ1', 'DMQ1');
    const expectedSettings1 = toSettings(DYNAMODB_STREAM_TYPE, 'myTasks1', 0.75, 2, extractMessageFromRecord1, loadTaskTrackingState1, saveTaskTrackingState1, handleIncompleteMessages1, discardUnusableRecords1, discardRejectedMessages1, 'taskTrackingTableName1', 'DRQ1', 'DMQ1');
    check(context, settings1, undefined, require('../default-dynamodb-options.json'), false, expectedSettings1);

    // Don't force a different configuration
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    const settings2 = toSettings(KINESIS_STREAM_TYPE, 'myTasks_NoOverride2', 0.81, 77, extractMessageFromRecord2, loadTaskTrackingState2, saveTaskTrackingState2, handleIncompleteMessages2, discardUnusableRecords2, discardRejectedMessages2, 'taskTrackingTableName2', 'DRQ2', 'DMQ2');
    check(context, settings2, undefined, require('../default-kinesis-options.json'), false, expectedSettings1);

    // Force a new configuration
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    const settings3 = toSettings(KINESIS_STREAM_TYPE, 'myTasks_Override3', 0.91, 7, extractMessageFromRecord2, loadTaskTrackingState2, saveTaskTrackingState2, handleIncompleteMessages2, discardUnusableRecords2, discardRejectedMessages2, 'taskTrackingTableName3', 'DRQ3', 'DMQ3');
    const expectedSettings3 = toSettings(KINESIS_STREAM_TYPE, 'myTasks_Override3', 0.91, 7, extractMessageFromRecord2, loadTaskTrackingState2, saveTaskTrackingState2, handleIncompleteMessages2, discardUnusableRecords2, discardRejectedMessages2, 'taskTrackingTableName3', 'DRQ3', 'DMQ3');
    check(context, settings3, undefined, require('../default-kinesis-options.json'), true, expectedSettings3);

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

// =====================================================================================================================
// configureStreamProcessingWithSettings with event & awsContext
// =====================================================================================================================

test('configureStreamProcessingWithSettings with event & awsContext', t => {
  function check(context, settings, stdSettings, stdOptions, event, awsContext, forceConfiguration, expectedSettings, expectedStage) {
    return checkConfigureStreamProcessingWithSettings(t, context, settings, stdSettings, stdOptions, event, awsContext,
      forceConfiguration, expectedSettings, expectedStage);
  }

  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");
    const expectedStage = 'dev99';

    const context = {};

    t.notOk(isStreamProcessingConfigured(context), `Stream processing must NOT be configured yet`);

    // Dummy extract message from record functions
    const extractMessageFromRecord1 = (record, context) => record;
    const extractMessageFromRecord2 = (record, context) => record;

    const loadTaskTrackingState1 = (messages, context) => messages;
    const loadTaskTrackingState2 = (messages, context) => messages;

    const saveTaskTrackingState1 = (messages, context) => messages;
    const saveTaskTrackingState2 = (messages, context) => messages;

    const handleIncompleteMessages1 = (messages, incompleteMessages, context) => incompleteMessages;
    const handleIncompleteMessages2 = (messages, incompleteMessages, context) => incompleteMessages;

    const discardUnusableRecords1 = (unusableRecords, context) => unusableRecords;
    const discardUnusableRecords2 = (unusableRecords, context) => unusableRecords;

    const discardRejectedMessages1 = (rejectedMessages, context) => rejectedMessages;
    const discardRejectedMessages2 = (rejectedMessages, context) => rejectedMessages;

    // Generate a sample AWS event
    const event = sampleAwsEvent('TestStream_DEV2', 'partitionKey', '', false);

    // Generate a sample AWS context
    const awsContext = sampleAwsContext('1.0.1', 'dev1');

    // Configure for the first time
    const settings1 = toSettings(DYNAMODB_STREAM_TYPE, 'myTasks1', 0.75, 2, extractMessageFromRecord1, loadTaskTrackingState1, saveTaskTrackingState1, handleIncompleteMessages1, discardUnusableRecords1, discardRejectedMessages1, 'taskTrackingTableName1', 'DRQ1', 'DMQ1');
    const expectedSettings1 = toSettings(DYNAMODB_STREAM_TYPE, 'myTasks1', 0.75, 2, extractMessageFromRecord1, loadTaskTrackingState1, saveTaskTrackingState1, handleIncompleteMessages1, discardUnusableRecords1, discardRejectedMessages1, 'taskTrackingTableName1', 'DRQ1', 'DMQ1');
    check(context, settings1, undefined, require('../default-dynamodb-options.json'), event, awsContext, false, expectedSettings1, expectedStage);

    // Don't force a different configuration
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    const settings2 = toSettings(KINESIS_STREAM_TYPE, 'myTasks_NoOverride2', 0.81, 77, extractMessageFromRecord2, loadTaskTrackingState2, saveTaskTrackingState2, handleIncompleteMessages2, discardUnusableRecords2, discardRejectedMessages2, 'taskTrackingTableName2', 'DRQ2', 'DMQ2');
    check(context, settings2, undefined, require('../default-kinesis-options.json'), event, awsContext, false, expectedSettings1, expectedStage);

    // Force a new configuration
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    const settings3 = toSettings(KINESIS_STREAM_TYPE, 'myTasks_Override3', 0.91, 7, extractMessageFromRecord2, loadTaskTrackingState2, saveTaskTrackingState2, handleIncompleteMessages2, discardUnusableRecords2, discardRejectedMessages2, 'taskTrackingTableName3', 'DRQ3', 'DMQ3');
    const expectedSettings3 = toSettings(KINESIS_STREAM_TYPE, 'myTasks_Override3', 0.91, 7, extractMessageFromRecord2, loadTaskTrackingState2, saveTaskTrackingState2, handleIncompleteMessages2, discardUnusableRecords2, discardRejectedMessages2, 'taskTrackingTableName3', 'DRQ3', 'DMQ3');
    check(context, settings3, undefined, require('../default-kinesis-options.json'), event, awsContext, true, expectedSettings3, expectedStage);

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

// =====================================================================================================================
// configureStreamProcessing with options only
// =====================================================================================================================

test('configureStreamProcessing with options only', t => {
  function check(context, settings, options, stdSettings, stdOptions, event, awsContext, forceConfiguration, expectedSettings, expectedStage) {
    return checkConfigureStreamProcessing(t, context, settings, options, stdSettings, stdOptions, event, awsContext, forceConfiguration, expectedSettings, expectedStage);
  }

  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

    const context = {};

    t.notOk(isStreamProcessingConfigured(context), `Stream processing must NOT be configured yet`);

    // Configure for the first time
    const options1 = toOptions(DYNAMODB_STREAM_TYPE, 'myTasks1', 0.75, 2, 'taskTrackingTableName1', 'DRQ1', 'DMQ1');
    const expectedSettings1 = toSettings(DYNAMODB_STREAM_TYPE, 'myTasks1', 0.75, 2, useStreamEventRecordAsMessage, loadTaskTrackingStateFromDynamoDB, saveTaskTrackingStateToDynamoDB, replayAllMessagesIfIncomplete, discardUnusableRecordsToDRQ, discardRejectedMessagesToDMQ, 'taskTrackingTableName1', 'DRQ1', 'DMQ1');
    check(context, undefined, options1, undefined, require('../default-dynamodb-options.json'), undefined, undefined, false, expectedSettings1, undefined);

    // Don't force a different configuration
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    const options2 = toOptions(KINESIS_STREAM_TYPE, 'myTasks_NoOverride2', 0.81, 77, 'taskTrackingTableName2', 'DRQ2', 'DMQ2');
    const stdOptions = require('../default-kinesis-options.json');
    check(context, undefined, options2, undefined, stdOptions, undefined, undefined, false, expectedSettings1, undefined);

    // Force a new configuration
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    const options3 = toOptions(KINESIS_STREAM_TYPE, 'myTasks_Override3', 0.91, 7, 'taskTrackingTableName3', 'DRQ3', 'DMQ3');
    const expectedSettings3 = toSettings(KINESIS_STREAM_TYPE, 'myTasks_Override3', 0.91, 7, extractJsonMessageFromKinesisRecord, skipLoadTaskTrackingState, skipSaveTaskTrackingState, resubmitIncompleteMessagesToKinesis, discardUnusableRecordsToDRQ, discardRejectedMessagesToDMQ, 'taskTrackingTableName3', 'DRQ3', 'DMQ3');
    check(context, undefined, options3, undefined, stdOptions, undefined, undefined, true, expectedSettings3, undefined);

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

// =====================================================================================================================
// configureStreamProcessing with settings only
// =====================================================================================================================

test('configureStreamProcessing with settings only', t => {

  function check(context, settings, options, stdSettings, stdOptions, event, awsContext, forceConfiguration, expectedSettings, expectedStage) {
    return checkConfigureStreamProcessing(t, context, settings, options, stdSettings, stdOptions, event, awsContext,
      forceConfiguration, expectedSettings, expectedStage);
  }

  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

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
    const settings1 = toSettings(DYNAMODB_STREAM_TYPE, 'myTasks1', 0.75, 2, extractMessageFromRecord1, loadTaskTrackingState1, saveTaskTrackingState1, handleIncompleteMessages1, discardUnusableRecords1, discardRejectedMessages1, 'taskTrackingTableName1', 'DRQ1', 'DMQ1');
    const expectedSettings1 = toSettings(DYNAMODB_STREAM_TYPE, 'myTasks1', 0.75, 2, extractMessageFromRecord1, loadTaskTrackingState1, saveTaskTrackingState1, handleIncompleteMessages1, discardUnusableRecords1, discardRejectedMessages1, 'taskTrackingTableName1', 'DRQ1', 'DMQ1');
    check(context, settings1, undefined, undefined, require('../default-dynamodb-options.json'), undefined, undefined, false, expectedSettings1, undefined);

    // Don't force a different configuration
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    const settings2 = toSettings(KINESIS_STREAM_TYPE, 'myTasks_NoOverride2', 0.81, 77, extractMessageFromRecord2, loadTaskTrackingState2, saveTaskTrackingState2, handleIncompleteMessages2, discardUnusableRecords2, discardRejectedMessages2, 'taskTrackingTableName2', 'DRQ2', 'DMQ2');
    check(context, settings2, undefined, undefined, require('../default-kinesis-options.json'), undefined, undefined, false, expectedSettings1, undefined);

    // Force a new configuration
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    const settings3 = toSettings(KINESIS_STREAM_TYPE, 'myTasks_Override3', 0.91, 7, extractMessageFromRecord2, loadTaskTrackingState2, saveTaskTrackingState2, handleIncompleteMessages2, discardUnusableRecords2, discardRejectedMessages2, 'taskTrackingTableName3', 'DRQ3', 'DMQ3');
    const expectedSettings3 = toSettings(KINESIS_STREAM_TYPE, 'myTasks_Override3', 0.91, 7, extractMessageFromRecord2, loadTaskTrackingState2, saveTaskTrackingState2, handleIncompleteMessages2, discardUnusableRecords2, discardRejectedMessages2, 'taskTrackingTableName3', 'DRQ3', 'DMQ3');
    check(context, settings3, undefined, undefined, require('../default-kinesis-options.json'), undefined, undefined, true, expectedSettings3, undefined);

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

// =====================================================================================================================
// configureStreamProcessing with settings and options
// =====================================================================================================================

test('configureStreamProcessing with settings and options', t => {
  function check(context, settings, options, stdSettings, stdOptions, event, awsContext, forceConfiguration, expectedSettings, expectedStage) {
    return checkConfigureStreamProcessing(t, context, settings, options, stdSettings, stdOptions, event, awsContext,
      forceConfiguration, expectedSettings, expectedStage);
  }

  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

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
    const settings1 = toSettingsWithFunctionsOnly(extractMessageFromRecord1, loadTaskTrackingState1, saveTaskTrackingState1, handleIncompleteMessages1, discardUnusableRecords1, discardRejectedMessages1);
    const options1 = toOptions(DYNAMODB_STREAM_TYPE, 'myTasks1', 0.75, 2, 'taskTrackingTableName1', 'DRQ1', 'DMQ1');
    const expectedSettings1 = toSettings(DYNAMODB_STREAM_TYPE, 'myTasks1', 0.75, 2, extractMessageFromRecord1, loadTaskTrackingState1, saveTaskTrackingState1, handleIncompleteMessages1, discardUnusableRecords1, discardRejectedMessages1, 'taskTrackingTableName1', 'DRQ1', 'DMQ1');

    check(context, settings1, options1, undefined, require('../default-dynamodb-options.json'), undefined, undefined, false, expectedSettings1, undefined);

    // Don't force a different configuration
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    const settings2 = toSettingsWithFunctionsOnly(extractMessageFromRecord2, loadTaskTrackingState2, saveTaskTrackingState2, handleIncompleteMessages2, discardUnusableRecords2, discardRejectedMessages2);
    const options2 = toOptions(KINESIS_STREAM_TYPE, 'myTasks_NoOverride2', 0.81, 77, 'taskTrackingTableName2', 'DRQ2', 'DMQ2');
    check(context, settings2, options2, undefined, require('../default-kinesis-options.json'), undefined, undefined, false, expectedSettings1, undefined);

    // Force a new configuration
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    const settings3 = toSettingsWithFunctionsOnly(extractMessageFromRecord2, loadTaskTrackingState2, saveTaskTrackingState2, handleIncompleteMessages2, discardUnusableRecords2, discardRejectedMessages2);
    const options3 = toOptions(KINESIS_STREAM_TYPE, 'myTasks_Override3', 0.91, 7, 'taskTrackingTableName3', 'DRQ3', 'DMQ3');
    const expectedSettings3 = toSettings(KINESIS_STREAM_TYPE, 'myTasks_Override3', 0.91, 7, extractMessageFromRecord2, loadTaskTrackingState2, saveTaskTrackingState2, handleIncompleteMessages2, discardUnusableRecords2, discardRejectedMessages2, 'taskTrackingTableName3', 'DRQ3', 'DMQ3');
    check(context, settings3, options3, undefined, require('../default-kinesis-options.json'), undefined, undefined, true, expectedSettings3, undefined);

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

// =====================================================================================================================
// configureStreamProcessing with settings, options, event and awsContext
// =====================================================================================================================

test('configureStreamProcessing with settings, options, event and awsContext', t => {
  function check(context, settings, options, stdSettings, stdOptions, event, awsContext, forceConfiguration, expectedSettings, expectedStage) {
    return checkConfigureStreamProcessing(t, context, settings, options, stdSettings, stdOptions, event, awsContext,
      forceConfiguration, expectedSettings, expectedStage);
  }

  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");
    const expectedStage = 'dev99';

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

    // Generate a sample AWS event
    const event = sampleAwsEvent('TestStream_DEV2', 'partitionKey', '', false);

    // Generate a sample AWS context
    const awsContext = sampleAwsContext('1.0.1', 'dev1');

    // Configure for the first time
    const settings1 = toSettings(DYNAMODB_STREAM_TYPE, 'myTasks1', 0.75, 2, extractMessageFromRecord1, loadTaskTrackingState1, saveTaskTrackingState1, handleIncompleteMessages1, discardUnusableRecords1, discardRejectedMessages1, 'taskTrackingTableName1', 'DRQ1', 'DMQ1');
    const options1 = toOptions(DYNAMODB_STREAM_TYPE, 'myTasks1B', 0.74, 1, 'taskTrackingTableName1B', 'DRQ1B', 'DMQ1B');
    const expectedSettings1 = toSettings(DYNAMODB_STREAM_TYPE, 'myTasks1', 0.75, 2, extractMessageFromRecord1, loadTaskTrackingState1, saveTaskTrackingState1, handleIncompleteMessages1, discardUnusableRecords1, discardRejectedMessages1, 'taskTrackingTableName1', 'DRQ1', 'DMQ1');

    check(context, settings1, options1, undefined, require('../default-dynamodb-options.json'), event, awsContext, false, expectedSettings1, expectedStage);

    // Don't force a different configuration
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    const settings2 = toSettings(KINESIS_STREAM_TYPE, 'myTasks_NoOverride2', 0.81, 77, extractMessageFromRecord2, loadTaskTrackingState2, saveTaskTrackingState2, handleIncompleteMessages2, discardUnusableRecords2, discardRejectedMessages2, 'taskTrackingTableName2', 'DRQ2', 'DMQ2');
    const options2 = toOptions(KINESIS_STREAM_TYPE, 'myTasks_NoOverride2B', 0.80, 76, 'taskTrackingTableName2B', 'DRQ2B', 'DMQ2B');
    check(context, settings2, options2, undefined, require('../default-kinesis-options.json'), event, awsContext, false, expectedSettings1, expectedStage);

    // Force a new configuration
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    const settings3 = toSettings(KINESIS_STREAM_TYPE, 'myTasks_Override3', 0.91, 7, extractMessageFromRecord2, loadTaskTrackingState2, saveTaskTrackingState2, handleIncompleteMessages2, discardUnusableRecords2, discardRejectedMessages2, 'taskTrackingTableName3', 'DRQ3', 'DMQ3');
    const options3 = toOptions(KINESIS_STREAM_TYPE, 'myTasks_Override3B', 0.90, 6, 'taskTrackingTableName3B', 'DRQ3B', 'DMQ3B');
    const expectedSettings3 = toSettings(KINESIS_STREAM_TYPE, 'myTasks_Override3', 0.91, 7, extractMessageFromRecord2, loadTaskTrackingState2, saveTaskTrackingState2, handleIncompleteMessages2, discardUnusableRecords2, discardRejectedMessages2, 'taskTrackingTableName3', 'DRQ3', 'DMQ3');
    check(context, settings3, options3, undefined, require('../default-kinesis-options.json'), event, awsContext, true, expectedSettings3, expectedStage);

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

// =====================================================================================================================
// configureDefaultKinesisStreamProcessing without event & awsContext
// =====================================================================================================================

test('configureDefaultKinesisStreamProcessing without event & awsContext', t => {
  function checkConfigureDefaultKinesisStreamProcessing(context, options, stdSettings, stdOptions, forceConfiguration, expectedSettings) {
    const before = context.streamProcessing;
    const mustChange = forceConfiguration || !isStreamProcessingConfigured(context);

    const c = configureDefaultKinesisStreamProcessing(context, options, stdSettings, stdOptions, undefined, undefined, forceConfiguration);

    t.ok(c === context, `Context returned must be given context`);
    checkSettings(t, context, before, mustChange, expectedSettings);
    checkDependencies(t, context, stdSettings, stdOptions, undefined, undefined, undefined);
  }

  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

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
    const stdOptions = require('../default-kinesis-options.json');
    const options = stdOptions.streamProcessingOptions;

    const expectedSettings = toSettings(options.streamType, options.taskTrackingName, options.timeoutAtPercentageOfRemainingTime, options.maxNumberOfAttempts, extractJsonMessageFromKinesisRecord, skipLoadTaskTrackingState, skipSaveTaskTrackingState, resubmitIncompleteMessagesToKinesis, discardUnusableRecordsToDRQ, discardRejectedMessagesToDMQ, undefined, options.deadRecordQueueName, options.deadMessageQueueName);

    checkConfigureDefaultKinesisStreamProcessing(context, options, undefined, stdOptions, false, expectedSettings);

    // Force a totally different configuration to overwrite the default Kinesis configuration
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    const dynamoDBSettings = toSettings(DYNAMODB_STREAM_TYPE, 'myTasks_Override', 0.91, 7, extractMessageFromRecord1, loadTaskTrackingState1, saveTaskTrackingState1, handleIncompleteMessages1, discardUnusableRecords1, discardRejectedMessages1, 'taskTrackingTableName', 'DRQ', 'DMQ');
    checkConfigureStreamProcessingWithSettings(t, context, dynamoDBSettings, undefined, require('../default-dynamodb-options.json'), undefined, undefined, true, dynamoDBSettings, undefined);

    // Don't force the default configuration back again
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    checkConfigureDefaultKinesisStreamProcessing(context, options, undefined, stdOptions, false, dynamoDBSettings);

    // Force the default configuration back again
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    checkConfigureDefaultKinesisStreamProcessing(context, options, undefined, stdOptions, true, expectedSettings);

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

// =====================================================================================================================
// configureDefaultKinesisStreamProcessing with event & awsContext
// =====================================================================================================================

test('configureDefaultKinesisStreamProcessing with event & awsContext', t => {
  function checkConfigureDefaultKinesisStreamProcessing(context, options, stdSettings, stdOptions, event, awsContext, forceConfiguration, expectedSettings, expectedStage) {
    const before = context.streamProcessing;
    const mustChange = forceConfiguration || !isStreamProcessingConfigured(context);

    const c = configureDefaultKinesisStreamProcessing(context, options, stdSettings, stdOptions, event, awsContext, forceConfiguration);

    t.ok(c === context, `Context returned must be given context`);
    checkSettings(t, context, before, mustChange, expectedSettings);
    checkDependencies(t, context, stdSettings, stdOptions, event, awsContext, expectedStage);
  }

  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");
    const expectedStage = 'dev99';

    const context = {};

    // Dummy extract message from record functions
    const extractMessageFromRecord1 = (record, context) => record;
    const loadTaskTrackingState1 = (messages, context) => messages;
    const saveTaskTrackingState1 = (messages, context) => messages;
    const handleIncompleteMessages1 = (messages, context) => messages;
    const discardUnusableRecords1 = (records, context) => records;
    const discardRejectedMessages1 = (rejectedMessages, messages, context) => messages;

    t.notOk(isStreamProcessingConfigured(context), `Default stream processing must NOT be configured yet`);

    // Generate a sample AWS event
    const event = sampleAwsEvent('TestStream_DEV2', 'partitionKey', '', false);

    // Generate a sample AWS context
    const awsContext = sampleAwsContext('1.0.1', 'dev1');

    // Configure defaults for the first time
    const stdOptions = require('../default-kinesis-options.json');
    const options = stdOptions.streamProcessingOptions;

    const expectedSettings = toSettings(options.streamType, options.taskTrackingName, options.timeoutAtPercentageOfRemainingTime, options.maxNumberOfAttempts, extractJsonMessageFromKinesisRecord, skipLoadTaskTrackingState, skipSaveTaskTrackingState, resubmitIncompleteMessagesToKinesis, discardUnusableRecordsToDRQ, discardRejectedMessagesToDMQ, undefined, options.deadRecordQueueName, options.deadMessageQueueName);

    checkConfigureDefaultKinesisStreamProcessing(context, options, undefined, stdOptions, event, awsContext, false, expectedSettings, expectedStage);

    // Force a totally different configuration to overwrite the default Kinesis configuration
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    const dynamoDBSettings = toSettings(DYNAMODB_STREAM_TYPE, 'myTasks_Override', 0.91, 7, extractMessageFromRecord1, loadTaskTrackingState1, saveTaskTrackingState1, handleIncompleteMessages1, discardUnusableRecords1, discardRejectedMessages1, 'taskTrackingTableName', 'DRQ', 'DMQ');
    checkConfigureStreamProcessingWithSettings(t, context, dynamoDBSettings, undefined, require('../default-dynamodb-options.json'), event, awsContext, true, dynamoDBSettings, expectedStage);

    // Don't force the default configuration back again
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    checkConfigureDefaultKinesisStreamProcessing(context, options, undefined, stdOptions, event, awsContext, false, dynamoDBSettings, expectedStage);

    // Force the default configuration back again
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    checkConfigureDefaultKinesisStreamProcessing(context, options, undefined, stdOptions, event, awsContext, true, expectedSettings, expectedStage);

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

// =====================================================================================================================
// configureDefaultDynamoDBStreamProcessing without event & awsContext
// =====================================================================================================================

test('configureDefaultDynamoDBStreamProcessing without event & awsContext', t => {
  function checkConfigureDefaultDynamoDBStreamProcessing(context, options, stdSettings, stdOptions, forceConfiguration, expectedSettings) {
    const before = context.streamProcessing;
    const mustChange = forceConfiguration || !isStreamProcessingConfigured(context);

    const c = configureDefaultDynamoDBStreamProcessing(context, options, stdSettings, stdOptions, undefined, undefined, forceConfiguration);

    t.ok(c === context, `Context returned must be given context`);
    checkSettings(t, context, before, mustChange, expectedSettings);
    checkDependencies(t, context, stdSettings, stdOptions, undefined, undefined, undefined);
  }

  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

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
    const stdOptions = require('../default-dynamodb-options.json');
    const options = stdOptions.streamProcessingOptions;

    const expectedSettings = toSettings(options.streamType, options.taskTrackingName, options.timeoutAtPercentageOfRemainingTime, options.maxNumberOfAttempts, useStreamEventRecordAsMessage, loadTaskTrackingStateFromDynamoDB, saveTaskTrackingStateToDynamoDB, replayAllMessagesIfIncomplete, discardUnusableRecordsToDRQ, discardRejectedMessagesToDMQ, options.taskTrackingTableName, options.deadRecordQueueName, options.deadMessageQueueName);

    checkConfigureDefaultDynamoDBStreamProcessing(context, options, undefined, stdOptions, false, expectedSettings);

    // Force a totally different configuration to overwrite the default DynamoDB configuration
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    const kinesisSettings = toSettings(KINESIS_STREAM_TYPE, 'myTasks_Override', 0.91, 7, extractMessageFromRecord1, loadTaskTrackingState1, saveTaskTrackingState1, handleIncompleteMessages1, discardUnusableRecords1, discardRejectedMessages1, 'taskTrackingTableName', 'DRQ', 'DMQ');
    checkConfigureStreamProcessingWithSettings(t, context, kinesisSettings, undefined, require('../default-kinesis-options.json'), undefined, undefined, true, kinesisSettings, undefined);

    // Don't force the default configuration back again
    checkConfigureDefaultDynamoDBStreamProcessing(context, options, undefined, stdOptions, false, kinesisSettings);

    // Force the default configuration back again
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    checkConfigureDefaultDynamoDBStreamProcessing(context, options, undefined, stdOptions, true, expectedSettings);

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

// =====================================================================================================================
// configureDefaultDynamoDBStreamProcessing with event & awsContext
// =====================================================================================================================

test('configureDefaultDynamoDBStreamProcessing with event & awsContext', t => {
  function checkConfigureDefaultDynamoDBStreamProcessing(context, options, stdSettings, stdOptions, event, awsContext, forceConfiguration, expectedSettings, expectedStage) {
    const before = context.streamProcessing;
    const mustChange = forceConfiguration || !isStreamProcessingConfigured(context);

    const c = configureDefaultDynamoDBStreamProcessing(context, options, stdSettings, stdOptions, event, awsContext, forceConfiguration);

    t.ok(c === context, `Context returned must be given context`);
    checkSettings(t, context, before, mustChange, expectedSettings);
    checkDependencies(t, context, stdSettings, stdOptions, event, awsContext, expectedStage);
  }

  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");
    const expectedStage = 'dev99';

    const context = {};

    // Dummy extract message from record functions
    const extractMessageFromRecord1 = (record, context) => record;
    const loadTaskTrackingState1 = (messages, context) => messages;
    const saveTaskTrackingState1 = (messages, context) => messages;
    const handleIncompleteMessages1 = (messages, context) => messages;
    const discardUnusableRecords1 = (records, context) => records;
    const discardRejectedMessages1 = (rejectedMessages, messages, context) => messages;

    t.notOk(isStreamProcessingConfigured(context), `Default stream processing must NOT be configured yet`);

    // Generate a sample AWS event
    const event = sampleAwsEvent('TestStream_DEV2', 'partitionKey', '', false);

    // Generate a sample AWS context
    const awsContext = sampleAwsContext('1.0.1', 'dev1');

    // Configure defaults for the first time
    const stdOptions = require('../default-dynamodb-options.json');
    const options = stdOptions.streamProcessingOptions;

    const expectedSettings = toSettings(options.streamType, options.taskTrackingName, options.timeoutAtPercentageOfRemainingTime, options.maxNumberOfAttempts, useStreamEventRecordAsMessage, loadTaskTrackingStateFromDynamoDB, saveTaskTrackingStateToDynamoDB, replayAllMessagesIfIncomplete, discardUnusableRecordsToDRQ, discardRejectedMessagesToDMQ, options.taskTrackingTableName, options.deadRecordQueueName, options.deadMessageQueueName);

    checkConfigureDefaultDynamoDBStreamProcessing(context, options, undefined, stdOptions, event, awsContext, false, expectedSettings, expectedStage);

    // Force a totally different configuration to overwrite the default DynamoDB configuration
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    const kinesisSettings = toSettings(KINESIS_STREAM_TYPE, 'myTasks_Override', 0.91, 7, extractMessageFromRecord1, loadTaskTrackingState1, saveTaskTrackingState1, handleIncompleteMessages1, discardUnusableRecords1, discardRejectedMessages1, 'taskTrackingTableName', 'DRQ', 'DMQ');
    checkConfigureStreamProcessingWithSettings(t, context, kinesisSettings, undefined, require('../default-kinesis-options.json'), event, awsContext, true, kinesisSettings, expectedStage);

    // Don't force the default configuration back again
    checkConfigureDefaultDynamoDBStreamProcessing(context, options, undefined, stdOptions, event, awsContext, false, kinesisSettings, expectedStage);

    // Force the default configuration back again
    context.kinesis = undefined;
    context.dynamoDBDocClient = undefined;
    deleteCachedInstances();
    checkConfigureDefaultDynamoDBStreamProcessing(context, options, undefined, stdOptions, event, awsContext, true, expectedSettings, expectedStage);

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

// =====================================================================================================================
// getStreamProcessingSetting and getStreamProcessingFunction
// =====================================================================================================================

test('getStreamProcessingSetting and getStreamProcessingFunction', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

    const context = {};
    const options = require('../default-kinesis-options.json');

    // Configure default stream processing settings
    configureDefaultKinesisStreamProcessing(context);

    const defaultSettings = getDefaultKinesisStreamProcessingSettings(options.streamProcessingOptions);

    t.equal(getStreamProcessingSetting(context, STREAM_TYPE_SETTING), defaultSettings.streamType, `streamType setting must be ${defaultSettings.streamType}`);
    t.equal(getStreamProcessingSetting(context, TASK_TRACKING_NAME_SETTING), defaultSettings.taskTrackingName, `taskTrackingName setting must be ${defaultSettings.taskTrackingName}`);
    t.equal(getStreamProcessingSetting(context, TIMEOUT_AT_PERCENTAGE_OF_REMAINING_TIME_SETTING), defaultSettings.timeoutAtPercentageOfRemainingTime, `timeoutAtPercentageOfRemainingTime setting must be ${defaultSettings.timeoutAtPercentageOfRemainingTime}`);
    t.equal(getStreamProcessingSetting(context, MAX_NUMBER_OF_ATTEMPTS_SETTING), defaultSettings.maxNumberOfAttempts, `maxNumberOfAttempts setting must be ${defaultSettings.maxNumberOfAttempts}`);

    t.equal(getStreamProcessingFunction(context, EXTRACT_MESSAGE_FROM_RECORD_SETTING), extractJsonMessageFromKinesisRecord, `extractMessageFromRecord function must be ${stringify(extractJsonMessageFromKinesisRecord)}`);
    t.equal(getStreamProcessingFunction(context, DISCARD_UNUSABLE_RECORDS_SETTING), discardUnusableRecordsToDRQ, `discardUnusableRecords function must be ${stringify(discardUnusableRecordsToDRQ)}`);
    t.equal(getStreamProcessingFunction(context, DISCARD_REJECTED_MESSAGES_SETTING), discardRejectedMessagesToDMQ, `discardRejectedMessages function must be ${stringify(discardRejectedMessagesToDMQ)}`);
    t.equal(getStreamProcessingFunction(context, HANDLE_INCOMPLETE_MESSAGES_SETTING), resubmitIncompleteMessagesToKinesis, `handleIncompleteMessages function must be ${stringify(resubmitIncompleteMessagesToKinesis)}`);

    t.equal(getStreamProcessingSetting(context, DEAD_RECORD_QUEUE_NAME_SETTING), defaultSettings.deadRecordQueueName, `deadRecordQueueName setting must be ${defaultSettings.deadRecordQueueName}`);
    t.equal(getStreamProcessingSetting(context, DEAD_MESSAGE_QUEUE_NAME_SETTING), defaultSettings.deadMessageQueueName, `deadMessageQueueName setting must be ${defaultSettings.deadMessageQueueName}`);

    t.equal(getExtractMessageFromRecordFunction(context), extractJsonMessageFromKinesisRecord, `extractMessageFromRecord function must be ${stringify(extractJsonMessageFromKinesisRecord)}`);
    t.equal(getDiscardUnusableRecordsFunction(context), discardUnusableRecordsToDRQ, `discardUnusableRecords function must be ${stringify(discardUnusableRecordsToDRQ)}`);
    t.equal(getDiscardRejectedMessagesFunction(context), discardRejectedMessagesToDMQ, `discardRejectedMessages function must be ${stringify(discardRejectedMessagesToDMQ)}`);
    t.equal(getHandleIncompleteMessagesFunction(context), resubmitIncompleteMessagesToKinesis, `handleIncompleteMessages function must be ${stringify(resubmitIncompleteMessagesToKinesis)}`);

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

// =====================================================================================================================
// extractJsonMessageFromKinesisRecord
// =====================================================================================================================

test('extractJsonMessageFromKinesisRecord', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

    const context = {};
    logging.configureDefaultLogging(context);
    configureDefaultKinesisStreamProcessing(context, undefined, undefined, undefined, undefined, undefined, true);

    const eventSourceARN = samples.sampleKinesisEventSourceArn('eventSourceArnRegion', 'TestStream_DEV');
    const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');
    const msg = sampleMessage();

    // Parse the non-JSON message and expect an error
    t.throws(() => extractJsonMessageFromKinesisRecord(record, context), SyntaxError, `parsing a non-JSON message must throw an error`);

    record.kinesis.data = base64.toBase64(msg);

    const message = extractJsonMessageFromKinesisRecord(record, context);

    t.ok(message, 'JSON message must be extracted');
    t.deepEqual(message, msg, 'JSON message must match original');

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

// =====================================================================================================================
// extractJsonMessageFromKinesisRecord
// =====================================================================================================================

test('useStreamEventRecordAsMessage', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

    const context = {};
    configureDefaultDynamoDBStreamProcessing(context, undefined, undefined, require('../default-dynamodb-options.json'), undefined, undefined, true);

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

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

// =====================================================================================================================
// discardUnusableRecordsToDRQ
// =====================================================================================================================

test('discardUnusableRecordsToDRQ with 0 records', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

    const context = {
      kinesis: dummyKinesis(t, 'discardUnusableRecordsToDRQ', undefined)
    };
    logging.configureDefaultLogging(context);

    const eventSourceARN = samples.sampleKinesisEventSourceArn('eventSourceArnRegion', 'TestStream_DEV');
    const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');

    const awsContext = samples.sampleAwsContext('functionName', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'functionName', '1.0.1'));
    const event = samples.sampleKinesisEventWithRecords([record]);
    stages.configureDefaultStageHandling(context, false);
    stages.configureStage(context, event, awsContext, true);

    configureDefaultKinesisStreamProcessing(context, undefined, undefined, undefined, event, awsContext, true);

    t.plan(1);
    discardUnusableRecordsToDRQ([], context)
      .then(results => {
        t.equal(results.length, 0, `discardUnusableRecordsToDRQ results (${results.length}) must be 0`);
      })
      .catch(err => {
        t.fail(`discardUnusableRecordsToDRQ expected no failure - error: ${err.stack}`);
      });

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('discardUnusableRecordsToDRQ with 1 record', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

    const context = {
      kinesis: dummyKinesis(t, 'discardUnusableRecordsToDRQ', undefined)
    };
    logging.configureDefaultLogging(context);

    const eventSourceARN = samples.sampleKinesisEventSourceArn('eventSourceArnRegion', 'TestStream_DEV');
    const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');

    const awsContext = samples.sampleAwsContext('functionName', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'functionName', '1.0.1'));
    const event = samples.sampleKinesisEventWithRecords([record]);
    stages.configureDefaultStageHandling(context, false);
    stages.configureStage(context, event, awsContext, true);

    configureDefaultKinesisStreamProcessing(context, undefined, undefined, undefined, event, awsContext, true);

    t.plan(2);
    discardUnusableRecordsToDRQ([record], context)
      .then(results => {
        t.equal(results.length, 1, `discardUnusableRecordsToDRQ results (${results.length}) must be 1`);
      })
      .catch(err => {
        t.fail(`discardUnusableRecordsToDRQ expected no failure - error: ${err.stack}`);
      });

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('discardUnusableRecordsToDRQ with 2 records', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

    const context = {
      kinesis: dummyKinesis(t, 'discardUnusableRecordsToDRQ', undefined)
    };
    logging.configureDefaultLogging(context);

    const eventSourceARN = samples.sampleKinesisEventSourceArn('eventSourceArnRegion', 'TestStream_DEV');
    const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');
    const record2 = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');

    const awsContext = samples.sampleAwsContext('functionName', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'functionName', '1.0.1'));
    const records = [record, record2];
    const event = samples.sampleKinesisEventWithRecords(records);
    stages.configureDefaultStageHandling(context, false);
    stages.configureStage(context, event, awsContext, true);

    configureDefaultKinesisStreamProcessing(context, undefined, undefined, undefined, event, awsContext, true);

    t.plan(3);
    discardUnusableRecordsToDRQ(records, context)
      .then(results => {
        t.equal(results.length, 2, `discardUnusableRecordsToDRQ results (${results.length}) must be 2`);
      })
      .catch(err => {
        t.fail(`discardUnusableRecordsToDRQ expected no failure - error: ${err.stack}`);
      });

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('discardUnusableRecordsToDRQ with 1 record and failure', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

    const error = new Error('Planned failure');
    const context = {
      kinesis: dummyKinesis(t, 'discardUnusableRecordsToDRQ', error)
    };
    logging.configureDefaultLogging(context);

    const eventSourceARN = samples.sampleKinesisEventSourceArn('eventSourceArnRegion', 'TestStream_DEV');
    const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');

    const awsContext = samples.sampleAwsContext('functionName', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'functionName', '1.0.1'));
    const event = samples.sampleKinesisEventWithRecords([record]);
    stages.configureDefaultStageHandling(context, false);
    stages.configureStage(context, event, awsContext, true);

    configureDefaultKinesisStreamProcessing(context, undefined, undefined, undefined, event, awsContext, true);

    t.plan(2);
    discardUnusableRecordsToDRQ([record], context)
      .then(() => {
        t.fail(`discardUnusableRecordsToDRQ expected a failure`);
      })
      .catch(err => {
        t.equal(err, error, `discardUnusableRecordsToDRQ error (${err}) must be ${error}`);
      });

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

// =====================================================================================================================
// discardRejectedMessagesToDMQ
// =====================================================================================================================

test('discardRejectedMessagesToDMQ with 0 messages', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

    const context = {
      kinesis: dummyKinesis(t, 'discardRejectedMessagesToDMQ', undefined)
    };
    logging.configureDefaultLogging(context);

    const eventSourceARN = samples.sampleKinesisEventSourceArn('eventSourceArnRegion', 'TestStream_DEV');
    const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');

    const awsContext = samples.sampleAwsContext('functionName', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'functionName', '1.0.1'));
    const event = samples.sampleKinesisEventWithRecords([record]);
    stages.configureDefaultStageHandling(context, false);
    stages.configureStage(context, event, awsContext, true);

    configureDefaultKinesisStreamProcessing(context, undefined, undefined, undefined, event, awsContext, true);

    t.plan(1);
    discardRejectedMessagesToDMQ([], context)
      .then(results => {
        t.equal(results.length, 0, `discardRejectedMessagesToDMQ results (${results.length}) must be 0`);
      })
      .catch(err => {
        t.fail(`discardRejectedMessagesToDMQ expected no failure - error: ${err.stack}`);
      });

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('discardRejectedMessagesToDMQ with 1 message', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

    const context = {
      kinesis: dummyKinesis(t, 'discardRejectedMessagesToDMQ', undefined)
    };
    logging.configureDefaultLogging(context);

    const eventSourceARN = samples.sampleKinesisEventSourceArn('eventSourceArnRegion', 'TestStream_DEV');
    const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');
    const message = sampleMessage();
    message.taskTracking = {record: record};

    const awsContext = samples.sampleAwsContext('functionName', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'functionName', '1.0.1'));
    const event = samples.sampleKinesisEventWithRecords([record]);
    stages.configureDefaultStageHandling(context, false);
    stages.configureStage(context, event, awsContext, true);

    configureDefaultKinesisStreamProcessing(context, undefined, undefined, undefined, event, awsContext, true);

    t.plan(2);
    discardRejectedMessagesToDMQ([message], context)
      .then(results => {
        t.equal(results.length, 1, `discardRejectedMessagesToDMQ results (${results.length}) must be 1`);
      })
      .catch(err => {
        t.fail(`discardRejectedMessagesToDMQ expected no failure - error: ${err.stack}`);
      });

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('discardRejectedMessagesToDMQ with 2 messages', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

    const context = {
      kinesis: dummyKinesis(t, 'discardRejectedMessagesToDMQ', undefined)
    };
    logging.configureDefaultLogging(context);

    const eventSourceARN = samples.sampleKinesisEventSourceArn('eventSourceArnRegion', 'TestStream_DEV');
    const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');
    const message = sampleMessage();
    message.taskTracking = {record: record};

    const record2 = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');
    const message2 = sampleMessage();
    message2.taskTracking = {record: record2};

    const awsContext = samples.sampleAwsContext('functionName', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'functionName', '1.0.1'));
    const records = [record, record2];
    const messages = [message, message2];
    const event = samples.sampleKinesisEventWithRecords(records);
    stages.configureDefaultStageHandling(context, false);
    stages.configureStage(context, event, awsContext, true);

    configureDefaultKinesisStreamProcessing(context, undefined, undefined, undefined, event, awsContext, true);

    t.plan(3);
    discardRejectedMessagesToDMQ(messages, context)
      .then(results => {
        t.equal(results.length, 2, `discardRejectedMessagesToDMQ results (${results.length}) must be 2`);
      })
      .catch(err => {
        t.fail(`discardRejectedMessagesToDMQ expected no failure - error: ${err.stack}`);
      });

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('discardRejectedMessagesToDMQ with 1 record and failure', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

    const error = new Error('Planned failure');
    const context = {
      kinesis: dummyKinesis(t, 'discardRejectedMessagesToDMQ', error)
    };
    logging.configureDefaultLogging(context);

    const eventSourceARN = samples.sampleKinesisEventSourceArn('eventSourceArnRegion', 'TestStream_DEV');
    const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');
    const message = sampleMessage();
    message.taskTracking = {record: record};

    const awsContext = samples.sampleAwsContext('functionName', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'functionName', '1.0.1'));
    const event = samples.sampleKinesisEventWithRecords([record]);
    stages.configureDefaultStageHandling(context, false);
    stages.configureStage(context, event, awsContext, true);

    configureDefaultKinesisStreamProcessing(context, undefined, undefined, undefined, event, awsContext, true);

    t.plan(2);
    discardRejectedMessagesToDMQ([message], context)
      .then(() => {
        t.fail(`discardRejectedMessagesToDMQ expected a failure`);
      })
      .catch(err => {
        t.equal(err, error, `discardRejectedMessagesToDMQ error (${err}) must be ${error}`);
      });

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

// =====================================================================================================================
// resubmitIncompleteMessagesToKinesis
// =====================================================================================================================

test('resubmitIncompleteMessagesToKinesis with 0 messages', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

    const context = {
      kinesis: dummyKinesis(t, 'resubmitIncompleteMessagesToKinesis', undefined)
    };
    logging.configureDefaultLogging(context);

    const streamName = 'TestStream_DEV';
    const eventSourceARN = samples.sampleKinesisEventSourceArn('eventSourceArnRegion', streamName);
    const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');

    const awsContext = samples.sampleAwsContext('functionName', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'functionName', '1.0.1'));
    const event = samples.sampleKinesisEventWithRecords([record]);
    stages.configureDefaultStageHandling(context, false);
    stages.configureStage(context, event, awsContext, true);

    configureDefaultKinesisStreamProcessing(context, undefined, undefined, undefined, event, awsContext, true);

    t.plan(1);
    resubmitIncompleteMessagesToKinesis([], [], context)
      .then(results => {
        t.equal(results.length, 0, `resubmitIncompleteMessagesToKinesis results (${results.length}) must be 0`);
      })
      .catch(err => {
        t.fail(`resubmitIncompleteMessagesToKinesis expected no failure - error: ${err.stack}`);
      });

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('resubmitIncompleteMessagesToKinesis with 1 message', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

    const context = {
      kinesis: dummyKinesis(t, 'resubmitIncompleteMessagesToKinesis', undefined)
    };
    logging.configureDefaultLogging(context);

    const streamName = 'TestStream_DEV';
    const eventSourceARN = samples.sampleKinesisEventSourceArn('eventSourceArnRegion', streamName);
    const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');
    const message = sampleMessage();
    message.taskTracking = {record: record};

    const awsContext = samples.sampleAwsContext('functionName', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'functionName', '1.0.1'));
    const event = samples.sampleKinesisEventWithRecords([record]);
    stages.configureDefaultStageHandling(context, false);
    stages.configureStage(context, event, awsContext, true);

    configureDefaultKinesisStreamProcessing(context, undefined, undefined, undefined, event, awsContext, true);

    t.plan(2);
    resubmitIncompleteMessagesToKinesis([message], [message], context)
      .then(results => {
        t.equal(results.length, 1, `resubmitIncompleteMessagesToKinesis results (${results.length}) must be 1`);
      })
      .catch(err => {
        t.fail(`resubmitIncompleteMessagesToKinesis expected no failure - error: ${err.stack}`);
      });

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('resubmitIncompleteMessagesToKinesis with 2 messages', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

    const context = {
      kinesis: dummyKinesis(t, 'resubmitIncompleteMessagesToKinesis', undefined)
    };
    logging.configureDefaultLogging(context);

    const streamName = 'TestStream_DEV';
    const eventSourceARN = samples.sampleKinesisEventSourceArn('eventSourceArnRegion', streamName);
    const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');
    const message = sampleMessage();
    message.taskTracking = {record: record};

    const record2 = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');
    const message2 = sampleMessage();
    message2.taskTracking = {record: record2};

    const awsContext = samples.sampleAwsContext('functionName', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'functionName', '1.0.1'));
    const records = [record, record2];
    const messages = [message, message2];
    const event = samples.sampleKinesisEventWithRecords(records);
    stages.configureDefaultStageHandling(context, false);
    stages.configureStage(context, event, awsContext, true);

    configureDefaultKinesisStreamProcessing(context, undefined, undefined, undefined, event, awsContext, true);

    t.plan(3);
    resubmitIncompleteMessagesToKinesis(messages, messages, context)
      .then(results => {
        t.equal(results.length, 2, `resubmitIncompleteMessagesToKinesis results (${results.length}) must be 2`);
      })
      .catch(err => {
        t.fail(`resubmitIncompleteMessagesToKinesis expected no failure - error: ${err.stack}`);
      });

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});

test('resubmitIncompleteMessagesToKinesis with 1 record and failure', t => {
  try {
    setRegionStageAndDeleteCachedInstances('us-west-1', "dev99");

    const error = new Error('Planned failure');
    const context = {
      kinesis: dummyKinesis(t, 'resubmitIncompleteMessagesToKinesis', error)
    };
    logging.configureDefaultLogging(context);

    const streamName = 'TestStream_DEV';
    const eventSourceARN = samples.sampleKinesisEventSourceArn('eventSourceArnRegion', streamName);
    const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');
    const message = sampleMessage();
    message.taskTracking = {record: record};

    const awsContext = samples.sampleAwsContext('functionName', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'functionName', '1.0.1'));
    const event = samples.sampleKinesisEventWithRecords([record]);
    stages.configureDefaultStageHandling(context, false);
    stages.configureStage(context, event, awsContext, true);

    configureDefaultKinesisStreamProcessing(context, undefined, undefined, undefined, event, awsContext, true);

    t.plan(2);
    resubmitIncompleteMessagesToKinesis([message], [message], context)
      .then(() => {
        t.fail(`resubmitIncompleteMessagesToKinesis expected a failure`);
      })
      .catch(err => {
        t.equal(err, error, `resubmitIncompleteMessagesToKinesis error (${err}) must be ${error}`);
      });

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
});
