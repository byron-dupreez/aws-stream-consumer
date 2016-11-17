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
const configureDefaultKinesisStreamProcessing = streamProcessing.configureDefaultKinesisStreamProcessing;
const getDefaultKinesisStreamProcessingSettings = streamProcessing.getDefaultKinesisStreamProcessingSettings;

const getStreamProcessingSetting = streamProcessing.getStreamProcessingSetting;
const getStreamProcessingFunction = streamProcessing.getStreamProcessingFunction;

// Convenience accessors for specific stream processing functions
const getExtractMessageFromRecordFunction = streamProcessing.getExtractMessageFromRecordFunction;
const getDiscardUnusableRecordsFunction = streamProcessing.getDiscardUnusableRecordsFunction;
const getDiscardRejectedMessagesFunction = streamProcessing.getDiscardRejectedMessagesFunction;
const getResubmitIncompleteMessagesFunction = streamProcessing.getResubmitIncompleteMessagesFunction;

// Default extractMessageFromRecord functions
const extractJsonMessageFromKinesisRecord = streamProcessing.DEFAULTS.extractJsonMessageFromKinesisRecord;
// Default discardUnusableRecords functions
const discardUnusableRecordsToDRQ = streamProcessing.DEFAULTS.discardUnusableRecordsToDRQ;
// Default discardRejectedMessages functions
const discardRejectedMessagesToDMQ = streamProcessing.DEFAULTS.discardRejectedMessagesToDMQ;
// Default resubmitIncompleteMessages functions
const resubmitIncompleteMessagesToKinesis = streamProcessing.DEFAULTS.resubmitIncompleteMessagesToKinesis;

// Generic settings names
const STREAM_TYPE_SETTING = streamProcessing.STREAM_TYPE_SETTING;
const TASK_TRACKING_NAME_SETTING = streamProcessing.TASK_TRACKING_NAME_SETTING;
const TIMEOUT_AT_PERCENTAGE_OF_REMAINING_TIME_SETTING = streamProcessing.TIMEOUT_AT_PERCENTAGE_OF_REMAINING_TIME_SETTING;
const MAX_NUMBER_OF_ATTEMPTS_SETTING = streamProcessing.MAX_NUMBER_OF_ATTEMPTS_SETTING;

// Processing function settings names
const EXTRACT_MESSAGE_FROM_RECORD_SETTING = streamProcessing.EXTRACT_MESSAGE_FROM_RECORD_SETTING;
const DISCARD_UNUSABLE_RECORDS_SETTING = streamProcessing.DISCARD_UNUSABLE_RECORDS_SETTING;
const DISCARD_REJECTED_MESSAGES_SETTING = streamProcessing.DISCARD_REJECTED_MESSAGES_SETTING;
const RESUBMIT_INCOMPLETE_MESSAGES_SETTING = streamProcessing.RESUBMIT_INCOMPLETE_MESSAGES_SETTING;

// Specialised settings names
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

function checkConfigureStreamProcessing(t, context, streamType, taskTrackingName, timeoutAtPercentageOfRemainingTime,
  maxNumberOfAttempts, extractMessageFromRecord, discardUnusableRecords, discardRejectedMessages,
  resubmitIncompleteMessages, deadRecordQueueName, deadMessageQueueName, forceConfiguration) {

  const mustChange = forceConfiguration || !isStreamProcessingConfigured(context);

  const before = context.streamProcessing;

  const settings = {
    // generic settings
    streamType: streamType,
    taskTrackingName: taskTrackingName,
    timeoutAtPercentageOfRemainingTime: timeoutAtPercentageOfRemainingTime,
    maxNumberOfAttempts: maxNumberOfAttempts,
    // configurable processing functions
    extractMessageFromRecord: extractMessageFromRecord,
    discardUnusableRecords: discardUnusableRecords,
    discardRejectedMessages: discardRejectedMessages,
    resubmitIncompleteMessages: resubmitIncompleteMessages,
    // specialised settings needed by default implementations
    deadRecordQueueName: deadRecordQueueName,
    deadMessageQueueName: deadMessageQueueName
  };

  const c = configureStreamProcessing(context, settings, forceConfiguration);

  t.ok(c === context, `Context (${c}) returned must be given context`);
  t.ok(isStreamProcessingConfigured(context), `Stream processing must be configured now`);

  const after = context.streamProcessing;
  equal(t, after.streamType, mustChange ? streamType : before.streamType, 'streamType');
  equal(t, after.taskTrackingName, mustChange ? taskTrackingName : before.taskTrackingName, 'taskTrackingName');
  equal(t, after.timeoutAtPercentageOfRemainingTime, mustChange ? timeoutAtPercentageOfRemainingTime : before.timeoutAtPercentageOfRemainingTime, 'timeoutAtPercentageOfRemainingTime');
  equal(t, after.maxNumberOfAttempts, mustChange ? maxNumberOfAttempts : before.maxNumberOfAttempts, 'maxNumberOfAttempts');
  equal(t, after.extractMessageFromRecord, mustChange ? extractMessageFromRecord : before.extractMessageFromRecord, 'extractMessageFromRecord');
  equal(t, after.discardUnusableRecords, mustChange ? discardUnusableRecords : before.discardUnusableRecords, 'discardUnusableRecords');
  equal(t, after.discardRejectedMessages, mustChange ? discardRejectedMessages : before.discardRejectedMessages, 'discardRejectedMessages');
  equal(t, after.resubmitIncompleteMessages, mustChange ? resubmitIncompleteMessages : before.resubmitIncompleteMessages, 'resubmitIncompleteMessages');
  equal(t, after.deadRecordQueueName, mustChange ? deadRecordQueueName : before.deadRecordQueueName, 'deadRecordQueueName');
  equal(t, after.deadMessageQueueName, mustChange ? deadMessageQueueName : before.deadMessageQueueName, 'deadMessageQueueName');
}

// =====================================================================================================================
// isStreamProcessingConfigured
// =====================================================================================================================

test('isStreamProcessingConfigured', t => {
  const context = {};
  t.notOk(isStreamProcessingConfigured(context), `Stream processing must NOT be configured yet`);

  configureDefaultKinesisStreamProcessing(context, true);

  t.ok(isStreamProcessingConfigured(context), `Stream processing must be configured now`);

  t.end();
});

// =====================================================================================================================
// configureStreamProcessing
// =====================================================================================================================

test('configureStreamProcessing', t => {
  function check(context, streamType, taskTrackingName, timeoutAtPercentageOfRemainingTime,
    maxNumberOfAttempts, extractMessageFromRecord, discardUnusableRecords, discardRejectedMessages,
    resubmitIncompleteMessages, deadRecordQueueName, deadMessageQueueName, forceConfiguration) {

    return checkConfigureStreamProcessing(t, context, streamType, taskTrackingName, timeoutAtPercentageOfRemainingTime,
      maxNumberOfAttempts, extractMessageFromRecord, discardUnusableRecords, discardRejectedMessages,
      resubmitIncompleteMessages, deadRecordQueueName, deadMessageQueueName, forceConfiguration);
  }

  const context = {};

  t.notOk(isStreamProcessingConfigured(context), `Stream processing must NOT be configured yet`);

  // Dummy extract message from record functions
  const extractMessageFromRecord1 = (record, context) => record;
  const extractMessageFromRecord2 = (record, context) => record;

  const discardUnusableRecords1 = (unusableRecords, context) => unusableRecords;
  const discardUnusableRecords2 = (unusableRecords, context) => unusableRecords;

  const discardRejectedMessages1 = (rejectedMessages, context) => rejectedMessages;
  const discardRejectedMessages2 = (rejectedMessages, context) => rejectedMessages;

  const resubmitIncompleteMessages1 = (incompleteMessages, streamName, context) => incompleteMessages;
  const resubmitIncompleteMessages2 = (incompleteMessages, streamName, context) => incompleteMessages;

  // Configure for the first time
  check(context, DYNAMODB_STREAM_TYPE, 'myTasks', 0.75, 2, extractMessageFromRecord1, discardUnusableRecords1, discardRejectedMessages1, resubmitIncompleteMessages1, 'DRQ1', 'DMQ1', false);

  // Don't force a different configuration
  check(context, KINESIS_STREAM_TYPE, 'myTasks_NoOverride', 0.81, 77, extractMessageFromRecord2, discardUnusableRecords2, discardRejectedMessages2, resubmitIncompleteMessages2, 'DRQ2', 'DMQ2', false);

  // Force a new configuration
  check(context, KINESIS_STREAM_TYPE, 'myTasks_Override', 0.91, 7, extractMessageFromRecord2, discardUnusableRecords2, discardRejectedMessages2, resubmitIncompleteMessages2, 'DRQ3', 'DMQ2', true);

  t.end();
});

// =====================================================================================================================
// configureDefaultKinesisStreamProcessing
// =====================================================================================================================

test('configureDefaultKinesisStreamProcessing', t => {
  // Set up region
  regions.ONLY_FOR_TESTING.setRegionIfNotSet('us-west-1');
  const region = regions.getRegion();

  const config = require('../config.json');

  function check(context, streamType, taskTrackingName, timeoutAtPercentageOfRemainingTime, maxNumberOfAttempts,
    extractMessageFromRecord, discardUnusableRecords, discardRejectedMessages, resubmitIncompleteMessages,
    deadRecordQueueName, deadMessageQueueName, forceConfiguration) {

    const mustChange = forceConfiguration || !isStreamProcessingConfigured(context);

    const before = context.streamProcessing;

    const c = configureDefaultKinesisStreamProcessing(context, forceConfiguration);

    t.ok(c === context, `Context (${c}) returned must be given context`);
    t.ok(isStreamProcessingConfigured(context), `Default stream processing must be configured now`);

    const after = context.streamProcessing;
    equal(t, after.streamType, mustChange ? streamType : before.streamType, 'streamType');
    equal(t, after.taskTrackingName, mustChange ? taskTrackingName : before.taskTrackingName, 'taskTrackingName');
    equal(t, after.timeoutAtPercentageOfRemainingTime, mustChange ? timeoutAtPercentageOfRemainingTime : before.timeoutAtPercentageOfRemainingTime, 'timeoutAtPercentageOfRemainingTime');
    equal(t, after.maxNumberOfAttempts, mustChange ? maxNumberOfAttempts : before.maxNumberOfAttempts, 'maxNumberOfAttempts');
    equal(t, after.extractMessageFromRecord, mustChange ? extractMessageFromRecord : before.extractMessageFromRecord, 'extractMessageFromRecord');
    equal(t, after.discardUnusableRecords, mustChange ? discardUnusableRecords : before.discardUnusableRecords, 'discardUnusableRecords');
    equal(t, after.discardRejectedMessages, mustChange ? discardRejectedMessages : before.discardRejectedMessages, 'discardRejectedMessages');
    equal(t, after.resubmitIncompleteMessages, mustChange ? resubmitIncompleteMessages : before.resubmitIncompleteMessages, 'resubmitIncompleteMessages');
    equal(t, after.deadRecordQueueName, mustChange ? deadRecordQueueName : before.deadRecordQueueName, 'deadRecordQueueName');
    equal(t, after.deadMessageQueueName, mustChange ? deadMessageQueueName : before.deadMessageQueueName, 'deadMessageQueueName');

    // Check Kinesis is also configured
    t.ok(context.kinesis, 'context.kinesis must be configured');
    equal(t, context.kinesis.config.region, region, 'context.kinesis.config.region');
    equal(t, context.kinesis.config.maxRetries, config.kinesisOptions.maxRetries, 'context.kinesis.config.maxRetries');
  }

  const context = {};

  // Dummy extract message from record functions
  const extractMessageFromRecord1 = (record, context) => record;
  const discardUnusableRecords1 = (records, context) => records;
  const discardRejectedMessages1 = (rejectedMessages, messages, context) => messages;
  const resubmitIncompleteMessages1 = (messages, context) => messages;

  t.notOk(isStreamProcessingConfigured(context), `Default stream processing must NOT be configured yet`);

  // Configure defaults for the first time
  const defaultSettings = getDefaultKinesisStreamProcessingSettings(config.kinesisStreamProcessingSettings);

  check(context, defaultSettings.streamType, defaultSettings.taskTrackingName, defaultSettings.timeoutAtPercentageOfRemainingTime,
    defaultSettings.maxNumberOfAttempts, extractJsonMessageFromKinesisRecord, discardUnusableRecordsToDRQ,
    discardRejectedMessagesToDMQ, resubmitIncompleteMessagesToKinesis, defaultSettings.deadRecordQueueName,
    defaultSettings.deadMessageQueueName, false);

  // Force a different configuration
  checkConfigureStreamProcessing(t, context, DYNAMODB_STREAM_TYPE, 'myTasks_Override', 0.91, 7, extractMessageFromRecord1,
    discardUnusableRecords1, discardRejectedMessages1, resubmitIncompleteMessages1, 'DRQ', 'DMQ', true);

  // Don't force the default configuration back again
  check(context, defaultSettings.streamType, defaultSettings.taskTrackingName, defaultSettings.timeoutAtPercentageOfRemainingTime,
    defaultSettings.maxNumberOfAttempts, extractJsonMessageFromKinesisRecord, discardUnusableRecordsToDRQ,
    discardRejectedMessagesToDMQ, resubmitIncompleteMessagesToKinesis, defaultSettings.deadRecordQueueName,
    defaultSettings.deadMessageQueueName, false);

  // Force the default configuration back again
  check(context, defaultSettings.streamType, defaultSettings.taskTrackingName, defaultSettings.timeoutAtPercentageOfRemainingTime,
    defaultSettings.maxNumberOfAttempts, extractJsonMessageFromKinesisRecord, discardUnusableRecordsToDRQ,
    discardRejectedMessagesToDMQ, resubmitIncompleteMessagesToKinesis, defaultSettings.deadRecordQueueName,
    defaultSettings.deadMessageQueueName, true);

  t.end();
});

// =====================================================================================================================
// getStreamProcessingSetting and getStreamProcessingFunction
// =====================================================================================================================

test('getStreamProcessingSetting and getStreamProcessingFunction', t => {
  const context = {};
  const config = require('../config.json');

  // Configure default stream processing settings
  configureDefaultKinesisStreamProcessing(context);

  const defaultSettings = getDefaultKinesisStreamProcessingSettings(config.kinesisStreamProcessingSettings);

  equal(t, getStreamProcessingSetting(context, STREAM_TYPE_SETTING), defaultSettings.streamType, 'streamType setting');
  equal(t, getStreamProcessingSetting(context, TASK_TRACKING_NAME_SETTING), defaultSettings.taskTrackingName, 'taskTrackingName setting');
  equal(t, getStreamProcessingSetting(context, TIMEOUT_AT_PERCENTAGE_OF_REMAINING_TIME_SETTING), defaultSettings.timeoutAtPercentageOfRemainingTime, 'timeoutAtPercentageOfRemainingTime setting');
  equal(t, getStreamProcessingSetting(context, MAX_NUMBER_OF_ATTEMPTS_SETTING), defaultSettings.maxNumberOfAttempts, 'maxNumberOfAttempts setting');

  equal(t, getStreamProcessingFunction(context, EXTRACT_MESSAGE_FROM_RECORD_SETTING), extractJsonMessageFromKinesisRecord, 'extractMessageFromRecord function');
  equal(t, getStreamProcessingFunction(context, DISCARD_UNUSABLE_RECORDS_SETTING), discardUnusableRecordsToDRQ, 'discardUnusableRecords function');
  equal(t, getStreamProcessingFunction(context, DISCARD_REJECTED_MESSAGES_SETTING), discardRejectedMessagesToDMQ, 'discardRejectedMessages function');
  equal(t, getStreamProcessingFunction(context, RESUBMIT_INCOMPLETE_MESSAGES_SETTING), resubmitIncompleteMessagesToKinesis, 'resubmitIncompleteMessages function');

  equal(t, getStreamProcessingSetting(context, DEAD_RECORD_QUEUE_NAME_SETTING), defaultSettings.deadRecordQueueName, 'deadRecordQueueName setting');
  equal(t, getStreamProcessingSetting(context, DEAD_MESSAGE_QUEUE_NAME_SETTING), defaultSettings.deadMessageQueueName, 'deadMessageQueueName setting');

  equal(t, getExtractMessageFromRecordFunction(context), extractJsonMessageFromKinesisRecord, 'extractMessageFromRecord function');
  equal(t, getDiscardUnusableRecordsFunction(context), discardUnusableRecordsToDRQ, 'discardUnusableRecords function');
  equal(t, getDiscardRejectedMessagesFunction(context), discardRejectedMessagesToDMQ, 'discardRejectedMessages function');
  equal(t, getResubmitIncompleteMessagesFunction(context), resubmitIncompleteMessagesToKinesis, 'resubmitIncompleteMessages function');

  t.end();
});

// =====================================================================================================================
// extractJsonMessageFromKinesisRecord
// =====================================================================================================================

test('extractJsonMessageFromKinesisRecord', t => {
  const context = {};
  logging.configureDefaultLogging(context);
  configureDefaultKinesisStreamProcessing(context, true);

  const eventSourceARN = samples.sampleEventSourceArn('eventSourceArnRegion', 'TestStream_DEV');
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
// discardUnusableRecordsToDRQ
// =====================================================================================================================

test('discardUnusableRecordsToDRQ with 0 records', t => {
  const context = {
    kinesis: dummyKinesis(t, 'discardUnusableRecordsToDRQ', undefined)
  };
  logging.configureDefaultLogging(context);

  const eventSourceARN = samples.sampleEventSourceArn('eventSourceArnRegion', 'TestStream_DEV');
  const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');

  const awsContext = samples.sampleAwsContext('functionName', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'functionName', '1.0.1'));
  const event = samples.sampleKinesisEventWithRecords([record]);
  stages.configureDefaultStageHandling(context, false);
  stages.configureStage(context, event, awsContext, true);

  configureDefaultKinesisStreamProcessing(context, true);

  t.plan(1);
  discardUnusableRecordsToDRQ([], context)
    .then(results => {
      t.equal(results.length, 0, `discardUnusableRecordsToDRQ results (${results.length}) must be 0`);
    })
    .catch(err => {
      t.fail(`discardUnusableRecordsToDRQ expected no failure - error: ${err.stack}`);
    });
});

test('discardUnusableRecordsToDRQ with 1 record', t => {
  const context = {
    kinesis: dummyKinesis(t, 'discardUnusableRecordsToDRQ', undefined)
  };
  logging.configureDefaultLogging(context);

  const eventSourceARN = samples.sampleEventSourceArn('eventSourceArnRegion', 'TestStream_DEV');
  const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');

  const awsContext = samples.sampleAwsContext('functionName', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'functionName', '1.0.1'));
  const event = samples.sampleKinesisEventWithRecords([record]);
  stages.configureDefaultStageHandling(context, false);
  stages.configureStage(context, event, awsContext, true);

  configureDefaultKinesisStreamProcessing(context, true);

  t.plan(2);
  discardUnusableRecordsToDRQ([record], context)
    .then(results => {
      t.equal(results.length, 1, `discardUnusableRecordsToDRQ results (${results.length}) must be 1`);
    })
    .catch(err => {
      t.fail(`discardUnusableRecordsToDRQ expected no failure - error: ${err.stack}`);
    });
});

test('discardUnusableRecordsToDRQ with 2 records', t => {
  const context = {
    kinesis: dummyKinesis(t, 'discardUnusableRecordsToDRQ', undefined)
  };
  logging.configureDefaultLogging(context);

  const eventSourceARN = samples.sampleEventSourceArn('eventSourceArnRegion', 'TestStream_DEV');
  const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');
  const record2 = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');

  const awsContext = samples.sampleAwsContext('functionName', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'functionName', '1.0.1'));
  const records = [record, record2];
  const event = samples.sampleKinesisEventWithRecords(records);
  stages.configureDefaultStageHandling(context, false);
  stages.configureStage(context, event, awsContext, true);

  configureDefaultKinesisStreamProcessing(context, true);

  t.plan(3);
  discardUnusableRecordsToDRQ(records, context)
    .then(results => {
      t.equal(results.length, 2, `discardUnusableRecordsToDRQ results (${results.length}) must be 2`);
    })
    .catch(err => {
      t.fail(`discardUnusableRecordsToDRQ expected no failure - error: ${err.stack}`);
    });
});

test('discardUnusableRecordsToDRQ with 1 record and failure', t => {
  const error = new Error('Planned failure');
  const context = {
    kinesis: dummyKinesis(t, 'discardUnusableRecordsToDRQ', error)
  };
  logging.configureDefaultLogging(context);

  const eventSourceARN = samples.sampleEventSourceArn('eventSourceArnRegion', 'TestStream_DEV');
  const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');

  const awsContext = samples.sampleAwsContext('functionName', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'functionName', '1.0.1'));
  const event = samples.sampleKinesisEventWithRecords([record]);
  stages.configureDefaultStageHandling(context, false);
  stages.configureStage(context, event, awsContext, true);

  configureDefaultKinesisStreamProcessing(context, true);

  t.plan(2);
  discardUnusableRecordsToDRQ([record], context)
    .then(results => {
      t.fail(`discardUnusableRecordsToDRQ expected a failure`);
    })
    .catch(err => {
      t.equal(err, error, `discardUnusableRecordsToDRQ error (${err}) must be ${error}`);
    });
});

// =====================================================================================================================
// discardRejectedMessagesToDMQ
// =====================================================================================================================

test('discardRejectedMessagesToDMQ with 0 messages', t => {
  const context = {
    kinesis: dummyKinesis(t, 'discardRejectedMessagesToDMQ', undefined)
  };
  logging.configureDefaultLogging(context);

  const eventSourceARN = samples.sampleEventSourceArn('eventSourceArnRegion', 'TestStream_DEV');
  const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');

  const awsContext = samples.sampleAwsContext('functionName', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'functionName', '1.0.1'));
  const event = samples.sampleKinesisEventWithRecords([record]);
  stages.configureDefaultStageHandling(context, false);
  stages.configureStage(context, event, awsContext, true);

  configureDefaultKinesisStreamProcessing(context, true);

  t.plan(1);
  discardRejectedMessagesToDMQ([], context)
    .then(results => {
      t.equal(results.length, 0, `discardRejectedMessagesToDMQ results (${results.length}) must be 0`);
    })
    .catch(err => {
      t.fail(`discardRejectedMessagesToDMQ expected no failure - error: ${err.stack}`);
    });
});

test('discardRejectedMessagesToDMQ with 1 message', t => {
  const context = {
    kinesis: dummyKinesis(t, 'discardRejectedMessagesToDMQ', undefined)
  };
  logging.configureDefaultLogging(context);

  const eventSourceARN = samples.sampleEventSourceArn('eventSourceArnRegion', 'TestStream_DEV');
  const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');
  const message = sampleMessage();
  message.taskTracking = { record: record };

  const awsContext = samples.sampleAwsContext('functionName', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'functionName', '1.0.1'));
  const event = samples.sampleKinesisEventWithRecords([record]);
  stages.configureDefaultStageHandling(context, false);
  stages.configureStage(context, event, awsContext, true);

  configureDefaultKinesisStreamProcessing(context, true);

  t.plan(2);
  discardRejectedMessagesToDMQ([message], context)
    .then(results => {
      t.equal(results.length, 1, `discardRejectedMessagesToDMQ results (${results.length}) must be 1`);
    })
    .catch(err => {
      t.fail(`discardRejectedMessagesToDMQ expected no failure - error: ${err.stack}`);
    });
});

test('discardRejectedMessagesToDMQ with 2 messages', t => {
  const context = {
    kinesis: dummyKinesis(t, 'discardRejectedMessagesToDMQ', undefined)
  };
  logging.configureDefaultLogging(context);

  const eventSourceARN = samples.sampleEventSourceArn('eventSourceArnRegion', 'TestStream_DEV');
  const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');
  const message = sampleMessage();
  message.taskTracking = { record: record };

  const record2 = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');
  const message2 = sampleMessage();
  message2.taskTracking = { record: record2 };

  const awsContext = samples.sampleAwsContext('functionName', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'functionName', '1.0.1'));
  const records = [record, record2];
  const messages = [message, message2];
  const event = samples.sampleKinesisEventWithRecords(records);
  stages.configureDefaultStageHandling(context, false);
  stages.configureStage(context, event, awsContext, true);

  configureDefaultKinesisStreamProcessing(context, true);

  t.plan(3);
  discardRejectedMessagesToDMQ(messages, context)
    .then(results => {
      t.equal(results.length, 2, `discardRejectedMessagesToDMQ results (${results.length}) must be 2`);
    })
    .catch(err => {
      t.fail(`discardRejectedMessagesToDMQ expected no failure - error: ${err.stack}`);
    });
});

test('discardRejectedMessagesToDMQ with 1 record and failure', t => {
  const error = new Error('Planned failure');
  const context = {
    kinesis: dummyKinesis(t, 'discardRejectedMessagesToDMQ', error)
  };
  logging.configureDefaultLogging(context);

  const eventSourceARN = samples.sampleEventSourceArn('eventSourceArnRegion', 'TestStream_DEV');
  const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');
  const message = sampleMessage();
  message.taskTracking = { record: record };

  const awsContext = samples.sampleAwsContext('functionName', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'functionName', '1.0.1'));
  const event = samples.sampleKinesisEventWithRecords([record]);
  stages.configureDefaultStageHandling(context, false);
  stages.configureStage(context, event, awsContext, true);

  configureDefaultKinesisStreamProcessing(context, true);

  t.plan(2);
  discardRejectedMessagesToDMQ([message], context)
    .then(results => {
      t.fail(`discardRejectedMessagesToDMQ expected a failure`);
    })
    .catch(err => {
      t.equal(err, error, `discardRejectedMessagesToDMQ error (${err}) must be ${error}`);
    });
});


// =====================================================================================================================
// resubmitIncompleteMessagesToKinesis
// =====================================================================================================================

test('resubmitIncompleteMessagesToKinesis with 0 messages', t => {
  const context = {
    kinesis: dummyKinesis(t, 'resubmitIncompleteMessagesToKinesis', undefined)
  };
  logging.configureDefaultLogging(context);

  const streamName = 'TestStream_DEV';
  const eventSourceARN = samples.sampleEventSourceArn('eventSourceArnRegion', streamName);
  const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');

  const awsContext = samples.sampleAwsContext('functionName', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'functionName', '1.0.1'));
  const event = samples.sampleKinesisEventWithRecords([record]);
  stages.configureDefaultStageHandling(context, false);
  stages.configureStage(context, event, awsContext, true);

  configureDefaultKinesisStreamProcessing(context, true);

  t.plan(1);
  resubmitIncompleteMessagesToKinesis([], streamName, context)
    .then(results => {
      t.equal(results.length, 0, `resubmitIncompleteMessagesToKinesis results (${results.length}) must be 0`);
    })
    .catch(err => {
      t.fail(`resubmitIncompleteMessagesToKinesis expected no failure - error: ${err.stack}`);
    });
});

test('resubmitIncompleteMessagesToKinesis with 1 message', t => {
  const context = {
    kinesis: dummyKinesis(t, 'resubmitIncompleteMessagesToKinesis', undefined)
  };
  logging.configureDefaultLogging(context);

  const streamName = 'TestStream_DEV';
  const eventSourceARN = samples.sampleEventSourceArn('eventSourceArnRegion', streamName);
  const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');
  const message = sampleMessage();
  message.taskTracking = { record: record };

  const awsContext = samples.sampleAwsContext('functionName', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'functionName', '1.0.1'));
  const event = samples.sampleKinesisEventWithRecords([record]);
  stages.configureDefaultStageHandling(context, false);
  stages.configureStage(context, event, awsContext, true);

  configureDefaultKinesisStreamProcessing(context, true);

  t.plan(2);
  resubmitIncompleteMessagesToKinesis([message], streamName, context)
    .then(results => {
      t.equal(results.length, 1, `resubmitIncompleteMessagesToKinesis results (${results.length}) must be 1`);
    })
    .catch(err => {
      t.fail(`resubmitIncompleteMessagesToKinesis expected no failure - error: ${err.stack}`);
    });
});

test('resubmitIncompleteMessagesToKinesis with 2 messages', t => {
  const context = {
    kinesis: dummyKinesis(t, 'resubmitIncompleteMessagesToKinesis', undefined)
  };
  logging.configureDefaultLogging(context);

  const streamName = 'TestStream_DEV';
  const eventSourceARN = samples.sampleEventSourceArn('eventSourceArnRegion', streamName);
  const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');
  const message = sampleMessage();
  message.taskTracking = { record: record };

  const record2 = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');
  const message2 = sampleMessage();
  message2.taskTracking = { record: record2 };

  const awsContext = samples.sampleAwsContext('functionName', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'functionName', '1.0.1'));
  const records = [record, record2];
  const messages = [message, message2];
  const event = samples.sampleKinesisEventWithRecords(records);
  stages.configureDefaultStageHandling(context, false);
  stages.configureStage(context, event, awsContext, true);

  configureDefaultKinesisStreamProcessing(context, true);

  t.plan(3);
  resubmitIncompleteMessagesToKinesis(messages, streamName, context)
    .then(results => {
      t.equal(results.length, 2, `resubmitIncompleteMessagesToKinesis results (${results.length}) must be 2`);
    })
    .catch(err => {
      t.fail(`resubmitIncompleteMessagesToKinesis expected no failure - error: ${err.stack}`);
    });
});

test('resubmitIncompleteMessagesToKinesis with 1 record and failure', t => {
  const error = new Error('Planned failure');
  const context = {
    kinesis: dummyKinesis(t, 'resubmitIncompleteMessagesToKinesis', error)
  };
  logging.configureDefaultLogging(context);

  const streamName = 'TestStream_DEV';
  const eventSourceARN = samples.sampleEventSourceArn('eventSourceArnRegion', streamName);
  const record = samples.sampleKinesisRecord(undefined, undefined, eventSourceARN, 'eventAwsRegion');
  const message = sampleMessage();
  message.taskTracking = { record: record };

  const awsContext = samples.sampleAwsContext('functionName', '1.0.1', samples.sampleInvokedFunctionArn('invokedFunctionArnRegion', 'functionName', '1.0.1'));
  const event = samples.sampleKinesisEventWithRecords([record]);
  stages.configureDefaultStageHandling(context, false);
  stages.configureStage(context, event, awsContext, true);

  configureDefaultKinesisStreamProcessing(context, true);

  t.plan(2);
  resubmitIncompleteMessagesToKinesis([message], streamName, context)
    .then(results => {
      t.fail(`resubmitIncompleteMessagesToKinesis expected a failure`);
    })
    .catch(err => {
      t.equal(err, error, `resubmitIncompleteMessagesToKinesis error (${err}) must be ${error}`);
    });
});


