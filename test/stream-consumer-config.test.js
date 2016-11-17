'use strict';

/**
 * Unit tests for aws-stream-consumer/stream-consumer-config.js
 * @author Byron du Preez
 */

const test = require("tape");

// The test subject
const configuration = require('../stream-consumer-config');

const isStreamConsumerConfigured = configuration.isStreamConsumerConfigured;
const configureStreamConsumer = configuration.configureStreamConsumer;

const getStreamConsumerSetting = configuration.getStreamConsumerSetting;

const streamProcessing = require('../stream-processing');

// External dependencies
const regions = require("aws-core-utils/regions");
const stages = require("aws-core-utils/stages");
const kinesisUtils = require("aws-core-utils/kinesis-utils");

const logging = require("logging-utils");

// Testing dependencies
const samples = require("./samples");

const testing = require("./testing");
// const okNotOk = testing.okNotOk;
// const checkOkNotOk = testing.checkOkNotOk;
// const checkMethodOkNotOk = testing.checkMethodOkNotOk;
const equal = testing.equal;
// const checkEqual = testing.checkEqual;
// const checkMethodEqual = testing.checkMethodEqual;

function setupRegion(region) {
  regions.ONLY_FOR_TESTING.setRegionIfNotSet(region);
  return regions.getRegion(true);
}

function sampleAwsEvent(streamName, partitionKey, data, omitEventSourceARN) {
  const region = process.env.AWS_REGION;
  const eventSourceArn = omitEventSourceARN ? undefined : samples.sampleEventSourceArn(region, streamName);
  return samples.sampleKinesisEventWithSampleRecord(partitionKey, data, eventSourceArn, region);
}

function sampleAwsEventWithRecords(streamNames, partitionKey, data) {
  const region = process.env.AWS_REGION;
  const records = streamNames.map(streamName => {
    const eventSourceArn = samples.sampleEventSourceArn(region, streamName);
    return samples.sampleKinesisRecord(partitionKey, data, eventSourceArn, region);
  });
  return samples.sampleKinesisEventWithRecords(records);
}

function sampleAwsContext(functionVersion, functionAlias) {
  const region = process.env.AWS_REGION;
  const functionName = 'sampleFunctionName';
  const invokedFunctionArn = samples.sampleInvokedFunctionArn(region, functionName, functionAlias);
  return samples.sampleAwsContext(functionName, functionVersion, invokedFunctionArn);
}

// =====================================================================================================================
// isStreamConsumerConfigured
// =====================================================================================================================

test('isStreamConsumerConfigured', t => {
  const context = {};

  // Simulate a region in AWS_REGION for testing
  setupRegion('us-west-2');

  // Must not be configured yet
  t.notOk(isStreamConsumerConfigured(context), `stream consumer must not be configured`);

  // Generate a sample AWS event
  const streamName = 'TestStream_DEV2';
  const event = sampleAwsEvent(streamName, 'partitionKey', '', false);

  // Generate a sample AWS context
  const awsContext = sampleAwsContext('1.0.1', 'dev1');

  configureDefaults(context);

  // Now configure the stream consumer runtime settings
  configureStreamConsumer(context, event, awsContext);

  // Must be configured now
  t.ok(isStreamConsumerConfigured(context), `stream consumer must be configured`);

  t.end();
});

// =====================================================================================================================
// configureStreamConsumer
// =====================================================================================================================

test('configureStreamConsumer must fail if missing region', t => {
  const context = {};

  // Simulate no region in AWS_REGION for testing
  process.env.AWS_REGION = '';
  t.notOk(process.env.AWS_REGION, 'AWS region must be empty');

  // Generate a sample AWS event
  const streamName = 'TestStream_DEV2';
  const event = sampleAwsEvent(streamName, 'partitionKey', '', false);

  // Generate a sample AWS context
  const awsContext = sampleAwsContext('1.0.1', 'dev1');

  configureDefaults(context);

  try {
    configureStreamConsumer(context, event, awsContext);
    t.fail(`configureStreamConsumer should NOT have passed`);

  } catch (err) {
    t.pass(`configureStreamConsumer should have failed (${err})`);
    const errMsgMatch = 'Failed to get AWS_REGION';
    t.ok(err.message.indexOf(errMsgMatch) !== -1, `configureStreamConsumer error should contain (${errMsgMatch})`);
  }

  t.end();
});

test('configureStreamConsumer must fail if missing stage', t => {
  const context = {};

  // Simulate a region in AWS_REGION for testing
  setupRegion('us-west-2');

  // Generate a sample AWS event
  const streamName = 'TestStream';
  const event = sampleAwsEvent(streamName, 'partitionKey', '', false);

  // Generate a sample AWS context
  const awsContext = sampleAwsContext('1.0.1', '1.0.1');

  configureDefaults(context);

  try {
    configureStreamConsumer(context, event, awsContext);
    t.fail(`configureStreamConsumer should NOT have passed`);

  } catch (err) {
    t.pass(`configureStreamConsumer should have failed (${err})`);
    const errMsgMatch = 'Failed to resolve stage';
    t.ok(err.message.indexOf(errMsgMatch) !== -1, `configureStreamConsumer error should contain (${errMsgMatch})`);
  }

  t.end();
});

test('configureStreamConsumer must fail if missing resubmitStreamName', t => {
  const context = {};

  // Simulate a region in AWS_REGION for testing
  setupRegion('us-west-2');

  // Generate a sample AWS event
  const streamName = 'TestStream';
  const event = sampleAwsEvent(streamName, 'partitionKey', '', true);

  // Generate a sample AWS context
  const awsContext = sampleAwsContext('1.0.1', 'dev');

  configureDefaults(context);

  try {
    configureStreamConsumer(context, event, awsContext);
    t.fail(`configureStreamConsumer should NOT have passed`);

  } catch (err) {
    t.pass(`configureStreamConsumer should have failed (${err})`);
    const errMsgMatch = 'Failed to resolve a source stream name';
    t.ok(err.message.indexOf(errMsgMatch) !== -1, `configureStreamConsumer error should contain (${errMsgMatch})`);
  }

  t.end();
});

test('configureStreamConsumer must fail if too many, non-distinct source stream names', t => {
  const context = {};

  // Simulate a region in AWS_REGION for testing
  setupRegion('us-west-2');

  // Generate a sample AWS event with multiple, non-distinct source stream names
  const streamNames = ['TestStream_DEV1', 'TestStream_DEV1', 'TestStream_DEV2'];
  const event = sampleAwsEventWithRecords(streamNames, 'partitionKey', '');

  // Generate a sample AWS context
  const awsContext = sampleAwsContext('1.0.1', 'dev');

  configureDefaults(context);

  try {
    configureStreamConsumer(context, event, awsContext);
    t.fail(`configureStreamConsumer should NOT have passed`);

  } catch (err) {
    t.pass(`configureStreamConsumer should have failed (${err})`);
    const errMsgMatch = 'Resolved too many, non-distinct source stream names';
    t.ok(err.message.indexOf(errMsgMatch) !== -1, `configureStreamConsumer error should contain (${errMsgMatch})`);
  }

  t.end();
});

function configureDefaults(context) {
  logging.configureDefaultLogging(context);
  stages.configureDefaultStageHandling(context);
  const config = require('../config.json');
  kinesisUtils.configureKinesis(context, config.kinesisOptions);
  streamProcessing.configureDefaultKinesisStreamProcessing(context);
}

test('configureStreamConsumer with perfect conditions', t => {
  const context = {};

  // Simulate a region in AWS_REGION for testing
  const region = setupRegion('us-west-2');
  const config = require('../config.json');

  // Generate a sample AWS event
  const streamName = 'TestStream_DEV2';
  const event = sampleAwsEvent(streamName, 'partitionKey', '', false);

  // Generate a sample AWS context
  const awsContext = sampleAwsContext('1.0.1', 'dev1');

  // Simulate perfect conditions - everything meant to be configured beforehand has been configured as well
  configureDefaults(context);

  try {
    configureStreamConsumer(context, event, awsContext);
    t.pass(`configureStreamConsumer should have passed`);

    equal(t, context.region, region, 'context.region');
    equal(t, context.stage, 'dev1', 'context.stage');
    equal(t, context.awsContext, awsContext, 'context.awsContext');
    equal(t, context.streamConsumer.resubmitStreamName, streamName, 'context.streamConsumer.resubmitStreamName');

    t.ok(context.kinesis, 'context.kinesis must be configured');
    equal(t, context.kinesis.config.region, region, 'context.kinesis.config.region');
    equal(t, context.kinesis.config.maxRetries, config.kinesisOptions.maxRetries, 'context.kinesis.config.maxRetries');

  } catch (err) {
    t.fail(`configureStreamConsumer should NOT have failed (${err})`);
  }

  t.end();
});

// =====================================================================================================================
// getStreamConsumerSetting
// =====================================================================================================================

test('getStreamConsumerSetting', t => {
  const context = {};

  // Simulate a region in AWS_REGION for testing
  const region = setupRegion('us-west-2');

  // Generate a sample AWS event
  const streamName = 'TestStream_DEV2';
  const event = sampleAwsEvent(streamName, 'partitionKey', '', false);

  // Generate a sample AWS context
  const awsContext = sampleAwsContext('1.0.1', 'dev1');

  // Simulate perfect conditions - everything meant to be configured beforehand has been configured as well
  configureDefaults(context);

  configureStreamConsumer(context, event, awsContext);

  equal(t, getStreamConsumerSetting(context, 'resubmitStreamName'), streamName, 'resubmitStreamName setting');

  t.end();
});


