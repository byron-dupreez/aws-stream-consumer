'use strict';

/**
 * Unit tests for testing the configuration aspects of aws-stream-consumer/stream-consumer.js
 * @author Byron du Preez
 */

const test = require("tape");

// The test subject
const streamConsumer = require('../stream-consumer');
const isStreamConsumerConfigured = streamConsumer.isStreamConsumerConfigured;
const configureStreamConsumer = streamConsumer.configureStreamConsumer;

const logging = require("logging-utils");
const streamProcessing = require('../stream-processing');

// External dependencies
const regions = require("aws-core-utils/regions");
const stages = require("aws-core-utils/stages");
const kinesisCache = require("aws-core-utils/kinesis-cache");
const dynamoDBDocClientCache = require("aws-core-utils/dynamodb-doc-client-cache");

const Strings = require('core-functions/strings');
const stringify = Strings.stringify;

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
  const eventSourceArn = omitEventSourceARN ? undefined : samples.sampleKinesisEventSourceArn(region, streamName);
  return samples.sampleKinesisEventWithSampleRecord(partitionKey, data, eventSourceArn, region);
}

function sampleAwsEventWithRecords(streamNames, partitionKey, data) {
  const region = process.env.AWS_REGION;
  const records = streamNames.map(streamName => {
    const eventSourceArn = samples.sampleKinesisEventSourceArn(region, streamName);
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
// isStreamConsumerConfigured with Kinesis default options
// =====================================================================================================================

test('isStreamConsumerConfigured with Kinesis default options', t => {
  const context = {};
  const options = require('../kinesis-options.json');

  // Must not be configured yet
  t.notOk(isStreamConsumerConfigured(context), `stream consumer must not be configured`);

  // Simulate a region in AWS_REGION for testing
  const region = setupRegion('us-west-2');

  // Clear caches
  kinesisCache.deleteKinesis(region);
  dynamoDBDocClientCache.deleteDynamoDBDocClient(region);

  // Generate a sample AWS event
  const streamName = 'TestStream_DEV2';
  const event = sampleAwsEvent(streamName, 'partitionKey', '', false);

  // Generate a sample AWS context
  const awsContext = sampleAwsContext('1.0.1', 'dev1');

  // Now configure the stream consumer runtime settings
  configureStreamConsumer(context, undefined, options, event, awsContext);

  // Must be configured now
  t.ok(isStreamConsumerConfigured(context), `stream consumer must be configured`);

  t.end();
});

// =====================================================================================================================
// isStreamConsumerConfigured with DynamoDB default options
// =====================================================================================================================

test('isStreamConsumerConfigured with DynamoDB default options', t => {
  const context = {};
  const options = require('../dynamodb-options.json');

  // Must not be configured yet
  t.notOk(isStreamConsumerConfigured(context), `stream consumer must not be configured`);

  // Simulate a region in AWS_REGION for testing
  const region = setupRegion('us-west-2');

  // Clear caches
  kinesisCache.deleteKinesis(region);
  dynamoDBDocClientCache.deleteDynamoDBDocClient(region);

  // Generate a sample AWS event
  const eventSourceARN = samples.sampleDynamoDBEventSourceArn(region, 'TestTable_QA');
  const event = samples.awsDynamoDBUpdateSampleEvent(eventSourceARN);

  // Generate a sample AWS context
  const awsContext = sampleAwsContext('1.0.1', 'dev1');

  // Now configure the stream consumer runtime settings
  configureStreamConsumer(context, undefined, options, event, awsContext);

  // Must be configured now
  t.ok(isStreamConsumerConfigured(context), `stream consumer must be configured`);

  t.end();
});

// =====================================================================================================================
// configureStreamConsumer with Kinesis default options
// =====================================================================================================================

test('configureStreamConsumer with Kinesis default options must fail if missing region', t => {
  const context = {};
  const options = require('../kinesis-options.json');

  // Simulate no region in AWS_REGION for testing
  process.env.AWS_REGION = '';
  t.notOk(process.env.AWS_REGION, 'AWS region must be empty');

  // Generate a sample AWS event
  const streamName = 'TestStream_DEV2';
  const event = sampleAwsEvent(streamName, 'partitionKey', '', false);

  // Generate a sample AWS context
  const awsContext = sampleAwsContext('1.0.1', 'dev1');

  try {
    configureStreamConsumer(context, undefined, options, event, awsContext);
    t.fail(`configureStreamConsumer should NOT have passed`);

  } catch (err) {
    t.pass(`configureStreamConsumer must fail (${err})`);
    const errMsgMatch = 'Failed to get AWS_REGION';
    t.ok(err.message.indexOf(errMsgMatch) !== -1, `configureStreamConsumer error should contain (${errMsgMatch})`);
  }

  t.end();
});

test('configureStreamConsumer with Kinesis default options must fail if missing stage', t => {
  const context = {};
  const options = require('../kinesis-options.json');

  // Simulate a region in AWS_REGION for testing
  const region = setupRegion('us-west-2');

  // Clear caches
  kinesisCache.deleteKinesis(region);
  dynamoDBDocClientCache.deleteDynamoDBDocClient(region);

  // Generate a sample AWS event
  const streamName = 'TestStream';
  const event = sampleAwsEvent(streamName, 'partitionKey', '', false);

  // Generate a sample AWS context
  const awsContext = sampleAwsContext('1.0.1', '1.0.1');

  try {
    configureStreamConsumer(context, undefined, options, event, awsContext);
    t.fail(`configureStreamConsumer should NOT have passed`);

  } catch (err) {
    t.pass(`configureStreamConsumer must fail (${err})`);
    const errMsgMatch = 'Failed to resolve stage';
    t.ok(err.message.indexOf(errMsgMatch) !== -1, `configureStreamConsumer error should contain (${errMsgMatch})`);
  }

  t.end();
});

test('configureStreamConsumer with Kinesis default options & ideal conditions must pass', t => {
  const context = {};
  const options = require('../kinesis-options.json');

  // Simulate a region in AWS_REGION for testing
  const region = setupRegion('us-west-2');

  // Clear caches
  kinesisCache.deleteKinesis(region);
  dynamoDBDocClientCache.deleteDynamoDBDocClient(region);

  // Generate a sample AWS event
  const streamName = 'TestStream_DEV2';
  const event = sampleAwsEvent(streamName, 'partitionKey', '', false);

  // Generate a sample AWS context
  const awsContext = sampleAwsContext('1.0.1', 'dev1');

  try {
    // Simulate perfect conditions - everything meant to be configured beforehand has been configured as well
    configureStreamConsumer(context, undefined, options, event, awsContext);
    t.pass(`configureStreamConsumer should have passed`);

    t.ok(logging.isLoggingConfigured(context), 'logging must be configured');
    t.ok(stages.isStageHandlingConfigured(context), 'stage handling must be configured');
    t.ok(streamProcessing.isStreamProcessingConfigured(context), 'stream processing must be configured');
    t.ok(isStreamConsumerConfigured(context), 'stream consumer must be configured');
    t.ok(context.stageHandling && typeof context.stageHandling === 'object', 'context.stageHandling must be configured');
    t.ok(context.streamProcessing && typeof context.streamProcessing === 'object', 'context.streamProcessing must be configured');

    equal(t, context.region, region, 'context.region');
    equal(t, context.stage, 'dev1', 'context.stage');
    equal(t, context.awsContext, awsContext, 'context.awsContext');

    t.ok(context.kinesis, 'context.kinesis must be configured');
    equal(t, context.kinesis.config.region, region, 'context.kinesis.config.region');
    equal(t, context.kinesis.config.maxRetries, options.streamProcessingOptions.kinesisOptions.maxRetries, 'context.kinesis.config.maxRetries');

    t.notOk(context.dynamoDBDocClient, 'context.dynamoDBDocClient must not be configured');

  } catch (err) {
    t.fail(`configureStreamConsumer should NOT have failed (${err})`);
  }

  t.end();
});

// =====================================================================================================================
// configureStreamConsumer with DynamoDB default options
// =====================================================================================================================

test('configureStreamConsumer with Kinesis default options must fail if missing region', t => {
  const context = {};
  const options = require('../dynamodb-options.json');

  // Simulate no region in AWS_REGION for testing
  process.env.AWS_REGION = '';
  t.notOk(process.env.AWS_REGION, 'AWS region must be empty');

  // Generate a sample AWS event
  const eventSourceARN = samples.sampleDynamoDBEventSourceArn('us-west-2', 'TestTable_QA');
  const event = samples.awsDynamoDBUpdateSampleEvent(eventSourceARN);

  // Generate a sample AWS context
  const awsContext = sampleAwsContext('1.0.1', 'dev1');

  try {
    configureStreamConsumer(context, undefined, options, event, awsContext);
    t.fail(`configureStreamConsumer should NOT have passed`);

  } catch (err) {
    t.pass(`configureStreamConsumer must fail (${err})`);
    const errMsgMatch = 'Failed to get AWS_REGION';
    t.ok(err.message.indexOf(errMsgMatch) !== -1, `configureStreamConsumer error should contain (${errMsgMatch})`);
  }

  t.end();
});

test('configureStreamConsumer with DynamoDB default options must fail if missing stage', t => {
  const context = {};
  const options = require('../dynamodb-options.json');

  // Simulate a region in AWS_REGION for testing
  const region = setupRegion('us-west-2');

  // Clear caches
  kinesisCache.deleteKinesis(region);
  dynamoDBDocClientCache.deleteDynamoDBDocClient(region);

  // Generate a sample AWS event
  const eventSourceARN = samples.sampleDynamoDBEventSourceArn(region, 'TestTable');
  const event = samples.awsDynamoDBUpdateSampleEvent(eventSourceARN);

  // Generate a sample AWS context
  const awsContext = sampleAwsContext('1.0.1', '1.0.1');

  try {
    configureStreamConsumer(context, undefined, options, event, awsContext);
    t.fail(`configureStreamConsumer should NOT have passed`);

  } catch (err) {
    t.pass(`configureStreamConsumer must fail (${err})`);
    const errMsgMatch = 'Failed to resolve stage';
    t.ok(err.message.indexOf(errMsgMatch) !== -1, `configureStreamConsumer error should contain (${errMsgMatch})`);
  }

  t.end();
});

test('configureStreamConsumer with DynamoDB default options & ideal conditions must pass', t => {
  const context = {};
  const options = require('../dynamodb-options.json');

  // Simulate a region in AWS_REGION for testing
  const region = setupRegion('us-west-2');

  // Clear caches
  kinesisCache.deleteKinesis(region);
  dynamoDBDocClientCache.deleteDynamoDBDocClient(region);

  // Generate a sample AWS event
  const eventSourceARN = samples.sampleDynamoDBEventSourceArn(region, 'TestTable_QA');
  const event = samples.awsDynamoDBUpdateSampleEvent(eventSourceARN);

  // Generate a sample AWS context
  const awsContext = sampleAwsContext('1.0.1', 'dev1');

  try {
    // Simulate perfect conditions - everything meant to be configured beforehand has been configured as well
    configureStreamConsumer(context, undefined, options, event, awsContext);
    t.pass(`configureStreamConsumer should have passed`);

    t.ok(logging.isLoggingConfigured(context), 'logging must be configured');
    t.ok(stages.isStageHandlingConfigured(context), 'stage handling must be configured');
    t.ok(streamProcessing.isStreamProcessingConfigured(context), 'stream processing must be configured');
    t.ok(isStreamConsumerConfigured(context), 'stream consumer must be configured');
    t.ok(context.stageHandling && typeof context.stageHandling === 'object', 'context.stageHandling must be configured');
    t.ok(context.streamProcessing && typeof context.streamProcessing === 'object', 'context.streamProcessing must be configured');

    equal(t, context.region, region, 'context.region');
    equal(t, context.stage, 'dev1', 'context.stage');
    equal(t, context.awsContext, awsContext, 'context.awsContext');

    t.ok(context.kinesis, 'context.kinesis must be configured');
    equal(t, context.kinesis.config.region, region, 'context.kinesis.config.region');
    equal(t, context.kinesis.config.maxRetries, options.streamProcessingOptions.kinesisOptions.maxRetries, 'context.kinesis.config.maxRetries');

    t.ok(context.dynamoDBDocClient, 'context.dynamoDBDocClient must be configured');
    equal(t, context.dynamoDBDocClient.service.config.region, region, 'context.dynamoDBDocClient.service.config.region');
    equal(t, context.dynamoDBDocClient.service.config.maxRetries, options.streamProcessingOptions.dynamoDBDocClientOptions.maxRetries, 'context.dynamoDBDocClient.service.config.maxRetries');

  } catch (err) {
    t.fail(`configureStreamConsumer should NOT have failed (${err})`);
  }

  t.end();
});


