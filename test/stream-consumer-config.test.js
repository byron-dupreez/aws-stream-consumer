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

// const Strings = require('core-functions/strings');
// const stringify = Strings.stringify;

// Testing dependencies
const samples = require("./samples");

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

function checkDependencies(t, context, stdSettings, stdOptions, event, awsContext, expectedStage) {
  t.ok(logging.isLoggingConfigured(context), `logging must be configured`);
  t.ok(stages.isStageHandlingConfigured(context), `stage handling must be configured`);
  t.ok(context.stageHandling && typeof context.stageHandling === 'object', 'context.stageHandling must be configured');
  t.ok(context.custom && typeof context.custom === 'object', `context.custom must be configured`);
  t.ok(streamProcessing.isStreamProcessingConfigured(context), 'stream processing must be configured');
  t.ok(context.streamProcessing && typeof context.streamProcessing === 'object', 'context.streamProcessing must be configured');

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
// isStreamConsumerConfigured with default Kinesis options
// =====================================================================================================================

test('isStreamConsumerConfigured with default Kinesis options', t => {
  try {
    // Simulate a region in AWS_REGION for testing
    setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};
    const options = require('../default-kinesis-options.json');

    // Must not be configured yet
    t.notOk(isStreamConsumerConfigured(context), `stream consumer must not be configured`);

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const event = sampleAwsEvent(streamName, 'partitionKey', '', false);

    // Generate a sample AWS context
    const awsContext = sampleAwsContext('1.0.1', 'dev1');

    // Now configure the stream consumer runtime settings
    configureStreamConsumer(context, undefined, options, event, awsContext);

    // Must be configured now
    t.ok(isStreamConsumerConfigured(context), `stream consumer must be configured`);
    checkDependencies(t, context, undefined, options, event, awsContext, 'dev1');

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

// =====================================================================================================================
// isStreamConsumerConfigured with default DynamoDB options
// =====================================================================================================================

test('isStreamConsumerConfigured with default DynamoDB options', t => {
  try {
    // Simulate a region in AWS_REGION for testing
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};
    const options = require('../default-dynamodb-options.json');

    // Must not be configured yet
    t.notOk(isStreamConsumerConfigured(context), `stream consumer must not be configured`);

    // Generate a sample AWS event
    const eventSourceARN = samples.sampleDynamoDBEventSourceArn(region, 'TestTable_QA');
    const event = samples.awsDynamoDBUpdateSampleEvent(eventSourceARN);

    // Generate a sample AWS context
    const awsContext = sampleAwsContext('1.0.1', 'dev1');

    // Now configure the stream consumer runtime settings
    configureStreamConsumer(context, undefined, options, event, awsContext);

    // Must be configured now
    t.ok(isStreamConsumerConfigured(context), `stream consumer must be configured`);
    checkDependencies(t, context, undefined, options, event, awsContext, 'dev1');

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

// =====================================================================================================================
// configureStreamConsumer with default Kinesis options
// =====================================================================================================================

test('configureStreamConsumer with default Kinesis options must fail if missing region', t => {
  try {
    // Simulate NO region in AWS_REGION for testing
    const region = setRegionStageAndDeleteCachedInstances('', undefined);
    t.notOk(process.env.AWS_REGION, 'AWS region must be empty');

    const context = {};
    const options = require('../default-kinesis-options.json');

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

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

test('configureStreamConsumer with default Kinesis options must fail if missing stage', t => {
  try {
    // Simulate a region in AWS_REGION for testing
    setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};
    const options = require('../default-kinesis-options.json');

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

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

test('configureStreamConsumer with default Kinesis options & ideal conditions must pass', t => {
  try {
    // Simulate a region in AWS_REGION for testing
    setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};
    const options = require('../default-kinesis-options.json');

    // Generate a sample AWS event
    const streamName = 'TestStream_DEV2';
    const event = sampleAwsEvent(streamName, 'partitionKey', '', false);

    // Generate a sample AWS context
    const awsContext = sampleAwsContext('1.0.1', 'dev1');

    try {
      // Simulate perfect conditions - everything meant to be configured beforehand has been configured as well
      configureStreamConsumer(context, undefined, options, event, awsContext);
      t.pass(`configureStreamConsumer should have passed`);

      t.ok(streamProcessing.isStreamProcessingConfigured(context), 'stream processing must be configured');
      checkDependencies(t, context, undefined, options, event, awsContext, 'dev1');

    } catch (err) {
      t.fail(`configureStreamConsumer should NOT have failed (${err})`);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

// =====================================================================================================================
// configureStreamConsumer with default DynamoDB options
// =====================================================================================================================

test('configureStreamConsumer with default Kinesis options must fail if missing region', t => {
  try {
    // Simulate NO region in AWS_REGION for testing
    const region = setRegionStageAndDeleteCachedInstances('', undefined);
    t.notOk(process.env.AWS_REGION, 'AWS region must be empty');

    const context = {};
    const options = require('../default-dynamodb-options.json');

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

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

test('configureStreamConsumer with default DynamoDB options must fail if missing stage', t => {
  try {
    // Simulate a region in AWS_REGION for testing
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};
    const options = require('../default-dynamodb-options.json');

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

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});

test('configureStreamConsumer with default DynamoDB options & ideal conditions must pass', t => {
  try {
    // Simulate a region in AWS_REGION for testing
    const region = setRegionStageAndDeleteCachedInstances('us-west-2', undefined);

    const context = {};
    const options = require('../default-dynamodb-options.json');

    // Generate a sample AWS event
    const eventSourceARN = samples.sampleDynamoDBEventSourceArn(region, 'TestTable_QA');
    const event = samples.awsDynamoDBUpdateSampleEvent(eventSourceARN);

    // Generate a sample AWS context
    const awsContext = sampleAwsContext('1.0.1', 'dev1');

    try {
      // Simulate perfect conditions - everything meant to be configured beforehand has been configured as well
      configureStreamConsumer(context, undefined, options, event, awsContext);
      t.pass(`configureStreamConsumer should have passed`);

      t.ok(isStreamConsumerConfigured(context), 'stream consumer must be configured');
      checkDependencies(t, context, undefined, options, event, awsContext, 'dev1');

    } catch (err) {
      t.fail(`configureStreamConsumer should NOT have failed (${err})`);
    }

  } finally {
    process.env.AWS_REGION = undefined;
    process.env.STAGE = undefined;
  }
  t.end();
});
