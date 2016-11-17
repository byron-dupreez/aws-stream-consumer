'use strict';

/**
 * Utilities for configuring and accessing the runtime settings of a Kinesis or DynamoDB stream consumer from a given
 * AWS event and AWS context.
 * @module aws-stream-consumer/stream-consumer-config
 * @author Byron du Preez
 */
module.exports = {
  /** Returns true if a stream consumer's runtime settings have been configured on the given context; false otherwise */
  isStreamConsumerConfigured: isStreamConsumerConfigured,
  // Consumer configuration - configures the runtime settings for a stream consumer on a given context from a given AWS event and AWS context
  /** Configures the runtime settings for a stream consumer */
  configureStreamConsumer: configureStreamConsumer,
  // Gets the value of a named runtime setting of a stream consumer
  getStreamConsumerSetting: getStreamConsumerSetting
};

const streamProcessing = require('./stream-processing');

const regions = require('aws-core-utils/regions');
const stages = require('aws-core-utils/stages');
const arns = require('aws-core-utils/arns');
const streamEvents = require('aws-core-utils/stream-events');
//const kinesisUtils = require('aws-core-utils/kinesis-utils');

const Strings = require('core-functions/strings');
const isBlank = Strings.isBlank;
const isNotBlank = Strings.isNotBlank;
// const trim = Strings.trim;
const stringify = Strings.stringify;

const Arrays = require('core-functions/arrays');

const logging = require('logging-utils/logging-utils');

// =====================================================================================================================
// Consumer configuration - configures the runtime settings for a stream consumer on a given context from a given AWS event and AWS context
// =====================================================================================================================

/**
 * Returns true if a stream consumer's runtime settings have been configured on the given context; otherwise returns
 * false.
 * @param {Object} context - the context to check
 * @returns {boolean} true if configured; false otherwise
 */
function isStreamConsumerConfigured(context) {
  return !!context && typeof context.streamConsumer === 'object' && context.streamConsumer.resubmitStreamName &&
    logging.isLoggingConfigured(context) && stages.isStageHandlingConfigured(context) && context.awsContext &&
    streamProcessing.isStreamProcessingConfigured(context);
}

/**
 * Configures the runtime settings for a stream consumer on the given context from the given AWS event and AWS context
 * in preparation for processing of a batch of Kinesis or DynamoDB stream records. Any error thrown must subsequently
 * trigger a replay of all the records in the current batch until the Lambda can be fixed.
 *
 * @param {Object} context - the context onto which to configure a stream consumer's runtime settings
 * @param {Object} event - the AWS event, which was passed to your lambda
 * @param {Object} awsContext - the AWS context, which was passed to your lambda
 * @throws {Error} an error if the region, stage and/or source stream name cannot be resolved
 * @return {Object} the context object configured with a stream consumer's runtime settings
 */
function configureStreamConsumer(context, event, awsContext) {
  const config = require('./config.json');

  // Configure default logging from local config if not configured yet
  configureLoggingIfNotConfigured(context, config.logging);

  // Configure stage handling if not configured yet
  configureDefaultStageHandlingIfNotConfigured(context, 'configureStreamConsumer');

  // Configure region, stage & AWS context
  configureRegionStageAndAwsContext(context, event, awsContext);

  // Configure stream processing if not configured yet
  configureDefaultKinesisStreamProcessingIfNotConfigured(context);

  if (!context.streamConsumer) {
    context.streamConsumer = {};
  }

  // Resolve the name of the source stream from which the AWS event was received and to which any incomplete messages
  // must be resubmitted
  configureResubmitStreamName(context, event);
}

/**
 * Returns the value of the named runtime setting (if any) for a stream consumer from the given context.
 * @param context - the context from which to fetch the named setting's value
 * @param settingName - the name of the stream consumer's runtime setting
 * @returns {*|undefined} the value of the named setting (if any); otherwise undefined
 */
function getStreamConsumerSetting(context, settingName) {
  return context && context.streamConsumer && isNotBlank(settingName) ? context.streamConsumer[settingName] : undefined;
}

function configureLoggingIfNotConfigured(context, config) {
  if (!logging.isLoggingConfigured(context)) {
    logging.configureLoggingFromConfig(context, config);
    context.warn(`Logging was not configured yet - used default logging configuration from aws-stream-consumer/config.json`);
  }
}

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
}

/**
 * If no stage handling settings have been configured yet, then configure the given context with the default settings.
 */
function configureDefaultStageHandlingIfNotConfigured(context, caller) {
  // Configure stage handling if not already configured
  if (!stages.isStageHandlingConfigured(context)) {
    context.warn(`Stage handling was not configured before calling ${caller} - using default stage handling configuration`);
    stages.configureDefaultStageHandling(context, true);
  }
}

function configureDefaultKinesisStreamProcessingIfNotConfigured(context) {
  // Configure default stream processing if not already configured
  if (!streamProcessing.isStreamProcessingConfigured(context)) {
    context.warn(`Kinesis stream processing was not configured yet - using default Kinesis stream processing configuration`);
    streamProcessing.configureDefaultKinesisStreamProcessing(context, true);
  }
  // Validate that stream processing is configured correctly
  streamProcessing.validateStreamProcessingConfiguration(context);
}

/**
 * Resolves the name of the source stream from which the AWS event was received and to which any incomplete messages
 * must be resubmitted.
 * @param {Object} context - the context to configure
 * @param {Object} event - the AWS event, which was passed to your lambda
 * @returns {Object} the given context
 */
function configureResubmitStreamName(context, event) {
  if (isBlank(context.streamConsumer.resubmitStreamName)) {
    // Resolve the source Kinesis stream's ARN (or stream name only?)
    //const eventSourceARNs = streamEvents.getEventSourceARNs(event);
    const streamNames = streamEvents.getEventSourceStreamNames(event)
      .filter(streamName => isNotBlank(streamName));

    let streamName = streamNames.find(s => isNotBlank(s));

    if (streamNames.length > 1) {
      const distinctStreamNames = Arrays.distinct(streamNames);

      if (distinctStreamNames.length > 1) {
        const errorMsg = `FATAL - Resolved too many, non-distinct source stream names ${stringify(distinctStreamNames)} - need ONE for resubmission of failed messages. Fix your Lambda by possibly configuring a streamConsumer.resubmitStreamName on its context and redeploy ASAP, since this issue is blocking all of your stream's shards!`;
        context.error(errorMsg);
        throw new Error(errorMsg);
      }
    }
    // Ensure a stream name was resolved
    if (isBlank(streamName)) {
      const errorMsg = `FATAL - Failed to resolve a source stream name needed for resubmission of failed messages. Fix your Lambda by possibly configuring a streamConsumer.resubmitStreamName on its context and redeploy ASAP, since this issue is blocking all of your stream's shards!`;
      context.error(errorMsg);
      throw new Error(errorMsg);
    }
    //const resubmitStreamARN = eventSourceARNs.find(arn => isNotBlank(arn));
    //context.kinesisConsumer.resubmitStreamARN = resubmitStreamARN;
    context.streamConsumer.resubmitStreamName = streamName;
  }
  return context;
}
