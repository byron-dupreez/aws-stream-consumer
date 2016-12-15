'use strict';

const MAX_PARTITION_KEY_SIZE = 256;

// Setting names
const STREAM_TYPE_SETTING = 'streamType';
const TASK_TRACKING_NAME_SETTING = 'taskTrackingName';
const TIMEOUT_AT_PERCENTAGE_OF_REMAINING_TIME_SETTING = 'timeoutAtPercentageOfRemainingTime';
const MAX_NUMBER_OF_ATTEMPTS_SETTING = 'maxNumberOfAttempts';

const EXTRACT_MESSAGE_FROM_RECORD_SETTING = 'extractMessageFromRecord';
const LOAD_TASK_TRACKING_STATE_SETTING = 'loadTaskTrackingState';
const SAVE_TASK_TRACKING_STATE_SETTING = 'saveTaskTrackingState';
const HANDLE_INCOMPLETE_MESSAGES_SETTING = 'handleIncompleteMessages';
const DISCARD_UNUSABLE_RECORDS_SETTING = 'discardUnusableRecords';
const DISCARD_REJECTED_MESSAGES_SETTING = 'discardRejectedMessages';

const TASK_TRACKING_TABLE_NAME_SETTING = 'taskTrackingTableName';
const DEAD_RECORD_QUEUE_NAME_SETTING = 'deadRecordQueueName';
const DEAD_MESSAGE_QUEUE_NAME_SETTING = 'deadMessageQueueName';

// Valid stream types
const KINESIS_STREAM_TYPE = "kinesis";
const DYNAMODB_STREAM_TYPE = "dynamodb";

/**
 * Utilities for configuring stream processing, which configures and determines the processing behaviour of a stream
 * consumer.
 * @module aws-stream-consumer/stream-processing-config
 * @author Byron du Preez
 */
module.exports = {
  // Stream processing configuration - configures and determines the processing behaviour of a stream consumer
  isStreamProcessingConfigured: isStreamProcessingConfigured,

  configureStreamProcessing: configureStreamProcessing,
  configureStreamProcessingWithSettings: configureStreamProcessingWithSettings,

  validateStreamProcessingConfiguration: validateStreamProcessingConfiguration,

  getDefaultKinesisStreamProcessingSettings: getDefaultKinesisStreamProcessingSettings,
  configureDefaultKinesisStreamProcessing: configureDefaultKinesisStreamProcessing,

  getDefaultDynamoDBStreamProcessingSettings: getDefaultDynamoDBStreamProcessingSettings,
  configureDefaultDynamoDBStreamProcessing: configureDefaultDynamoDBStreamProcessing,

  // Accessors for stream processing settings and functions
  getStreamProcessingSetting: getStreamProcessingSetting,
  getStreamProcessingFunction: getStreamProcessingFunction,
  // Convenience accessors for specific stream processing settings
  getStreamType: getStreamType,
  isKinesisStreamType: isKinesisStreamType,
  isDynamoDBStreamType: isDynamoDBStreamType,
  getMaxNumberOfAttempts: getMaxNumberOfAttempts,
  // Convenience accessors for specific stream processing functions
  getExtractMessageFromRecordFunction: getExtractMessageFromRecordFunction,
  getLoadTaskTrackingStateFunction: getLoadTaskTrackingStateFunction,
  getSaveTaskTrackingStateFunction: getSaveTaskTrackingStateFunction,
  getHandleIncompleteMessagesFunction: getHandleIncompleteMessagesFunction,
  getDiscardUnusableRecordsFunction: getDiscardUnusableRecordsFunction,
  getDiscardRejectedMessagesFunction: getDiscardRejectedMessagesFunction,

  /**
   * Default implementations of the stream processing functions, which are NOT meant to be used directly and are ONLY
   * exposed to facilitate re-using some of these functions if needed in a customised stream processing configuration.
   */
  DEFAULTS: {
    // Default Kinesis stream processing functions
    // ===========================================

    // Default Kinesis extractMessageFromRecord function
    extractJsonMessageFromKinesisRecord: extractJsonMessageFromKinesisRecord,

    // Default Kinesis loadTaskTrackingState function
    skipLoadTaskTrackingState: skipLoadTaskTrackingState,

    // Default Kinesis saveTaskTrackingState function
    skipSaveTaskTrackingState: skipSaveTaskTrackingState,

    // Default Kinesis handleIncompleteMessages function
    resubmitIncompleteMessagesToKinesis: resubmitIncompleteMessagesToKinesis,

    // Default DynamoDB stream processing functions
    // ============================================

    // Default DynamoDB extractMessageFromRecord function
    useStreamEventRecordAsMessage: useStreamEventRecordAsMessage,

    // Default DynamoDB loadTaskTrackingState function
    loadTaskTrackingStateFromDynamoDB: loadTaskTrackingStateFromDynamoDB,

    // Default DynamoDB saveTaskTrackingState function
    saveTaskTrackingStateToDynamoDB: saveTaskTrackingStateToDynamoDB,

    // Default DynamoDB handleIncompleteMessages function
    replayAllMessagesIfIncomplete: replayAllMessagesIfIncomplete,

    // Default common Kinesis and DynamoDB stream processing functions
    // ===============================================================

    // Default discardUnusableRecords function
    discardUnusableRecordsToDRQ: discardUnusableRecordsToDRQ,

    // Default discardRejectedMessages function
    discardRejectedMessagesToDMQ: discardRejectedMessagesToDMQ,
  },
  // Generic settings names
  STREAM_TYPE_SETTING: STREAM_TYPE_SETTING,
  TASK_TRACKING_NAME_SETTING: TASK_TRACKING_NAME_SETTING,
  TIMEOUT_AT_PERCENTAGE_OF_REMAINING_TIME_SETTING: TIMEOUT_AT_PERCENTAGE_OF_REMAINING_TIME_SETTING,
  MAX_NUMBER_OF_ATTEMPTS_SETTING: MAX_NUMBER_OF_ATTEMPTS_SETTING,

  // Generic functions settings names
  EXTRACT_MESSAGE_FROM_RECORD_SETTING: EXTRACT_MESSAGE_FROM_RECORD_SETTING,
  LOAD_TASK_TRACKING_STATE_SETTING: LOAD_TASK_TRACKING_STATE_SETTING,
  SAVE_TASK_TRACKING_STATE_SETTING: SAVE_TASK_TRACKING_STATE_SETTING,
  HANDLE_INCOMPLETE_MESSAGES_SETTING: HANDLE_INCOMPLETE_MESSAGES_SETTING,
  DISCARD_UNUSABLE_RECORDS_SETTING: DISCARD_UNUSABLE_RECORDS_SETTING,
  DISCARD_REJECTED_MESSAGES_SETTING: DISCARD_REJECTED_MESSAGES_SETTING,

  // Specialised settings names used by default processing function implementations
  TASK_TRACKING_TABLE_NAME_SETTING: TASK_TRACKING_TABLE_NAME_SETTING,
  DEAD_RECORD_QUEUE_NAME_SETTING: DEAD_RECORD_QUEUE_NAME_SETTING,
  DEAD_MESSAGE_QUEUE_NAME_SETTING: DEAD_MESSAGE_QUEUE_NAME_SETTING,

  // Valid stream types
  KINESIS_STREAM_TYPE: KINESIS_STREAM_TYPE,
  DYNAMODB_STREAM_TYPE: DYNAMODB_STREAM_TYPE
};

const regions = require('aws-core-utils/regions');
const stages = require('aws-core-utils/stages');
const contexts = require('aws-core-utils/contexts');
const arns = require('aws-core-utils/arns');
const streamEvents = require('aws-core-utils/stream-events');
const kinesisCache = require('aws-core-utils/kinesis-cache');
const dynamoDBDocClientCache = require('aws-core-utils/dynamodb-doc-client-cache');
const dynamoDBUtils = require('aws-core-utils/dynamodb-utils');

const Objects = require('core-functions/objects');
const Strings = require('core-functions/strings');
const isBlank = Strings.isBlank;
const isNotBlank = Strings.isNotBlank;
const trim = Strings.trim;
const stringify = Strings.stringify;

const logging = require('logging-utils');

// =====================================================================================================================
// Stream processing configuration - configures and determines the processing behaviour of a stream consumer
// =====================================================================================================================

/**
 * Returns true if stream processing is already configured on the given context; false otherwise.
 * @param {Object|StreamProcessing} context - the context to check
 * @returns {boolean} true if configured; false otherwise
 */
function isStreamProcessingConfigured(context) {
  return context && typeof context === 'object' && context.streamProcessing && typeof context.streamProcessing === 'object';
}

/**
 * Configures the given context as a standard context with the given standard settings and standard options and with
 * EITHER the given stream processing settings (if any) OR the default stream processing settings partially overridden
 * by the given stream processing options (if any), but only if stream processing is not already configured on the given
 * context OR if forceConfiguration is true.
 *
 * Note that if either the given event or AWS context are undefined, then everything other than the region, stage and
 * AWS context will be configured. This missing configuration can be configured at a later point in your code by
 * invoking {@linkcode stages#configureRegionStageAndAwsContext}. This separation of configuration is primarily useful
 * for unit testing.
 *
 * @param {Object|StreamProcessing|StandardContext} context - the context to configure
 * @param {StreamProcessingSettings|undefined} [settings] - optional stream processing settings to use to configure stream processing
 * @param {StreamProcessingOptions|undefined} [options] - optional stream processing options to use to override default options
 * @param {StandardSettings|undefined} [standardSettings] - optional standard settings to use to configure dependencies
 * @param {StandardOptions|undefined} [standardOptions] - optional other options to use to configure dependencies
 * @param {AwsEvent|undefined} [event] - the AWS event, which was passed to your lambda
 * @param {AwsContext|undefined} [awsContext] - the AWS context, which was passed to your lambda
 * @param {boolean|undefined} [forceConfiguration] - whether or not to force configuration of the given settings, which
 * will override any previously configured stream processing settings on the given context
 * @returns {StreamProcessing} the given context configured with stream processing settings, stage handling settings and
 * logging functionality
 */
function configureStreamProcessing(context, settings, options, standardSettings, standardOptions, event, awsContext, forceConfiguration) {
  const settingsAvailable = settings && typeof settings === 'object';
  const optionsAvailable = options && typeof options === 'object';

  // Check if stream processing was already configured
  const streamProcessingWasConfigured = isStreamProcessingConfigured(context);

  // Attempt to discover what stream type is being configured
  const streamType = resolveStreamType(settings, options);

  // Determine the stream processing settings to be used
  const defaultSettings = streamType === DYNAMODB_STREAM_TYPE ?
    getDefaultDynamoDBStreamProcessingSettings(options) : getDefaultKinesisStreamProcessingSettings(options);

  const streamProcessingSettings = settingsAvailable ?
    Objects.merge(defaultSettings, settings, false, false) : defaultSettings;

  // Configure stream processing with the given or derived stream processing settings
  configureStreamProcessingWithSettings(context, streamProcessingSettings, standardSettings, standardOptions, event,
    awsContext, forceConfiguration);

  // Log a warning if no settings and no options were provided and the default settings were applied
  if (!settingsAvailable && !optionsAvailable && (forceConfiguration || !streamProcessingWasConfigured)) {
    context.warn(`Stream processing was configured without settings or options - used default stream processing configuration (${stringify(streamProcessingSettings)})`);
  }
  return context;
}

/**
 * Configures the given context with the given stream processing settings, but only if stream processing is not already
 * configured on the given context OR if forceConfiguration is true, and with the given standard settings and options.
 *
 * Note that if either the given event or AWS context are undefined, then everything other than the region, stage and
 * AWS context will be configured. This missing configuration can be configured at a later point in your code by
 * invoking {@linkcode stages#configureRegionStageAndAwsContext}. This separation of configuration is primarily useful
 * for unit testing.
 *
 * @param {Object|StreamProcessing|StandardContext} context - the context onto which to configure the given stream processing settings and standard settings
 * @param {StreamProcessingSettings} settings - the stream processing settings to use
 * @param {StandardSettings|undefined} [standardSettings] - optional standard settings to use to configure dependencies
 * @param {StandardOptions|undefined} [standardOptions] - optional standard options to use to configure dependencies
 * @param {AwsEvent|undefined} [event] - the AWS event, which was passed to your lambda
 * @param {AwsContext|undefined} [awsContext] - the AWS context, which was passed to your lambda
 * @param {boolean|undefined} [forceConfiguration] - whether or not to force configuration of the given settings and
 * options, which will override any previously configured stream processing and stage handling settings on the given context
 * @return {StreamProcessing} the context object configured with stream processing (either existing or new) and standard settings
 */
function configureStreamProcessingWithSettings(context, settings, standardSettings, standardOptions, event, awsContext, forceConfiguration) {
  // Configure all of the stream processing dependencies if not configured by configuring the given context as a
  // standard context with stage handling, logging, custom settings, an optional Kinesis instance and an optional
  // DynamoDB.DocumentClient instance using the given standard settings and standard options and ALSO optionally with
  // the current region, resolved stage and AWS context, if BOTH the optional given event and optional given AWS context
  // are defined
  contexts.configureStandardContext(context, standardSettings, standardOptions, event, awsContext, forceConfiguration);

  // If forceConfiguration is false check if the given context already has stream processing configured on it
  // and, if so, do nothing more and simply return the context as is (to prevent overriding an earlier configuration)
  if (!forceConfiguration && isStreamProcessingConfigured(context)) {
    return context;
  }

  // Configure stream processing with the given settings
  context.streamProcessing = settings;

  // Validate the stream processing configuration
  validateStreamProcessingConfiguration(context);

  return context;
}

/**
 * Attempts to resolve the stream type from the given stream processing settings and/or options (if any).
 * @param {StreamProcessingSettings|undefined} [settings] - optional stream processing settings to use
 * @param {StreamProcessingOptions|undefined} [options] - optional stream processing options to use
 * @returns {string} the stream type resolved from the given stream processing settings and/or options (if any)
 */
function resolveStreamType(settings, options) {
  const streamType = settings && typeof settings === 'object' && isNotBlank(settings.streamType) ?
    settings.streamType : options && typeof options === 'object' && isNotBlank(options.streamType) ?
    options.streamType : undefined;

  return isNotBlank(streamType) ? trim(streamType).toLowerCase() : streamType;
}

/**
 * Configures the given context as a standard context with the given standard settings and standard options and ALSO
 * with the default Kinesis stream processing settings partially overridden by the given stream processing options (if
 * any), but ONLY if stream processing is not already configured on the given context OR if forceConfiguration is true.
 *
 * Default Kinesis stream processing assumes the following:
 * - The stream event record is a Kinesis record
 * - The message is a JSON object serialized in base 64 format within the Kinesis record's data property
 * - See {@linkcode streamProcessing#extractMessageFromKinesisRecord} for the default extractMessageFromRecord
 *   implementation
 *
 * This behaviour can be changed by providing an alternative extractMessageFromRecord function via
 * {@linkcode configureStreamProcessing}.
 *
 * @see {@linkcode configureStreamProcessing} for more information.
 *
 * Note that if either the given event or AWS context are undefined, then everything other than the region, stage and
 * AWS context will be configured. This missing configuration can be configured at a later point in your code by
 * invoking {@linkcode stages#configureRegionStageAndAwsContext}. This separation of configuration is primarily useful
 * for unit testing.
 *
 * @param {Object|StreamProcessing|StandardContext} context - the context onto which to configure the default stream processing settings
 * @param {StreamProcessingOptions|undefined} [options] - optional stream processing options to use
 * @param {StandardSettings|undefined} [standardSettings] - optional standard settings to use to configure dependencies
 * @param {StandardOptions|undefined} [standardOptions] - optional standard options to use to configure dependencies
 * @param {AwsEvent|undefined} [event] - the AWS event, which was passed to your lambda
 * @param {AwsContext|undefined} [awsContext] - the AWS context, which was passed to your lambda
 * @param {boolean|undefined} forceConfiguration - whether or not to force configuration of the given settings, which
 * will override any previously configured stream processing and stage handling settings on the given context
 * @return {StreamProcessing} the context object configured with Kinesis stream processing settings (either existing or defaults)
 */
function configureDefaultKinesisStreamProcessing(context, options, standardSettings, standardOptions, event, awsContext, forceConfiguration) {
  // Get the default Kinesis stream processing settings from the local options file
  const settings = getDefaultKinesisStreamProcessingSettings(options);

  // Configure the context with the default stream processing settings defined above
  configureStreamProcessingWithSettings(context, settings, standardSettings, standardOptions, event, awsContext,
    forceConfiguration);

  return context;
}

/**
 * Configures the given context as a standard context with the given standard settings and standard options and ALSO
 * with the default DynamoDB stream processing settings partially overridden by the given stream processing options (if
 * any), but ONLY if stream processing is not already configured on the given context OR if forceConfiguration is true.
 *
 * Default DynamoDB stream processing assumes the following:
 * - The stream event record is a DynamoDB stream event record
 * - The message is the DynamoDB stream event record
 * - See {@linkcode streamProcessing#extractMessageFromDynamoDBRecord} for the default extractMessageFromRecord
 *   implementation
 *
 * This behaviour can be changed by providing an alternative extractMessageFromRecord function via
 * {@linkcode configureStreamProcessing}.
 *
 * @see {@linkcode configureStreamProcessing} for more information.
 *
 * Note that if either the given event or AWS context are undefined, then everything other than the region, stage and
 * AWS context will be configured. This missing configuration can be configured at a later point in your code by
 * invoking {@linkcode stages#configureRegionStageAndAwsContext}. This separation of configuration is primarily useful
 * for unit testing.
 *
 * @param {Object|StreamProcessing|StandardContext} context - the context onto which to configure the default stream processing settings
 * @param {StreamProcessingOptions|undefined} [options] - optional stream processing options to use
 * @param {StandardSettings|undefined} [standardSettings] - optional standard settings to use to configure dependencies
 * @param {StandardOptions|undefined} [standardOptions] - optional standard options to use to configure dependencies
 * @param {AwsEvent|undefined} [event] - the AWS event, which was passed to your lambda
 * @param {AwsContext|undefined} [awsContext] - the AWS context, which was passed to your lambda
 * @param {boolean|undefined} [forceConfiguration] - whether or not to force configuration of the given settings, which
 * will override any previously configured stream processing and stage handling settings on the given context
 * @return {StreamProcessing} the context object configured with DynamoDB stream processing settings (either existing or defaults)
 */
function configureDefaultDynamoDBStreamProcessing(context, options, standardSettings, standardOptions, event, awsContext, forceConfiguration) {
  // Get the default DynamoDB stream processing settings from the local options file
  const settings = getDefaultDynamoDBStreamProcessingSettings(options);

  // Configure the context with the default stream processing settings defined above
  configureStreamProcessingWithSettings(context, settings, standardSettings, standardOptions, event, awsContext, forceConfiguration);

  return context;
}

/**
 * Returns the default Kinesis stream processing settings partially overridden by the given stream processing options
 * (if any).
 *
 * This function is used internally by {@linkcode configureDefaultKinesisStreamProcessing}, but could also be used in
 * custom configurations to get the default settings as a base to be overridden with your custom settings before calling
 * {@linkcode configureStreamProcessing}.
 *
 * @param {StreamProcessingOptions} [options] - optional stream processing options to use to override the default options
 * @returns {StreamProcessingSettings} a stream processing settings object (including both property and function settings)
 */
function getDefaultKinesisStreamProcessingSettings(options) {
  const settings = options && typeof options === 'object' ? Objects.copy(options, true) : {};

  // Load defaults from local default-kinesis-options.json file
  const defaultOptions = loadDefaultKinesisStreamProcessingOptions();
  Objects.merge(defaultOptions, settings, false, false);

  const defaultSettings = {
    // Configurable processing functions
    extractMessageFromRecord: extractJsonMessageFromKinesisRecord,
    loadTaskTrackingState: skipLoadTaskTrackingState,
    saveTaskTrackingState: skipSaveTaskTrackingState,
    handleIncompleteMessages: resubmitIncompleteMessagesToKinesis,
    discardUnusableRecords: discardUnusableRecordsToDRQ,
    discardRejectedMessages: discardRejectedMessagesToDMQ,
  };
  return Objects.merge(defaultSettings, settings, false, false);
}

/**
 * Returns the default DynamoDB stream processing settings partially overridden by the given stream processing options
 * (if any).
 *
 * This function is used internally by {@linkcode configureDefaultDynamoDBStreamProcessing}, but could also be used in
 * custom configurations to get the default settings as a base to be overridden with your custom settings before calling
 * {@linkcode configureStreamProcessing}.
 *
 * @param {StreamProcessingOptions} [options] - optional stream processing options to use to override the default options
 * @returns {StreamProcessingSettings} a stream processing settings object (including both property and function settings)
 */
function getDefaultDynamoDBStreamProcessingSettings(options) {
  const settings = options && typeof options === 'object' ? Objects.copy(options, true) : {};

  // Load defaults from local default-dynamodb-options.json file
  const defaultOptions = loadDefaultDynamoDBStreamProcessingOptions();
  Objects.merge(defaultOptions, settings, false, false);

  const defaultSettings = {
    // Configurable processing functions
    extractMessageFromRecord: useStreamEventRecordAsMessage,
    loadTaskTrackingState: loadTaskTrackingStateFromDynamoDB,
    saveTaskTrackingState: saveTaskTrackingStateToDynamoDB,
    handleIncompleteMessages: replayAllMessagesIfIncomplete,
    discardUnusableRecords: discardUnusableRecordsToDRQ,
    discardRejectedMessages: discardRejectedMessagesToDMQ,
  };
  return Objects.merge(defaultSettings, settings, false, false);
}

/**
 * Loads the default Kinesis stream processing options from the local default-kinesis-options.json file and fills in any missing
 * options with the static default options.
 * @returns {StreamProcessingOptions} the default stream processing options
 */
function loadDefaultKinesisStreamProcessingOptions() {
  const options = require('./default-kinesis-options.json');
  const defaultOptions = options && options.streamProcessingOptions && typeof options.streamProcessingOptions === 'object' ?
    options.streamProcessingOptions : {};

  const defaults = {
    // Generic settings
    streamType: KINESIS_STREAM_TYPE,
    taskTrackingName: 'taskTracking',
    timeoutAtPercentageOfRemainingTime: 0.9,
    maxNumberOfAttempts: 10,
    // Specialised settings needed by implementations using external task tracking
    // taskTrackingTableName: undefined,
    // Specialised settings needed by default implementations - e.g. DRQ and DMQ stream names
    deadRecordQueueName: 'DeadRecordQueue',
    deadMessageQueueName: 'DeadMessageQueue',
    // Kinesis & DynamoDB.DocumentClient options
    kinesisOptions: {},
    // dynamoDBDocClientOptions: undefined
  };
  return Objects.merge(defaults, defaultOptions, false, false);
}

/**
 * Loads the default DynamoDB stream processing options from the local default-dynamodb-options.json file and fills in any
 * missing options with the static default options.
 * @returns {StreamProcessingOptions} the default stream processing options
 */
function loadDefaultDynamoDBStreamProcessingOptions() {
  const options = require('./default-dynamodb-options.json');
  const defaultOptions = options && options.streamProcessingOptions && typeof options.streamProcessingOptions === 'object' ?
    options.streamProcessingOptions : {};

  const defaults = {
    // Generic settings
    streamType: DYNAMODB_STREAM_TYPE,
    taskTrackingName: 'taskTracking',
    timeoutAtPercentageOfRemainingTime: 0.9,
    maxNumberOfAttempts: 10,
    // Specialised settings needed by default DynamoDB implementations or implementations using external task tracking
    taskTrackingTableName: 'MessageTaskTracking',
    // Specialised settings needed by default implementations - e.g. DRQ and DMQ stream names
    deadRecordQueueName: 'DeadRecordQueue',
    deadMessageQueueName: 'DeadMessageQueue',
    // Kinesis & DynamoDB.DocumentClient options
    kinesisOptions: {},
    dynamoDBDocClientOptions: {}
  };

  return Objects.merge(defaults, defaultOptions, false, false);
}

function validateStreamProcessingConfiguration(context) {
  if (!getExtractMessageFromRecordFunction(context)) {
    const errMsg = `FATAL - Cannot extract any messages from any stream event records without a valid, configured extractMessageFromRecord function. Fix your Lambda by configuring a valid streamProcessing.extractMessageFromRecord function on its context via configureStreamProcessing and redeploy ASAP, since this issue is blocking all of your stream's shards!`;
    (context.error ? context.error : console.error)(errMsg);
    throw new Error(errMsg);
  }

  if (!getLoadTaskTrackingStateFunction(context)) {
    const errMsg = `FATAL - Cannot load task tracking state for any messages without a valid, configured loadTaskTrackingState function. Fix your Lambda by configuring a valid streamProcessing.loadTaskTrackingState function on its context via configureStreamProcessing and redeploy ASAP, since this issue is blocking all of your stream's shards!`;
    (context.error ? context.error : console.error)(errMsg);
    throw new Error(errMsg);
  }

  if (!getSaveTaskTrackingStateFunction(context)) {
    const errMsg = `FATAL - Cannot save task tracking state for any messages without a valid, configured saveTaskTrackingState function. Fix your Lambda by configuring a valid streamProcessing.saveTaskTrackingState function on its context via configureStreamProcessing and redeploy ASAP, since this issue is blocking all of your stream's shards!`;
    (context.error ? context.error : console.error)(errMsg);
    throw new Error(errMsg);
  }

  if (!getDiscardUnusableRecordsFunction(context)) {
    const errMsg = `FATAL - Cannot discard any unusable stream event records without a valid, configured discardUnusableRecords function. Fix your Lambda by configuring a valid streamProcessing.discardUnusableRecords function on its context via configureStreamProcessing and redeploy ASAP, since this issue is blocking all of your stream's shards!`;
    (context.error ? context.error : console.error)(errMsg);
    throw new Error(errMsg);
  }

  if (!getDiscardRejectedMessagesFunction(context)) {
    const errMsg = `FATAL - Cannot discard any rejected messages without a valid, configured discardRejectedMessages function. Fix your Lambda by configuring a valid streamProcessing.discardRejectedMessages function on its context via configureStreamProcessing and redeploy ASAP, since this issue is blocking all of your stream's shards!`;
    (context.error ? context.error : console.error)(errMsg);
    throw new Error(errMsg);
  }

  if (!getHandleIncompleteMessagesFunction(context)) {
    const errMsg = `FATAL - Cannot handle any incomplete messages without a valid, configured handleIncompleteMessages function. Fix your Lambda by configuring a valid streamProcessing.handleIncompleteMessages function on its context via configureStreamProcessing and redeploy ASAP, since this issue is blocking all of your stream's shards!`;
    (context.error ? context.error : console.error)(errMsg);
    throw new Error(errMsg);
  }
}

/**
 * Returns the value of the named stream processing setting (if any) on the given context.
 * @param {StreamProcessing} context - the context from which to fetch the named setting's value
 * @param {string} settingName - the name of the stream processing setting
 * @returns {*|undefined} the value of the named setting (if any); otherwise undefined
 */
function getStreamProcessingSetting(context, settingName) {
  return context && context.streamProcessing && isNotBlank(settingName) && context.streamProcessing[settingName] ?
    context.streamProcessing[settingName] : undefined;
}

/**
 * Returns the stream type configured on the given context.
 * @param {StreamProcessing} context - the context from which to fetch the stream type
 * @returns {string|undefined} the stream type (if any); otherwise undefined
 */
function getStreamType(context) {
  return getStreamProcessingSetting(context, STREAM_TYPE_SETTING);
}

function isKinesisStreamType(context) {
  return getStreamType(context) === KINESIS_STREAM_TYPE;
}

function isDynamoDBStreamType(context) {
  return getStreamType(context) === DYNAMODB_STREAM_TYPE;
}

/**
 * Returns the maximum number of attempts configured on the given context.
 * @param {StreamProcessing} context - the context from which to fetch the maximum number of attempts
 * @returns {number|undefined} the maximum number of attempts (if any); otherwise undefined
 */
function getMaxNumberOfAttempts(context) {
  return getStreamProcessingSetting(context, MAX_NUMBER_OF_ATTEMPTS_SETTING);
}

/**
 * Returns the function configured at the named stream processing setting on the given context (if any and if it's a
 * real function); otherwise returns undefined.
 * @param {StreamProcessing} context - the context from which to fetch the function
 * @param {string} settingName - the name of the stream processing setting
 * @returns {Function|undefined} the named function (if it's a function); otherwise undefined
 */
function getStreamProcessingFunction(context, settingName) {
  const fn = getStreamProcessingSetting(context, settingName);
  return typeof fn === 'function' ? fn : undefined;
}

/**
 * Returns the extractMessageFromRecord function configured on the given context (if any and if it's a real function);
 * otherwise returns undefined.
 * @param {StreamProcessing} context - the context from which to fetch the function
 * @returns {Function|undefined} the extractMessageFromRecord function (if it's a function); otherwise undefined
 */
function getExtractMessageFromRecordFunction(context) {
  return getStreamProcessingFunction(context, EXTRACT_MESSAGE_FROM_RECORD_SETTING);
}

/**
 * A default extractMessageFromRecord function that attempts to extract and parse the original JSON message object from
 * the given Kinesis stream event record and returns the message (if parsable) or throws an error (if not).
 *
 * @param {Record} record - a Kinesis stream event record
 * @param {StreamProcessing} context - the context
 * @return {Message} the message object (if successfully extracted)
 * @throws {Error} an error if a message could not be successfully extracted from the given record
 */
function extractJsonMessageFromKinesisRecord(record, context) {
  // First convert the Kinesis record's kinesis.data field back from Base 64 to UTF-8
  const msgData = new Buffer(record.kinesis.data, 'base64').toString('utf-8');

  if (context.traceEnabled) context.trace(`Parsing Kinesis record data (${msgData})`);

  try {
    // Convert the decoded record data back into its original JSON message object form
    return JSON.parse(msgData);

  } catch (err) {
    context.error(`Failed to parse decoded Kinesis record data (${msgData}) back to a JSON message object`, err.stack);
    throw err;
  }
}

/**
 * A default extractMessageFromRecord function that simply returns the given stream event record as the message object
 * (if defined) or throws an error (if not).
 *
 * @param {Record} record - a stream event record
 * @param {StreamProcessing} context - the context
 * @return {Message} the message object if defined
 * @throws {Error} an error if the given stream event record is not defined
 */
function useStreamEventRecordAsMessage(record, context) {
  if (!record || typeof record !== 'object') {
    const errMsg = `No stream event record (${record}) to use as the message`;
    context.error(errMsg);
    throw new Error(errMsg);
  }
  // Use the given stream event record as the message
  if (context.traceEnabled) context.trace(`Using stream event record (${stringify(record)}) as the message`);
  return record;
}

/**
 * Returns the discardUnusableRecords function configured on the given context (if any and if it's a real function);
 * otherwise returns undefined.
 * @param {StreamProcessing} context - the context from which to fetch the function
 * @returns {Function|undefined} the discardUnusableRecords function (if it's a function); otherwise undefined
 */
function getDiscardUnusableRecordsFunction(context) {
  return getStreamProcessingFunction(context, DISCARD_UNUSABLE_RECORDS_SETTING);
}

/**
 * Discards all the given unusable stream event records to the DRQ (i.e. Dead Record Queue).
 * @param {Record[]} unusableRecords - the list of unusable records to discard
 * @param {StreamProcessing} context - the context to use
 * @return {Promise} a promise that will complete when all of its discard unusable record promises complete
 */
function discardUnusableRecordsToDRQ(unusableRecords, context) {
  if (!unusableRecords || unusableRecords.length <= 0) {
    return Promise.resolve([]);
  }
  const kinesis = getKinesis(context);

  // Get the stage-qualified version of the DRQ stream name
  const unqualifiedDeadRecordQueueName = context.streamProcessing.deadRecordQueueName;
  const deadRecordQueueName = stages.toStageQualifiedStreamName(unqualifiedDeadRecordQueueName, context.stage, context);

  // Discard all of the unusable records
  const promises = unusableRecords.map(record => {
    const request = toDRQPutRequestFromUnusableRecord(record, deadRecordQueueName, context);
    return kinesis.putRecord(request).promise();
  });
  const m = unusableRecords.length;
  const plural = m !== 1 ? 's' : '';

  return Promise.all(promises)
    .then(results => {
      context.info(`Discarded ${m} unusable record${plural} to Kinesis DRQ (${deadRecordQueueName})`);
      return results;
    })
    .catch(err => {
      context.error(`Failed to discard ${m} unusable record${plural} to Kinesis DRQ (${deadRecordQueueName}) - error (${err})`, err.stack);
      throw err;
    });
}

function toDRQPutRequestFromUnusableRecord(record, deadRecordQueueName, context) {
  if (record.eventSource === 'aws:kinesis') {
    return toDRQPutRequestFromKinesisUnusableRecord(record, deadRecordQueueName)
  } else if (record.eventSource === 'aws:dynamodb') {
    return toDRQPutRequestFromDynamoDBUnusableRecord(record, deadRecordQueueName)
  } else {
    const errMsg = `Cannot convert unusable record to DRQ request with unexpected record eventSource (${record.eventSource})`;
    context.error(errMsg);
    throw new Error(errMsg);
  }
}

function toDRQPutRequestFromKinesisUnusableRecord(record, deadRecordQueueName) {
  const partitionKey = record.kinesis.partitionKey;
  const explicitHashKey = record.kinesis.explicitHashKey;

  const request = {
    StreamName: deadRecordQueueName,
    PartitionKey: partitionKey,
    Data: JSON.stringify(record)
  };
  if (explicitHashKey) {
    request.ExplicitHashKey = explicitHashKey;
  }
  return request;
}

function toDRQPutRequestFromDynamoDBUnusableRecord(record, deadRecordQueueName) {
  // Get the original record's event source stream name
  const eventSourceStreamName = trim(streamEvents.getKinesisEventSourceStreamName(record));
  const sourceStreamName = isNotBlank(eventSourceStreamName) ? eventSourceStreamName : '';

  // Combine all of the record's Keys into a single string
  const keysAndValues = record.dynamodb ? dynamoDBUtils.toKeyValueStrings(record.dynamodb.Keys).join('|') : '';

  // Generate a partition key to use for the DMQ request
  const partitionKey = `${sourceStreamName}|${keysAndValues}`.substring(0, MAX_PARTITION_KEY_SIZE);

  // Construct a Kinesis putRecord request to be sent to the DRQ
  return {
    StreamName: deadRecordQueueName,
    PartitionKey: partitionKey,
    Data: JSON.stringify(record)
  };
}

/**
 * Returns the discardRejectedMessages function configured on the given context (if any and if it's a real function);
 * otherwise returns undefined.
 * @param {StreamProcessing} context - the context from which to fetch the function
 * @returns {Function|undefined} the discardRejectedMessages function (if it's a function); otherwise undefined
 */
function getDiscardRejectedMessagesFunction(context) {
  return getStreamProcessingFunction(context, DISCARD_REJECTED_MESSAGES_SETTING);
}

/**
 * Routes all the given rejected messages to the DMQ (i.e. Dead Message Queue).
 * @param {Message[]} rejectedMessages the list of rejected messages to discard
 * @param {StreamProcessing} context the context to use
 * @return {Promise}
 */
function discardRejectedMessagesToDMQ(rejectedMessages, context) {
  if (!rejectedMessages || rejectedMessages.length <= 0) {
    return Promise.resolve([]);
  }
  const kinesis = getKinesis(context);

  // Get the stage-qualified version of the DRQ stream name
  const unqualifiedDeadMessageQueueName = context.streamProcessing.deadMessageQueueName;
  const deadMessageQueueName = stages.toStageQualifiedStreamName(unqualifiedDeadMessageQueueName, context.stage, context);

  // Discard all of the rejected messages to the DMQ
  const promises = rejectedMessages.map(message => {
    const request = toDMQPutRequestFromRejectedMessage(message, deadMessageQueueName, context);
    return kinesis.putRecord(request).promise();
  });
  const m = rejectedMessages.length;
  const plural = m !== 1 ? 's' : '';

  return Promise.all(promises)
    .then(results => {
      context.info(`Discarded ${m} rejected message${plural} to Kinesis DMQ (${deadMessageQueueName})`);
      return results;
    })
    .catch(err => {
      context.error(`Failed to discard ${m} rejected message${plural} to Kinesis DMQ (${deadMessageQueueName}) - error (${err})`, err.stack);
      throw err;
    });
}

function toDMQPutRequestFromRejectedMessage(message, deadRecordQueueName, context) {
  const record = getRecord(message, context);
  const eventSource = record.eventSource;

  if (record.eventSource === 'aws:kinesis') {
    return toDMQPutRequestFromKinesisRejectedMessage(message, record, deadRecordQueueName)
  } else if (record.eventSource === 'aws:dynamodb') {
    return toDMQPutRequestFromDynamoDBRejectedMessage(message, record, deadRecordQueueName)
  } else {
    const errMsg = `Cannot convert unusable record to DRQ request with unexpected record eventSource (${record.eventSource})`;
    context.error(errMsg);
    throw new Error(errMsg);
  }
}

function toDMQPutRequestFromKinesisRejectedMessage(message, record, deadMessageQueueName) {
  // Get the original record's event source stream name
  const eventSourceStreamName = trim(streamEvents.getKinesisEventSourceStreamName(record));
  const sourceStreamName = isNotBlank(eventSourceStreamName) ? eventSourceStreamName : '';

  // Get the original Kinesis record's key information
  const sourcePartitionKey = record.kinesis.partitionKey;
  const sourceExplicitHashKey = record.kinesis.explicitHashKey;
  const sourceSequenceNumber = record.kinesis.sequenceNumber;

  // Wrap the message in a rejected message "envelope" with metadata
  const rejectedMessage = {
    message: message,
    source: {
      streamName: sourceStreamName,
      partitionKeyOrKeys: sourcePartitionKey,
      sequenceNumber: sourceSequenceNumber,
    },
    discardedAt: new Date().toISOString()
  };
  if (sourceExplicitHashKey) {
    rejectedMessage.source.explicitHashKey = sourceExplicitHashKey;
  }

  // Construct a Kinesis putRecord request to be sent to the DMQ
  const request = {
    StreamName: deadMessageQueueName,
    PartitionKey: sourcePartitionKey,
    Data: JSON.stringify(rejectedMessage)
  };
  if (sourceExplicitHashKey) {
    request.ExplicitHashKey = sourceExplicitHashKey;
  }
  return request;
}

function toDMQPutRequestFromDynamoDBRejectedMessage(message, record, deadMessageQueueName) {
  // Get the original record's event source stream name
  const eventSourceStreamName = trim(streamEvents.getKinesisEventSourceStreamName(record));
  const sourceStreamName = isNotBlank(eventSourceStreamName) ? eventSourceStreamName : '';

  const sourceKeys = record.dynamodb ? JSON.stringify(record.dynamodb.Keys) : '';
  const sourceSequenceNumber = record.dynamodb ? record.dynamodb.SequenceNumber : '';

  // Wrap the message in a rejected message "envelope" with metadata
  const rejectedMessage = {
    message: message,
    source: {
      streamName: sourceStreamName,
      partitionKeyOrKeys: sourceKeys,
      sequenceNumber: sourceSequenceNumber,
    },
    discardedAt: new Date().toISOString()
  };

  // Combine all of the record's Keys into a single string
  const keysAndValues = record.dynamodb ? dynamoDBUtils.toKeyValueStrings(record.dynamodb.Keys).join('|') : '';

  // Generate a partition key to use for the DMQ request
  const partitionKey = `${sourceStreamName}|${keysAndValues}`.substring(0, MAX_PARTITION_KEY_SIZE);

  // Construct a Kinesis putRecord request to be sent to the DMQ
  return {
    StreamName: deadMessageQueueName,
    PartitionKey: partitionKey,
    Data: JSON.stringify(rejectedMessage)
  };
}

/**
 * Returns the loadTaskTrackingState function configured on the given context (if any and if it's a real function);
 * otherwise returns undefined.
 * @param {StreamProcessing} context - the context from which to fetch the function
 * @returns {Function|undefined} the loadTaskTrackingState function (if it's a function); otherwise undefined
 */
function getLoadTaskTrackingStateFunction(context) {
  return getStreamProcessingFunction(context, LOAD_TASK_TRACKING_STATE_SETTING);
}

/**
 * Returns the saveTaskTrackingState function configured on the given context (if any and if it's a real function);
 * otherwise returns undefined.
 * @param {StreamProcessing} context - the context from which to fetch the function
 * @returns {Function|undefined} the saveTaskTrackingState function (if it's a function); otherwise undefined
 */
function getSaveTaskTrackingStateFunction(context) {
  return getStreamProcessingFunction(context, SAVE_TASK_TRACKING_STATE_SETTING);
}

/**
 * Returns the handleIncompleteMessages function configured on the given context (if any and if it's a real function);
 * otherwise returns undefined.
 * @param {StreamProcessing} context - the context from which to fetch the function
 * @returns {Function|undefined} the handleIncompleteMessages function (if it's a function); otherwise undefined
 */
function getHandleIncompleteMessagesFunction(context) {
  return getStreamProcessingFunction(context, HANDLE_INCOMPLETE_MESSAGES_SETTING);
}

/**
 * A default handleIncompleteMessages function that attempts to resubmit all of the given incomplete messages back to
 * their source Kinesis stream.
 * @param {Message[]} messages - the entire batch of messages
 * @param {Message[]} incompleteMessages - the incomplete messages to be resubmitted
 * @param {StreamProcessing} context - the context
 * @returns {Promise} a promise that will complete when all of the resubmit incomplete message promises have completed
 */
function resubmitIncompleteMessagesToKinesis(messages, incompleteMessages, context) {
  const m = messages.length;
  const ms = `${m} message${m !== 1 ? 's' : ''}`;
  const i = incompleteMessages ? incompleteMessages.length : 0;
  const is = `${i} incomplete message${i !== 1 ? 's' : ''}`;
  const isOfMs = `${is} of ${ms}`;

  if (i <= 0) {
    return Promise.resolve([]);
  }
  const kinesis = getKinesis(context);

  context.debug(`Resubmitting ${isOfMs} back to Kinesis`);

  function resubmitMessage(message) {
    const record = getRecord(message, context);

    // Get the name of the source stream, from which this Kinesis record was received (which should already be stage-qualified)
    const sourceStreamName = trim(streamEvents.getKinesisEventSourceStreamName(record));

    const sourcePartitionKey = record.kinesis.partitionKey;
    const sourceExplicitHashKey = record.kinesis.explicitHashKey;
    const sourceSequenceNumber = record.kinesis.sequenceNumber;

    if (isBlank(sourceStreamName)) {
      const errMsg = `FATAL - Cannot resubmit message back to Kinesis, since failed to resolve the source stream name from eventSourceARN (${record.eventSourceARN}) of record with partition key (${sourcePartitionKey})${isNotBlank(sourceExplicitHashKey) ? `, explicit hash key (${sourceExplicitHashKey})` : ''} & sequence number (${sourceSequenceNumber})`;
      context.error(errMsg);
      return Promise.reject(new Error(errMsg));
    }

    // Generate a Kinesis putRecord request for the message
    const request = {
      StreamName: sourceStreamName,
      PartitionKey: sourcePartitionKey,
      SequenceNumberForOrdering: sourceSequenceNumber,
      Data: JSON.stringify(message)
    };
    if (isNotBlank(sourceExplicitHashKey)) {
      request.ExplicitHashKey = sourceExplicitHashKey;
    }

    // Resubmit message to kinesis
    return kinesis.putRecord(request).promise();
  }

  // Resubmit all of the incomplete messages
  const promises = incompleteMessages.map(message => resubmitMessage(message));

  return Promise.all(promises)
    .then(results => {
      context.info(`Resubmitted ${isOfMs} back to Kinesis`);
      return results;
    })
    .catch(err => {
      context.error(`Failed to resubmit ${isOfMs} back to Kinesis - error (${err})`, err.stack);
      throw err;
    });
}

/**
 * A default handleIncompleteMessages function that simply returns a rejected Promise if there are any incomplete
 * messages to trigger a replay of all of the messages in the batch, which is unfortunately the only way to replay
 * incomplete DynamoDB stream event records, because they do not appear to have a putRecord function that would enable
 * resubmission and because sequence is critical for DynamoDB stream events.
 * @param {Message[]} messages - the entire batch of messages
 * @param {Message[]} incompleteMessages - the incomplete messages
 * @param {StreamProcessing} context - the context
 * @returns {Promise} a promise that will either reject (if there are any incomplete messages) or resolve successfully (if not)
 */
function replayAllMessagesIfIncomplete(messages, incompleteMessages, context) {
  const m = messages.length;
  const ms = `${m} message${m !== 1 ? 's' : ''}`;
  const i = incompleteMessages ? incompleteMessages.length : 0;
  const is = `${i} incomplete message${i !== 1 ? 's' : ''}`;
  const isOfMs = `${is} of ${ms}`;

  if (!incompleteMessages || i <= 0) {
    context.info(`No need to trigger replay of entire batch of messages, since have ${isOfMs}`);
    return Promise.resolve([]);
  }

  const msg = `Triggering replay of entire batch of messages, since still have ${isOfMs}`;
  context.info(msg);
  return Promise.reject(new Error(msg));
}

function getKinesis(context) {
  if (!context.kinesis) {
    // Configure a default Kinesis instance on context.kinesis if not already configured
    const kinesisOptions = require('./default-kinesis-options.json').kinesisOptions;
    context.warn(`An AWS Kinesis instance was not configured on context.kinesis yet - configuring an instance with default options (${stringify(kinesisOptions)}). Preferably configure this beforehand, using aws-core-utils/kinesis-cache#configureKinesis`);
    kinesisCache.configureKinesis(context, kinesisOptions);
  }
  return context.kinesis;
}

function getDynamoDBDocClient(context) {
  if (!context.dynamoDBDocClient) {
    // Configure a default AWS DynamoDB.DocumentClient instance on context.dynamoDBDocClient if not already configured
    const dynamoDBDocClientOptions = require('./default-dynamodb-options.json').dynamoDBDocClientOptions;
    context.warn(`An AWS DynamoDB.DocumentClient instance was not configured on context.dynamoDBDocClient yet - configuring an instance with default options (${stringify(dynamoDBDocClientOptions)}). Preferably configure this beforehand, using aws-core-utils/dynamodb-doc-client-cache#configureDynamoDBDocClient`);
    dynamoDBDocClientCache.configureDynamoDBDocClient(context, dynamoDBDocClientOptions);
  }
  return context.dynamoDBDocClient;
}

function getRecord(message, context) {
  const taskTrackingName = context.streamProcessing.taskTrackingName;
  const taskTracking = message[taskTrackingName];
  return taskTracking ? taskTracking.record : undefined;
}

/**
 * A default loadTaskTrackingState function that does nothing other than returning the given messages in a Promise,
 * since the default Kinesis stream consumer behaviour is to resubmit incomplete messages along with their task tracking
 * state back to Kinesis, which means no task tracking state needs to be saved externally.
 * @param {Message[]} messages - the entire batch of messages being processed
 * @param {StreamProcessing} context - the context to use
 * @returns {Promise.<*>} a promise that will do nothing other than return the given messages
 */
function skipLoadTaskTrackingState(messages, context) {
  const m = messages ? messages.length : 0;
  const ms = `${m} message${(m !== 1 ? 's' : '')}`;
  context.debug(`Skipping load of task tracking state for ${ms}`);
  return Promise.resolve(messages);
}

/**
 * A default saveTaskTrackingState function that does nothing other than returning the given messages in a Promise,
 * since the default Kinesis stream consumer behaviour is to resubmit incomplete messages along with their task tracking
 * state back to Kinesis, which means no task tracking state needs to be saved externally.
 * @param {Message[]} messages - the entire batch of messages being processed
 * @param {StreamProcessing} context - the context to use
 * @returns {Promise.<*>} a promise that will do nothing other than return the given messages
 */
function skipSaveTaskTrackingState(messages, context) {
  const m = messages ? messages.length : 0;
  const ms = `${m} message${(m !== 1 ? 's' : '')}`;
  context.debug(`Skipping save of task tracking state for ${ms}`);
  return Promise.resolve(messages);
}

function loadTaskTrackingStateFromDynamoDB(messages, context) {
  //TODO implement
  const m = messages ? messages.length : 0;
  const plural = m !== 1 ? 's' : '';
  const ms = `${m} message${plural}`;

  if (m <= 0) {
    context.debug(`No task tracking state to load, since ${ms}`);
    return Promise.resolve(messages);
  }

  //const taskTrackingName = context.streamProcessing.taskTrackingName;

}

function saveTaskTrackingStateToDynamoDB(messages, context) {
  //TODO implement
  const m = messages ? messages.length : 0;
  const plural = m !== 1 ? 's' : '';
  const ms = `${m} message${plural}`;

  if (m <= 0) {
    context.debug(`No task tracking state to save, since ${ms}`);
    return Promise.resolve(messages);
  }

  const taskTrackingName = context.streamProcessing.taskTrackingName;

  const unqualifiedTaskTrackingTableName = context.streamProcessing.taskTrackingTableName; //TODO configure
  const tableName = stages.toStageQualifiedResourceName(unqualifiedTaskTrackingTableName, context.stage, context);


  const dynamoDBDocClient = getDynamoDBDocClient(context);

  function saveMessageTaskTrackingDetails(message, context) {
    //TODO implement saveMessageTaskTrackingDetails
    throw new Error('TODO implement saveMessageTaskTrackingDetails');
  }

  // Resubmit all of the incomplete messages
  const promises = messages.map(message => saveMessageTaskTrackingDetails(message));

  return Promise.all(promises)
    .then(results => {
      context.info(`Saved task tracking details of ${m} incomplete message${plural} to DynamoDB table (${tableName}) - results: ${stringify(results)}`);
      throw new Error(`Triggering replay of batch after saving task tracking details of ${m} incomplete message${plural} to DynamoDB table (${tableName})`);
    })
    .catch(err => {
      context.error(`Failed to save task tracking details of ${m} incomplete message${plural} to DynamoDB table (${tableName}) - error (${err})`, err.stack);
      throw err;
    });
}