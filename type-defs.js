'use strict';

/**
 * @typedef {StreamProcessing} StreamConsuming - an object with stream consumer configuration including stream
 * processing settings, stage handling settings, logging functionality, a region, a stage and an AWS context
 * @property {string} region - the configured region to use
 * @property {string} stage - the configured stage to use
 * @property {Object} awsContext - the AWS context, which was passed to your lambda
 */

/**
 * @typedef {Object} StreamConsumerSettings - settings to be used to configure a stream consumer with stream processing
 * settings, stage handling settings and logging settings
 * @property {LoggingSettings|undefined} [loggingSettings] - optional logging settings to use to configure logging
 * @property {StageHandlingSettings|undefined} [stageHandlingSettings] - optional stage handling settings to use to configure stage handling
 * @property {StreamProcessingSettings|undefined} [streamProcessingSettings] - optional stream processing settings to use to configure stream processing
 */

/**
 * @typedef {Object} StreamConsumerOptions - options to be used to configure a stream consumer with stream processing
 * options, stage handling options and logging options if no corresponding settings are provided
 * @property {LoggingOptions|undefined} [loggingOptions] - optional logging options to use to configure logging
 * @property {StageHandlingOptions|undefined} [stageHandlingOptions] - optional stage handling options to use to configure stage handling
 * @property {StreamProcessingOptions|undefined} [streamProcessingOptions] - optional stream processing options to use to configure stream processing
 */

/**
 * @typedef {StageHandling} StreamProcessing - an object configured with stream processing settings, stage handling
 * settings, logging functionality, an AWS.Kinesis instance and an optional AWS.DynamoDB.DocumentClient
 * @property {StreamProcessingSettings} streamProcessing - the configured stream processing settings to use
 * @property {AWS.Kinesis} kinesis - an AWS.Kinesis instance to use
 * @property {AWS.DynamoDB.DocumentClient|undefined} [dynamoDBDocClient] - an optional AWS.DynamoDB.DocumentClient instance to use
 */

/**
 * Stream processing settings which configure and determine the processing behaviour of an AWS stream consumer.
 * @typedef {StreamProcessingOptions} StreamProcessingSettings
 * @property {Function} extractMessageFromRecord - a synchronous function that will be used to extract a message from a
 * given stream event record, which must accept a record and the given context as arguments and return the extracted
 * message or throw an exception if a message cannot be extracted from the record
 * @property {Function} loadTaskTrackingState - a function that will be used to load the task tracking state of the
 * entire batch of messages and that must accept: an array of the entire batch of messages and the context
 * @property {Function} saveTaskTrackingState - a function that will be used to save the task tracking state of the
 * entire batch of messages and that must accept: an array of the entire batch of messages and the context
 * @property {Function} handleIncompleteMessages - a function that will be used to handle any incomplete messages and
 * that must accept: an array of the entire batch of messages; an array of incomplete messages; and the context and
 * ideally return a promise
 * @property {Function} discardUnusableRecords - a function that will be used to discard any unusable records and that must
 * accept an array of unusable records and the context and ideally return a promise
 * @property {Function} discardRejectedMessages - a function that will be used to discard any rejected messages and that
 * must accept an array of rejected messages and the context and ideally return a promise
 */

/**
 * Stream processing options which configure ONLY the property (i.e. non-function) settings of an AWS stream consumer
 * and are a subset of the full StreamProcessingSettings.
 * @typedef {Object} StreamProcessingOptions
 * @property {string} streamType - the type of stream being processed - valid values are "kinesis" or "dynamodb"
 * @property {string} taskTrackingName - the name of the task tracking object property on each message, which has or
 * will be assigned two properties: a 'ones' object property; and an 'alls' object property. The 'ones' property is a
 * map of all of the processOne tasks (i.e. the tasks for processing a single message at a time) keyed by task name.
 * The 'alls' property is a map of all of the processAll tasks (i.e. the tasks for processing all of the messages that
 * were received in a batch from an AWS stream) keyed by task name
 * @property {number} timeoutAtPercentageOfRemainingTime - the percentage of the remaining time at which to timeout
 * processing (expressed as a number between 0.0 and 1.0, e.g. 0.9 would mean timeout at 90% of the remaining time)
 * @property {number} maxNumberOfAttempts - the maximum number of attempts on each of a message's tasks that are allowed
 * before discarding the message and routing it to the Dead Message Queue. Note that if a message has multiple tasks, it
 * will only be discarded when all of its tasks have reached this maximum
 * @property {string} taskTrackingTableName - the unqualified name of the Task Tracking table from which to load and/or
 * to which to save the task tracking state of the entire batch of messages
 * @property {string} deadRecordQueueName - the unqualified stream name of the Dead Record Queue to which to discard unusable records
 * @property {string} deadMessageQueueName - the unqualified stream name of the Dead Message Queue to which to discard rejected messages
 * @property {Object|undefined} [kinesisOptions] - optional Kinesis constructor options to use to configure an AWS.Kinesis instance
 * @property {Object|undefined} [dynamoDBDocClientOptions] - optional DynamoDB.DocumentClient constructor options to use to configure an AWS.DynamoDB.DocumentClient instance
 */

/**
 * @typedef {Object} SPOtherSettings - other settings to be used to configure stream processing dependencies
 * @property {LoggingSettings|undefined} [loggingSettings] - optional logging settings to use to configure logging
 * @property {StageHandlingSettings|undefined} [stageHandlingSettings] - optional stage handling settings to use to configure stage handling
 */

/**
 * @typedef {Object} SPOtherOptions - other options be used if no corresponding settings are provided to configure
 * stream processing dependencies
 * @property {LoggingOptions|undefined} [loggingOptions] - optional logging options to use to configure logging
 * @property {StageHandlingOptions|undefined} [stageHandlingOptions] - optional stage handling options to use to configure stage handling
 */

/**
 * @typedef {Object} StreamConsumerResults - the stream consumer results, which are returned when the stream consumer
 * completes successfully
 * @property {Object[]} messages - a list of zero or more successfully extracted message objects
 * @property {Object[]} unusableRecords - a list of zero or more unusable records
 * @property {Task} processing - a task that tracks the state of the processing phase
 * @property {Task|undefined} [finalising] - a task that tracks the state of the finalising phase
 * @property {Object[]|undefined} [savedMessagesTaskTrackingState] - an optional list of zero or more messages that had their task tracking state successfully saved
 * @property {Object[]|undefined} [handledIncompleteMessages] - an optional list of zero or more successfully handled incomplete messages
 * @property {Object[]|undefined} [discardedUnusableRecords] - an optional list of zero or more successfully discarded unusable records
 * @property {Object[]|undefined} [discardedRejectedMessages] - an optional list of zero or more successfully discarded rejected messages
 * @property {Error|undefined} [saveMessagesTaskTrackingStateError] - an optional error with which save messages task tracking state failed
 * @property {Error|undefined} [handleIncompleteMessagesError] - an optional error with which handle incomplete records failed
 * @property {Error|undefined} [discardUnusableRecordsError] - an optional error with which discard unusable records failed
 * @property {Error|undefined} [discardRejectedMessagesError] - an optional error with which discard rejected messages failed
 * @property {boolean|undefined} [partial] - whether these results are partial (i.e. not all available yet) or full results
 * @property {Promise.<Object[]|Error>|undefined} [saveMessagesTaskTrackingStatePromise] - a promise of either a resolved list of zero or more messages that had their task tracking state successfully saved or a rejected error
 * @property {Promise.<Object[]|Error>|undefined} [handleIncompleteMessagesPromise] - a promise of either a resolved list of zero or more successfully handled incomplete records or a rejected error
 * @property {Promise.<Object[]|Error>|undefined} [discardUnusableRecordsPromise] - a promise of either a resolved list of zero or more successfully discarded unusable records or a rejected error
 * @property {Promise.<Object[]|Error>|undefined} [discardRejectedMessagesPromise] - a promise of either a resolved list of zero or more successfully discarded rejected messages or a rejected error
 */

/**
 * @typedef {Object} SummarizedStreamConsumerResults - the summarized stream consumer results
 * @property {number} messages - the number of successfully extracted message objects
 * @property {number} unusableRecords - the number of unusable records
 * @property {Task} processing - a task that tracks the state of the processing phase
 * @property {Task|undefined} [finalising] - a task that tracks the state of the finalising phase
 * @property {number|undefined} [savedMessagesTaskTrackingState] - the number of messages that had their task tracking state successfully saved
 * @property {number|undefined} [handledIncompleteMessages] - the number of successfully handled incomplete messages
 * @property {number|undefined} [discardedUnusableRecords] - the number of successfully discarded unusable records
 * @property {number|undefined} [discardedRejectedMessages] - the number of successfully discarded rejected messages
 * @property {boolean|undefined} [partial] - whether these results are partial (i.e. not all available yet) or full results
 * @property {string|undefined} [saveMessagesTaskTrackingStateError] - an optional error with which save messages task tracking state failed
 * @property {string|undefined} [handleIncompleteMessagesError] - an optional error with which handle incomplete records failed
 * @property {string|undefined} [discardUnusableRecordsError] - an optional error with which discard unusable records failed
 * @property {string|undefined} [discardRejectedMessagesError] - an optional error with which discard rejected messages failed
 */

/**
 * @typedef {Error} StreamConsumerError - the final error returned via a rejected promise when the stream consumer fails or times out
 * @property {StreamConsumerResults|undefined} [streamConsumerResults] - the full stream consumer results or partial
 * results available at the time of the final error or timeout
 */

