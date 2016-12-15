'use strict';

/**
 * @typedef {StreamProcessing} StreamConsumerContext - a stream consumer context object configured with stream
 * processing and all of the standard context configuration, including stage handling, logging, custom settings, an
 * optional Kinesis instance, an optional DynamoDB.DocumentClient, the current region, the resolved stage and the AWS
 * context.
 * @property {string} region - the configured region to use
 * @property {string} stage - the configured stage to use
 * @property {AwsContext} awsContext - the AWS context, which was passed to your lambda
 */

/**
 * @typedef {StandardSettings} StreamConsumerSettings - settings to be used to configure a stream consumer context with
 * stream processing settings and all of the standard settings
 * @property {StreamProcessingSettings|undefined} [streamProcessingSettings] - optional stream processing settings to use to configure stream processing
 */

/**
 * @typedef {StandardOptions} StreamConsumerOptions - options to be used to configure a stream consumer context with
 * stream processing options and all of the standard options
 * @property {StreamProcessingOptions|undefined} [streamProcessingOptions] - optional stream processing options to use to configure stream processing
 */

/**
 * @typedef {StandardContext} StreamProcessing - an object configured with stream processing, stage handling,
 * logging, custom settings, an optional AWS.Kinesis instance and an optional AWS.DynamoDB.DocumentClient and also
 * OPTIONALLY with the current region, the resolved stage and the AWS context
 * @property {StreamProcessingSettings} streamProcessing - the configured stream processing settings to use
 */

/**
 * Stream processing settings which configure and determine the processing behaviour of an AWS stream consumer.
 *
 * @typedef {StreamProcessingOptions} StreamProcessingSettings
 *
 * @property {function(record: Record, context: StreamConsumerContext): Message} extractMessageFromRecord - a synchronous function that
 * will be used to extract a message from a given stream event record, which must accept a record and the given context
 * as arguments and return the extracted message or throw an exception if a message cannot be extracted from the record
 *
 * @property {function(messages: Message[], context: StreamConsumerContext): Promise.<Message[]>} loadTaskTrackingState - a function that will
 * be used to load the task tracking state of the entire batch of messages and that must accept: an array of the entire
 * batch of messages and the context
 *
 * @property {function(messages: Message[], context: StreamConsumerContext): Promise} saveTaskTrackingState - a function that will be
 * used to save the task tracking state of the entire batch of messages and that must accept: an array of the entire
 * batch of messages and the context
 *
 * @property {function(messages: Message[], incompleteMessages: Message[], context: StreamConsumerContext): Promise.<*>} handleIncompleteMessages -
 * a function that will be used to handle any incomplete messages and that must accept: an array of the entire batch of
 * messages; an array of incomplete messages; and the context and ideally return a promise
 *
 * @property {function(unusableRecords: Record[], context: StreamConsumerContext): Promise} discardUnusableRecords - a function that
 * will be used to discard any unusable records and that must accept an array of unusable records and the context and
 * ideally return a promise
 *
 * @property {function(rejectedMessages: Message[], context: StreamConsumerContext): Promise} discardRejectedMessages - a function that
 * will be used to discard any rejected messages and that must accept an array of rejected messages and the context and
 * ideally return a promise
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
 */

/**
 * @typedef {Object} StreamConsumerResults - the stream consumer results, which are returned when the stream consumer
 * completes successfully
 * @property {Message[]} messages - a list of zero or more successfully extracted message objects
 * @property {Record[]} unusableRecords - a list of zero or more unusable records
 * @property {Task} processing - a task that tracks the state of the processing phase
 * @property {Task|undefined} [finalising] - a task that tracks the state of the finalising phase
 * @property {Message[]|undefined} [savedMessagesTaskTrackingState] - an optional list of zero or more messages that had their task tracking state successfully saved
 * @property {Message[]|undefined} [handledIncompleteMessages] - an optional list of zero or more successfully handled incomplete messages
 * @property {Record[]|undefined} [discardedUnusableRecords] - an optional list of zero or more successfully discarded unusable records
 * @property {Message[]|undefined} [discardedRejectedMessages] - an optional list of zero or more successfully discarded rejected messages
 * @property {Error|undefined} [saveMessagesTaskTrackingStateError] - an optional error with which save messages task tracking state failed
 * @property {Error|undefined} [handleIncompleteMessagesError] - an optional error with which handle incomplete records failed
 * @property {Error|undefined} [discardUnusableRecordsError] - an optional error with which discard unusable records failed
 * @property {Error|undefined} [discardRejectedMessagesError] - an optional error with which discard rejected messages failed
 * @property {boolean|undefined} [partial] - whether these results are partial (i.e. not all available yet) or full results
 * @property {Promise.<Message[]|Error>|undefined} [saveMessagesTaskTrackingStatePromise] - a promise of either a resolved list of zero or more messages that had their task tracking state successfully saved or a rejected error
 * @property {Promise.<Message[]|Error>|undefined} [handleIncompleteMessagesPromise] - a promise of either a resolved list of zero or more successfully handled incomplete records or a rejected error
 * @property {Promise.<Record[]|Error>|undefined} [discardUnusableRecordsPromise] - a promise of either a resolved list of zero or more successfully discarded unusable records or a rejected error
 * @property {Promise.<Message[]|Error>|undefined} [discardRejectedMessagesPromise] - a promise of either a resolved list of zero or more successfully discarded rejected messages or a rejected error
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

/**
 * @typedef {Object} Message - represents any kind of message object extracted from an AWS stream event record
 */

/**
 * @typedef {Object} Record - represents an AWS stream event record
 */