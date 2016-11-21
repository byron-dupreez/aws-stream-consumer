# aws-stream-consumer v1.0.0-beta.3

Utilities for building robust AWS Lambda consumers of stream events from Amazon Web Services (AWS) Kinesis or DynamoDB streams.

## Modules:
- `stream-consumer.js` module
  - Utilities and functions to be used to robustly consume messages from an AWS Kinesis or DynamoDB stream event
- `stream-consumer-config.js` module 
  - Utilities for configuring and accessing the runtime settings of a Kinesis or DynamoDB stream consumer from a given AWS event and AWS context
- `stream-processing.js` module 
  - Utilities for configuring stream processing, which configures and determines the processing behaviour of a stream consumer

## Purpose

The goal of the AWS stream consumer functions is to make the process of consuming records from an AWS Kinesis or DynamoDB 
stream more robust for an AWS Lambda stream consumer by providing solutions to and workarounds for common AWS stream 
consumption issues. 

#### Common AWS stream consumption issues
1. The fundamental issue is that either all of a stream event's records must be processed successfully or an error must 
   be thrown back to AWS Lambda to trigger a replay of all of the event's records again (assuming that you don't want to
   lose any of the records). This course-grained error handling makes no distinction between persistent and transient 
   errors and does not provide a way to only reprocess unsuccessful records.
    
2. The fact that AWS stream event records should always be processed in batches from the AWS stream (to increase 
   throughput and reduce the risk of slow consumption ultimately leading to message loss), both increases the complexity
   and the chance of failures. For example, while processing a batch of 100 messages, if processing fails on only 1 
   message with a transient error, then ideally we would want to only replay that 1 failed message, but the only replay
   option is to throw an error that will trigger a replay of the entire batch of messages.
   
3. Any persistent error encountered, which is unhandled, is fatal, because any record that cannot be processed due to a 
   persistent error will block the shard from which it came (and all the records behind it), since the stream will 
   continuously redeliver this record until it expires 24 hours to 7 days later (depending on your stream retention 
   configuration). At expiry, the record will be lost and the records behind it with similar ages are also at risk of 
   being lost.
      
4. A "poisonous" record that always causes an error to be thrown back to AWS Lambda when an attempt is made to parse it 
   into a message, will block the shard from which it came until it expires.
   
5. A successfully parsed, but still invalid message that can NEVER be successfully processed also blocks its shard until
   it expires.
   
6. Tasks/functions, which are executed on a message or batch of messages, that fail "indefinitely" will similarly block 
   the shard from which the message(s) originated.
   
7. Each AWS Lambda invocation has a configurable, but limited number of seconds that it is allowed to run and if a batch 
   of messages cannot be fully processed within that time, then the invocation will be timed out and an error will be 
   thrown back to AWS Lambda, which will cause the same batch of messages to be replayed again and, in the worst case
   scenario, continue to time out and replay indefinitely until the batch of messages expires.
   
#### Solutions to and workarounds for the above issues provided by aws-stream-consumer:
1. Any and all errors encountered during processing of a record or its extracted message are caught, logged and handled 
   either by "discarding" the unusable record or by tracking them as failed task states on each message. A task tracking 
   object is attached to each message to keep track of the state of each and every task (i.e. custom execute/processing 
   function) applied to a message. The stream consumer attempts to persist this task tracking information by resubmitting 
   incomplete messages with this information back to their shard instead of throwing these errors back to AWS Lambda. 
   This enables more fine-grained error handling and reprocessing of only incomplete messages.
   
2. Each message has its own task tracking information, so whether or not a single message or a batch of messages is 
   being consumed makes no difference. The information enables the stream consumer to determine which messages are 
   completely processed and which messages are still incomplete and then only replay incomplete messages by resubmitting
   them back to their shard.
   
3. Persistent errors can be dealt with by preferably explicitly rejecting a failing task, which marks it as 'Rejected',
   within the task's custom execute function, which is the responsibility of the developer of the custom task execute 
   function, or along with transient errors by "discarding" a message when all of its failing tasks have reached the 
   maximum number of allowed attempts.
   
4. Any error thrown during the extraction of a message from an event record, will be caught and logged and the record
   will be then treated as an "unusable" record. Any such unusable record will be "discarded" by passing it to the 
   configurable `discardUnusableRecords` function to be dealt with. The default `discardUnusableRecordsToDRQ` function, 
   routes these unusable records to a Kinesis "Dead Record Queue (DRQ)" stream.
   
5. Invalid messages that can never be successfully processed should ideally be identified and their failing task(s) 
   should be rejected, which marks them as 'Rejected', within the custom task execute function. If this is not done, 
   then invalid messages will be indistinguishable from valid messages that could not be successfully processed within 
   the allowed number of attempts.
   
6. Task tracking includes tracking the number of attempts at each task on each message, which enables the stream 
   consumer to "discard" a message when all of its failing tasks have reached the maximum number of allowed attempts by
   discarding these tasks, which marks them as 'Discarded', and then passing the message to the configurable 
   `discardRejectedMessages` function to be dealt with. The default `discardRejectedMessagesToDMQ` function, routes 
   these rejected messages to a Kinesis "Dead Message Queue (DMQ)" stream. 
   
7. The stream consumer attempts to deal with the issue of AWS Lambda time outs by setting up its own time out at a 
   configurable percentage of the remaining time that the AWS Lambda invocation has to execute. This time out races 
   against the completion of all processing tasks on all of the messages in the batch. If the time out triggers before
   processing has completed, the stream consumer finalises message processing prematurely with the current state of the 
   messages' tasks with the view that its better to preserve at least some of the task tracking information on each 
   message than none. The stream consumer finalises message processing in both the time out case and the successful 
   processing completion case by freezing all of the messages' tasks, which prevents subsequent updates by any still in 
   progress tasks in the time out case, by ensuring that the discarding of any unusable records has completed, by 
   resubmitting any incomplete messages back to their shard and by discarding any finalised message that contains a task 
   that was rejected (explicitly by custom task execute functions), discarded (due to exceeded attempts) or abandoned 
   (if code changes make previous task definitions obsolete). If the stream consumer is unable to finalise message 
   processing due to an error, then it is unfortunately left with no choice, but to throw the error back to AWS Lambda 
   to trigger a replay of the entire batch of records to prevent message loss. These errors need to be monitored.
   
## Current limitations
- The default configuration currently supports consuming AWS Kinesis stream events. 
- While the current stream consumer code allows for customisation of stream processing behaviour to support AWS DynamoDB 
  stream events, there is currently no out-of-the-box default configuration for supporting AWS DynamoDB stream events.
- The AWS stream consumer functions focus on ensuring "at least once" message delivery semantics, so currently there is 
  no support planned for "at most once" message delivery semantics.
- The message resubmission strategy attempts to preserve some semblance of the original sequence by resubmitting messages 
  using the Kinesis SequenceNumberForOrdering parameter set to the source record's sequence number. However, this does 
  not guarantee that the original sequence will be preserved, so if message sequence is vital you will need to cater for
  this separately.

## Installation
This module is exported as a [Node.js](https://nodejs.org/) module.

Using npm:
```bash
$ {sudo -H} npm i -g npm
$ npm i --save aws-stream-consumer
```

## Usage 

To use the `aws-stream-consumer` module:

1. Configure the stream consumer 
```js
// Create a context object
const context = {}; // ... or your own context object

const settings = undefined; // ... or your own settings for custom configuration of any or all logging, stage handling and/or stream processing settings
const options = require('./config-kinesis.json'); // ... or your own options for custom configuration of any or all logging, stage handling, kinesis and/or stream processing options
const forceConfiguration = false;

// Configure logging
const logging = require('logging-utils');
const loggingSettings = logging.getDefaultLoggingSettings(options.loggingOptions);
logging.configureLogging(context, loggingSettings, forceConfiguration); 
// ... or a slightly shorter version, similar to the above 2 lines
logging.configureDefaultLogging(context, options.loggingOptions, undefined, forceConfiguration);

// Configure stage-handling, which determines the behaviour of the stage handling functions
const stages = require('aws-core-utils/stages');
// ... EITHER using the default stage handling configuration
stages.configureDefaultStageHandling(context, options.stageHandlingOptions, settings, options, forceConfiguration); 
// ... OR using your own custom stage-handling configuration
const stageHandlingSettings = stages.getDefaultStageHandlingSettings(options.stageHandlingOptions);
// Optionally override the default stage handling functions with your own custom functions
// stageHandlingSettings.customToStage = undefined;
// stageHandlingSettings.convertAliasToStage = stages.DEFAULTS.convertAliasToStage;
// stageHandlingSettings.injectStageIntoStreamName = stages.DEFAULTS.toStageSuffixedStreamName;
// stageHandlingSettings.extractStageFromStreamName = stages.DEFAULTS.extractStageFromSuffixedStreamName;
// stageHandlingSettings.injectStageIntoResourceName = stages.DEFAULTS.toStageSuffixedResourceName;
// stageHandlingSettings.extractStageFromResourceName = stages.DEFAULTS.extractStageFromSuffixedResourceName;
stages.configureStageHandling(context, stageHandlingSettings, settings, options, forceConfiguration);

// Configure a default Kinesis instance (if you are using the default stream processing configuration or you are using Kinesis)
const kinesisUtils = require('aws-core-utils/kinesis-utils');
// Only specify a region in the kinesisOptions if you do NOT want to use your AWS Lambda's current region
kinesisUtils.configureKinesis(context, options.kinesisOptions);

// Configure stream processing
const streamProcessing = require('aws-stream-consumer/stream-processing');

// ... EITHER using the default Kinesis stream processing configuration 
streamProcessing.configureDefaultKinesisStreamProcessing(context, options.streamProcessingOptions, settings, options, forceConfiguration); 
// ... OR using your own custom stream processing configuration
const streamProcessingSettings = streamProcessing.getDefaultKinesisStreamProcessingSettings(options.streamProcessingOptions);
// Optionally customize the default stream processing functions
// streamProcessingSettings.extractMessageFromRecord = streamProcessing.DEFAULTS.extractJsonMessageFromKinesisRecord;
// streamProcessingSettings.discardUnusableRecords = streamProcessing.DEFAULTS.discardUnusableRecordsToDRQ;
// streamProcessingSettings.resubmitIncompleteMessages = streamProcessing.DEFAULTS.resubmitIncompleteMessagesToKinesis();
// streamProcessingSettings.discardRejectedMessages = streamProcessing.DEFAULTS.discardRejectedMessagesToDMQ;
streamProcessing.configureStreamProcessing(context, streamProcessingSettings, settings, options, forceConfiguration);

// Configure the stream consumer's runtime settings
const streamConsumerConfig = require('aws-stream-consumer/stream-consumer-config');

streamConsumerConfig.configureStreamConsumer(context, settings, options, awsEvent, awsContext);
```
2. Define the tasks that you want to execute on individual messages and/or on the entire batch of messages
```js
const taskDefs = require('task-utils/task-defs');
const TaskDef = taskDefs.TaskDef;

// Example process (one) individual message task definition
function saveMessageToDynamoDB(message, context) { /* ... */ }
const saveMessageTaskDef = TaskDef.defineTask(saveMessageToDynamoDB.name, saveMessageToDynamoDB);

// Add optional sub-task definition(s) to any of your task definitions as needed
saveMessageTaskDef.defineSubTasks(['sendPushNotification', 'sendEmail']);

// Example process (all) entire batch of messages task definition
function logMessagesToS3(messages, context) { /* ... */ }
const logMessagesToS3TaskDef = TaskDef.defineTask(logMessagesToS3.name, logMessagesToS3); // ... with sub-task definitions if needed
```
3. Process the AWS Kinesis (or DynamoDB) stream event
```js
const processOneTaskDefs = [saveMessageTaskDef]; // ... and/or more task definitions
const processAllTaskDefs = [logMessagesToS3TaskDef]; // ... and/or more task definitions

const streamConsumer = require('aws-stream-consumer/stream-consumer');
const promise = streamConsumer.processStreamEvent(awsEvent, awsContext, processOneTaskDefs, processAllTaskDefs, context);
```
4. Within your custom task execute function(s), update the message's (or messages') tasks' and/or sub-tasks' states
   * Example custom "process one" task execute function for processing a single, individual message at a time
```js
function saveMessageToDynamoDB(message, context) {
  // Note that 'this' will be the currently executing task witin your custom task execute function
  const task = this; 
  const subTask = task.getSubTask('sendPushNotification');
  
  // ... or alternatively from anywhere in the flow of your custom execute code
  const task1 = streamConsumer.getProcessOneTask(message, saveMessageToDynamoDB.name, context);
  const subTask1 = task1.getSubTask('sendPushNotification');
  
  const subTask2 = task1.getSubTask('sendEmail');
  
  // ...
  
  // Change the task's and/or sub-tasks' states based on outcomes, e.g.
  subTask1.succeed(subTask1Result);
  
  // ...
  
  subTask2.reject('Invalid email address', new Error('Invalid email address'), true);
  
  // ...
  
  task.fail(new Error('Task failed'));
    
  // ...
}
```
   * Example custom "process all" task execute function for processing the entire batch of messages
```js
function logMessagesToS3(messages, context) {
  // Note that 'this' will be the currently executing master task witin your custom task execute function
  // NB: Master tasks and sub-tasks will apply any state changes made to them to every message in the batch
  const masterTask = this; 
  const masterSubTask = masterTask.getSubTask('doX');
  
  // ... or alternatively from anywhere in the flow of your custom execute code
  const masterTask1 = streamConsumer.getProcessAllTask(messages, logMessagesToS3.name, context);
  const masterSubTask1 = masterTask1.getSubTask('doX');
  
  const masterSubTask2 = masterTask1.getSubTask('doY');
  
  // ...
  
  // Change the master task's and/or sub-tasks' states based on outcomes, e.g.
  masterSubTask1.succeed(subTask1Result);
  
  // ...
  
  masterSubTask2.reject('Cannot do X', new Error('X is un-doable'), true);
  
  // ...
  
  masterTask.fail(new Error('Task failed'));
    
  // ...
  
  // ALTERNATIVELY (or in addition) change the task state of individual messages
  const firstMessage = messages[0]; // e.g. working with the first message in the batch
  const messageTask1 = streamConsumer.getProcessAllTask(firstMessage, logMessagesToS3.name, context);
  const messageSubTask1 = messageTask1.getSubTask('doX');
  messageSubTask1.reject('Cannot do X on first message', new Error('X is un-doable on first message'), true);
  messageTask1.fail(new Error('Task failed on first message'));
  
  // ...
}
```

## Unit tests
This module's unit tests were developed with and must be run with [tape](https://www.npmjs.com/package/tape). The unit tests have been tested on [Node.js v4.3.2](https://nodejs.org/en/blog/release/v4.3.2/).  

Install tape globally if you want to run multiple tests at once:
```bash
$ npm install tape -g
```

Run all unit tests with:
```bash
$ npm test
```
or with tape:
```bash
$ tape test/*.js
```

See the [package source](https://github.com/byron-dupreez/aws-stream-consumer) for more details.

## Changes

### 1.0.0-beta.4

- Changes to `stream-consumer-config` module:
  - Changed `configureStreamConsumer` function to accept new `settings` and `options` arguments to enable complete 
    configuration of the stream consumer via the arguments
  - Removed `configureLoggingIfNotConfigured` function, which was migrated to `logging-utils/logging.js`
  - Removed `configureDefaultStageHandlingIfNotConfigured` function, which was migrated to `aws-core-utils/stages.js`
  - Removed `configureDefaultKinesisStreamProcessingIfNotConfigured` function, which was migrated to `stream-processing.js`
- Changes to `stream-processing` module:
  - Removed module-scope default variables
  - Added a typedef for `StreamProcessingOptions` to be used in JsDoc for parameters & return values
  - Added new `configureDependenciesIfNotConfigured` function to configure stream processing dependencies (i.e. logging, 
    stage handling & kinesis for now)
  - Added new `configureStreamProcessingIfNotConfigured` function to replace the `stream-consumer-config` module's
    `configureDefaultKinesisStreamProcessingIfNotConfigured` function and to first invoke the new 
    `configureDependenciesIfNotConfigured` function
  - Changed `configureStreamProcessing` function to accept `otherSettings` and `otherOptions` as 3rd & 4th arguments to 
    enable configuration of dependencies and to first invoke invoke new `configureDependenciesIfNotConfigured` function
  - Changed `configureDefaultKinesisStreamProcessing` function to accept `options`, `otherSettings` and `otherOptions` 
    as 2nd, 3rd & 4th arguments to enable customisation of default options and configuration of dependencies, and to 
    always invoke `configureStreamProcessing`
  - Changed `configureKinesisIfNotConfigured` to use local default options from `config-kinesis.json` if no kinesisOptions
    are provided and context.kinesis is not already configured
  - Changed `getDefaultKinesisStreamProcessingSettings` function to accept an explicit stream processing `options` 
    object of type `StreamProcessingOptions` as its sole argument instead of an arbitrary `config` object to enable
    customization of default options
  - Added new `loadDefaultKinesisStreamProcessingOptions` function to load default stream processing options from the 
    local `config-kinesis.json` file
  - Changed `getDefaultKinesisStreamProcessingSettings` function to use new `loadDefaultKinesisStreamProcessingOptions` function
  - Changed `getKinesis` function to use `configureKinesisIfNotConfigured` instead of directly calling 
    `aws-core-utils/kinesis-utils#configureKinesis` to enable use of local default kinesis options

### 1.0.0-beta.3
- Changes to `stream-consumer` module: 
  - Removed unused module-scope region constant.
  - Changed validation of stream event records to do specific validation based on stream type.
- Changes to `stream-processing` module:
  - Renamed `configureDefaultStreamProcessing` function to `configureDefaultKinesisStreamProcessing`.
  - Renamed `getDefaultStreamProcessingSettings` function to `getDefaultKinesisStreamProcessingSettings`.
- Changes to `stream-consumer-config` module:
  - Renamed `configureDefaultStreamProcessingIfNotConfigured` function to `configureDefaultKinesisStreamProcessingIfNotConfigured`.
- Removed unused `computeChecksums` setting from `config.json`.
- Updated `aws-core-utils` dependency to version 2.1.4.
- Updated `README.md` usage and limitations documentation.

### 1.0.0-beta.2
- Changes to `stream-processing` module: 
  - Changed `discardRejectedMessagesToDMQ` function to wrap the original message in a rejected message "envelope" with metadata

### 1.0.0-beta.1
- First beta release - unit tested, but not battle tested 

