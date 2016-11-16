'use strict';

/**
 * Generates samples of various AWS artifacts for testing.
 * @author Byron du Preez
 */

const uuid = require('node-uuid');
const base64 = require('core-functions/base64');

const sampleAwsAccountId = "XXXXXXXXXXXX";
const sampleIdentityArn = 'DUMMY_IDENTITY_ARN';

const sampleFunctionName = "testFunc";
const latestFunctionVersion = "$LATEST";


let nextSequenceNumber = 1;

const sampleMessage = {
  key1: 'value1',
  key2: 'value2',
  key3: 'value3'
};

module.exports = {
  sampleAwsAccountId: sampleAwsAccountId,
  sampleIdentityArn: sampleIdentityArn,
  sampleFunctionName: sampleFunctionName,
  sampleMessage: sampleMessage,
  latestFunctionVersion: latestFunctionVersion,

  // General
  sampleNumberString: sampleNumberString,

  // For AWS contexts
  sampleInvokedFunctionArn: sampleInvokedFunctionArn,
  sampleAwsContext: sampleAwsContext,

  // For Kinesis events
  sampleStreamName: sampleStreamName,
  sampleEventSourceArn: sampleEventSourceArn,
  sampleEventSourceArnFromPrefixSuffix: sampleEventSourceArnFromPrefixSuffix,
  sampleBase64Data: sampleBase64Data,
  sampleKinesisRecord: sampleKinesisRecord,
  sampleKinesisEventWithSampleRecord: sampleKinesisEventWithSampleRecord,
  sampleKinesisEventWithRecord: sampleKinesisEventWithRecord,
  sampleKinesisEventWithRecords: sampleKinesisEventWithRecords
};

const Strings = require('core-functions/strings');
//const isBlank = Strings.isBlank;
const isNotBlank = Strings.isNotBlank;
//const trim = Strings.trim;

function sampleNumberString(digits) {
  let number = "";
  for (let i = 0; i < digits; ++i) {
    number += Math.floor((Math.random() * 10)); // 0 to 9
  }
  return number;
}

function sampleStreamName(streamNamePrefix, streamNameSuffix) {
  const prefix = isNotBlank(streamNamePrefix) ? streamNamePrefix : 'TestKinesisStream';
  const suffix = isNotBlank(streamNameSuffix) ? streamNameSuffix : '';
  return `${prefix}${suffix}`;
}

function sampleInvokedFunctionArn(invokedFunctionArnRegion, functionName, functionAlias) {
  const region = isNotBlank(invokedFunctionArnRegion) ? invokedFunctionArnRegion : 'IF_ARN_REGION';
  const funcName = isNotBlank(functionName) ? functionName : sampleFunctionName;
  const aliasSuffix = isNotBlank(functionAlias) ? `:${functionAlias}` : '';
  return `arn:aws:lambda:${region}:${sampleAwsAccountId}:function:${funcName}${aliasSuffix}`
}

function sampleEventSourceArn(eventSourceArnRegion, streamName) {
  const region = isNotBlank(eventSourceArnRegion) ? eventSourceArnRegion : 'EF_ARN_REGION';
  const streamName1 = isNotBlank(streamName) ? streamName : sampleStreamName();
  return `arn:aws:kinesis:${region}:${sampleAwsAccountId}:stream/${streamName1}`;
}

function sampleEventSourceArnFromPrefixSuffix(eventSourceArnRegion, streamNamePrefix, streamNameSuffix) {
  const streamName = sampleStreamName(streamNamePrefix, streamNameSuffix);
  return sampleEventSourceArn(eventSourceArnRegion, streamName);
}

function sampleAwsContext(functionName, functionVersion, invokedFunctionArn, maxTimeInMillis) {
  const uuid1 = uuid.v4();
  const startTime = Date.now();
  const maximumTimeInMillis = maxTimeInMillis ? maxTimeInMillis : 1000;
  return {
    callbackWaitsForEmptyEventLoop: true,
    logGroupName: `/aws/lambda/${functionName}`,
    logStreamName: `2016/10/14/[$LATEST]${uuid1.replace(/-/g, "")}`,
    functionName: functionName,
    memoryLimitInMB: 128,
    functionVersion: functionVersion,
    invokeid: uuid1,
    awsRequestId: uuid1,
    invokedFunctionArn: invokedFunctionArn,
    getRemainingTimeInMillis() {
      return maximumTimeInMillis - (Date.now() - startTime);
    }
  };
}

function sampleBase64Data(obj) {
  return new Buffer(JSON.stringify(obj), 'base64');
}

function sampleKinesisRecord(partitionKey, data, eventSourceArn, eventAwsRegion) {
  // Data "SGVsbG8sIHRoaXMgaXMgYSB0ZXN0IDEyMy4=" is a harmless message ('Hello, this is a test 123.')
  // var c = new Buffer('SGVsbG8sIHRoaXMgaXMgYSB0ZXN0IDEyMy4=', 'base64').toString('utf-8');
  // var d = new Buffer('Hello, this is a test 123.', 'utf-8').toString('base64');
  const shardId = sampleNumberString(56);
  const kinesisPartitionKey = isNotBlank(partitionKey) ? partitionKey : uuid.v4();
  const kinesisData = data !== undefined ?
    typeof data === 'object' ? base64.toBase64(data) : data : "SGVsbG8sIHRoaXMgaXMgYSB0ZXN0IDEyMy4=";
  const sequenceNumber = nextSequenceNumber++; //sampleNumberString(56);
  const awsRegion = eventAwsRegion ? eventAwsRegion : 'EVENT_AWS_REGION';
  const event = {
    eventID: `shardId-000000000000:${shardId}`,
    eventVersion: "1.0",
    kinesis: {
      partitionKey: kinesisPartitionKey,
      data: kinesisData,
      kinesisSchemaVersion: "1.0",
      sequenceNumber: `${sequenceNumber}`
    },
    invokeIdentityArn: sampleIdentityArn,
    eventName: "aws:kinesis:record",
    //eventSourceARN: eventSourceArn,
    eventSource: "aws:kinesis",
    awsRegion: awsRegion
  };
  if (eventSourceArn !== undefined) {
    event.eventSourceARN = eventSourceArn;
  }
  return event;
}

function sampleKinesisEventWithSampleRecord(partitionKey, data, eventSourceArn, eventAwsRegion) {
  return sampleKinesisEventWithRecord(sampleKinesisRecord(partitionKey, data, eventSourceArn, eventAwsRegion))
}

function sampleKinesisEventWithRecord(kinesisRecord) {
  return {
    Records: [
      kinesisRecord
    ]
  };
}

function sampleKinesisEventWithRecords(kinesisRecords) {
  return {
    Records: kinesisRecords
  };
}
