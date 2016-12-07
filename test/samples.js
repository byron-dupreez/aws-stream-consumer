'use strict';

/**
 * Generates samples of various AWS artifacts for testing.
 * @author Byron du Preez
 */

const uuid = require('uuid');
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
  sampleKinesisEventSourceArn: sampleKinesisEventSourceArn,
  sampleKinesisEventSourceArnFromPrefixSuffix: sampleKinesisEventSourceArnFromPrefixSuffix,
  sampleBase64Data: sampleBase64Data,
  sampleKinesisRecord: sampleKinesisRecord,
  sampleKinesisEventWithSampleRecord: sampleKinesisEventWithSampleRecord,
  sampleKinesisEventWithRecord: sampleKinesisEventWithRecord,
  sampleKinesisEventWithRecords: sampleKinesisEventWithRecords,

  awsKinesisStreamsSampleEvent: awsKinesisStreamsSampleEvent,

  // For DynamoDB stream events
  sampleTableName: sampleTableName,
  sampleDynamoDBEventSourceArn: sampleDynamoDBEventSourceArn,
  sampleDynamoDBEventSourceArnFromPrefixSuffix: sampleDynamoDBEventSourceArnFromPrefixSuffix,

  awsDynamoDBUpdateSampleEvent: awsDynamoDBUpdateSampleEvent
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

function sampleTableName(tableNamePrefix, tableNameSuffix) {
  const prefix = isNotBlank(tableNamePrefix) ? tableNamePrefix : 'TestDynamoDBTable';
  const suffix = isNotBlank(tableNameSuffix) ? tableNameSuffix : '';
  return `${prefix}${suffix}`;
}

function sampleInvokedFunctionArn(invokedFunctionArnRegion, functionName, functionAlias) {
  const region = isNotBlank(invokedFunctionArnRegion) ? invokedFunctionArnRegion : 'IF_ARN_REGION';
  const funcName = isNotBlank(functionName) ? functionName : sampleFunctionName;
  const aliasSuffix = isNotBlank(functionAlias) ? `:${functionAlias}` : '';
  return `arn:aws:lambda:${region}:${sampleAwsAccountId}:function:${funcName}${aliasSuffix}`
}

function sampleKinesisEventSourceArn(eventSourceArnRegion, streamName) {
  const region = isNotBlank(eventSourceArnRegion) ? eventSourceArnRegion : 'EF_ARN_REGION';
  const streamName1 = isNotBlank(streamName) ? streamName : sampleStreamName();
  return `arn:aws:kinesis:${region}:${sampleAwsAccountId}:stream/${streamName1}`;
}

function sampleDynamoDBEventSourceArn(eventSourceArnRegion, tableName, timestamp) {
  const region = isNotBlank(eventSourceArnRegion) ? eventSourceArnRegion : 'EF_ARN_REGION';
  const tableName1 = isNotBlank(tableName) ? tableName : sampleTableName();
  const timestamp0 = isNotBlank(timestamp) ? timestamp : new Date().toISOString();
  const timestamp1 = timestamp0.endsWith('Z') ? timestamp0.substring(0, timestamp0.length - 1) : timestamp0;
  //arn:aws:dynamodb:us-east-1:111111111111:table/test/stream/2020-10-10T08:18:22.385
  return `arn:aws:dynamodb:${region}:${sampleAwsAccountId}:table/${tableName1}/stream/${timestamp1}`;
}

function sampleKinesisEventSourceArnFromPrefixSuffix(eventSourceArnRegion, streamNamePrefix, streamNameSuffix) {
  const streamName = sampleStreamName(streamNamePrefix, streamNameSuffix);
  return sampleKinesisEventSourceArn(eventSourceArnRegion, streamName);
}

function sampleDynamoDBEventSourceArnFromPrefixSuffix(eventSourceArnRegion, tableNamePrefix, tableNameSuffix, timestamp) {
  const tableName = sampleTableName(tableNamePrefix, tableNameSuffix);
  return sampleDynamoDBEventSourceArn(eventSourceArnRegion, tableName, timestamp);
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

function awsKinesisStreamsSampleEvent(identityArn, eventSourceArn) {
  return {
    "Records": [
      {
        "eventID": "shardId-000000000000:49545115243490985018280067714973144582180062593244200961",
        "eventVersion": "1.0",
        "kinesis": {
          "partitionKey": "partitionKey-3",
          "data": "SGVsbG8sIHRoaXMgaXMgYSB0ZXN0IDEyMy4=",
          "kinesisSchemaVersion": "1.0",
          "sequenceNumber": "49545115243490985018280067714973144582180062593244200961"
        },
        "invokeIdentityArn": identityArn,
        "eventName": "aws:kinesis:record",
        "eventSourceARN": eventSourceArn,
        "eventSource": "aws:kinesis",
        "awsRegion": "us-east-1"
      }
    ]
  };
}

function awsDynamoDBUpdateSampleEvent(eventSourceArn) {
  return {
    "Records": [
      {
        "eventID": "1",
        "eventVersion": "1.0",
        "dynamodb": {
          "Keys": {
            "Id": {
              "N": "101"
            }
          },
          "NewImage": {
            "Message": {
              "S": "New item!"
            },
            "Id": {
              "N": "101"
            }
          },
          "StreamViewType": "NEW_AND_OLD_IMAGES",
          "SequenceNumber": "111",
          "SizeBytes": 26
        },
        "awsRegion": "us-west-2",
        "eventName": "INSERT",
        "eventSourceARN": eventSourceArn,
        "eventSource": "aws:dynamodb"
      },
      {
        "eventID": "2",
        "eventVersion": "1.0",
        "dynamodb": {
          "OldImage": {
            "Message": {
              "S": "New item!"
            },
            "Id": {
              "N": "101"
            }
          },
          "SequenceNumber": "222",
          "Keys": {
            "Id": {
              "N": "101"
            }
          },
          "SizeBytes": 59,
          "NewImage": {
            "Message": {
              "S": "This item has changed"
            },
            "Id": {
              "N": "101"
            }
          },
          "StreamViewType": "NEW_AND_OLD_IMAGES"
        },
        "awsRegion": "us-west-2",
        "eventName": "MODIFY",
        "eventSourceARN": eventSourceArn,
        "eventSource": "aws:dynamodb"
      },
      {
        "eventID": "3",
        "eventVersion": "1.0",
        "dynamodb": {
          "Keys": {
            "Id": {
              "N": "101"
            }
          },
          "SizeBytes": 38,
          "SequenceNumber": "333",
          "OldImage": {
            "Message": {
              "S": "This item has changed"
            },
            "Id": {
              "N": "101"
            }
          },
          "StreamViewType": "NEW_AND_OLD_IMAGES"
        },
        "awsRegion": "us-west-2",
        "eventName": "REMOVE",
        "eventSourceARN": eventSourceArn,
        "eventSource": "aws:dynamodb"
      }
    ]
  };
}

