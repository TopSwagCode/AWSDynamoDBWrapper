using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using Expression = Amazon.DynamoDBv2.DocumentModel.Expression;
using System.Threading;

namespace AWSDynamoDBFramework
{
    public class AWSDynamoDBStream
    {
        public string LatestSequenceNumber { get; set; }
        public string StreamArn { get; set; }
        public string TableName { get; set; }
        public string LatestShardID { get; set; }
        public AWSDynamoDBIteratorType AWSDynamoDBIteratorType { get; set; }
        public AmazonDynamoDBStreamsClient AmazonDynamoDBStreamsClient { get; set; }

        public AWSDynamoDBStream(BasicAWSCredentials basicAWSCredentials, RegionEndpoint regionEndpoint, string tableName, AWSDynamoDBIteratorType type)
        {
            this.TableName = tableName;
            this.AmazonDynamoDBStreamsClient = new AmazonDynamoDBStreamsClient(basicAWSCredentials, regionEndpoint);
            this.AWSDynamoDBIteratorType = type;
            var listStreams = AmazonDynamoDBStreamsClient.ListStreams(new ListStreamsRequest()
            {
                TableName = this.TableName
            });

            this.StreamArn = listStreams.Streams.First().StreamArn;

            DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest()
            {
                StreamArn = this.StreamArn
            };

            var describeStreamResponse = this.AmazonDynamoDBStreamsClient.DescribeStream(describeStreamRequest);
            var shards = describeStreamResponse.StreamDescription.Shards;

            GetShardIteratorRequest getShardIteratorRequest = null;
            if(this.AWSDynamoDBIteratorType == AWSDynamoDBIteratorType.TRIM_HORIZON)
            {
                getShardIteratorRequest = new GetShardIteratorRequest()
                {
                    StreamArn = this.StreamArn,
                    ShardIteratorType = ShardIteratorType.TRIM_HORIZON,
                    ShardId = shards.First().ShardId,
                };
            }
            if(this.AWSDynamoDBIteratorType == AWSDynamoDBIteratorType.LATEST)
            {
                getShardIteratorRequest = new GetShardIteratorRequest()
                {
                    StreamArn = this.StreamArn,
                    ShardIteratorType = ShardIteratorType.LATEST,
                    ShardId = shards.Last().ShardId,
                    //SequenceNumber = shards.First().SequenceNumberRange.StartingSequenceNumber
                };
            }

            var shardIteratorResponse = this.AmazonDynamoDBStreamsClient.GetShardIterator(getShardIteratorRequest);

            this.LatestShardID = shardIteratorResponse.ShardIterator;

        }

        public List<Record> GetRecords()
        {
            var getRecordsRequest = new GetRecordsRequest()
            {
                ShardIterator = this.LatestShardID
            };

            var recordsResponse = this.AmazonDynamoDBStreamsClient.GetRecords(getRecordsRequest);
            LatestShardID = recordsResponse.NextShardIterator;
            var records = recordsResponse.Records;

            return records;
        }

        private T dictionaryToObject<T>(Dictionary<string,AttributeValue> dictionary)
        {
            var document = Document.FromAttributeMap(dictionary);
            return AWSDocumentConverter.ToObject<T>(document);
        }

        public List<AWSEventRecord<T>> GetRecords<T>()
        {
            List<AWSEventRecord<T>> eventRecords = new List<AWSEventRecord<T>>();
            var records = GetRecords();
            foreach (var record in records)
            {
                AWSEventRecord<T> eventRecord = new AWSEventRecord<T>()
                {
                    EventID = record.EventID,
                    EventName = record.EventName,
                    EventSource = record.EventSource,
                    EventVersion = record.EventVersion,
                    SequenceNumber = record.Dynamodb.SequenceNumber,
                    OldImage = dictionaryToObject<T>(record.Dynamodb.OldImage),
                    NewImage = dictionaryToObject<T>(record.Dynamodb.NewImage),
                };

                eventRecords.Add(eventRecord);
            }

            return eventRecords;
        }
    }

    public enum AWSDynamoDBIteratorType
    {
        TRIM_HORIZON,
        //AFTER_SEQUENCE_NUMBER, Not implemented
        LATEST
        //AT_SEQUENCE_NUMBER Not implemented
    }
}
