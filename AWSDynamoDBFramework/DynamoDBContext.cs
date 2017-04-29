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
    public class DynamoDBContext
    {
        private AmazonDynamoDBClient client;
        private Table dynamoDBTable;
        public AWSDynamoTableConfig AWSDynamoTableConfig { get; set; }
        public AWSDynamoDBTable AWSDynamoDBTable { get; set; }
        private BasicAWSCredentials BasicAWSCredentials { get; set; }
        private RegionEndpoint RegionEndpoint { get; set; }
        private AWSDocumentConverter AWSDocumentConverter { get; set; }

        private DynamoDBContext(RegionEndpoint regionEndpoint, string accessKey, string secretKey, AWSDynamoTableConfig tableConfig)
        {
            this.BasicAWSCredentials = new BasicAWSCredentials(accessKey, secretKey);
            this.RegionEndpoint = regionEndpoint;
            this.client = new AmazonDynamoDBClient( new BasicAWSCredentials(accessKey, secretKey), new AmazonDynamoDBConfig() { RegionEndpoint = regionEndpoint });
            this.AWSDynamoTableConfig = tableConfig;
            this.AWSDocumentConverter = new AWSDocumentConverter();
        }

        private DynamoDBContext(BasicAWSCredentials credentials, RegionEndpoint regionEndpoint, AWSDynamoTableConfig tableConfig)
        {
            this.BasicAWSCredentials = credentials;
            this.RegionEndpoint = regionEndpoint;
            this.client = new AmazonDynamoDBClient(credentials, new AmazonDynamoDBConfig() { RegionEndpoint = regionEndpoint });
            this.AWSDynamoTableConfig = tableConfig;
            this.AWSDocumentConverter = new AWSDocumentConverter();
        }

        public static DynamoDBContext GetDynamoDBContext(RegionEndpoint regionEndpoint, string accessKey, string secretKey, AWSDynamoTableConfig tableConfig)
        {
            DynamoDBContext ddbc = new DynamoDBContext(regionEndpoint, accessKey, secretKey, tableConfig);
            ddbc.CreateTable();
            return ddbc;
        }

        public static DynamoDBContext GetDynamoDBContext(BasicAWSCredentials credentials, RegionEndpoint regionEndpoint, AWSDynamoTableConfig tableConfig)
        {
            DynamoDBContext ddbc = new DynamoDBContext(credentials, regionEndpoint, tableConfig);
            ddbc.CreateTable();
            return ddbc;
        }

        public void CreateTable()
        {
            AWSDynamoDBTable = new AWSDynamoDBTable(client, AWSDynamoTableConfig.TableName, AWSDynamoTableConfig.KeyName, AWSDynamoTableConfig.KeyType, AWSDynamoTableConfig.SortKeyName, AWSDynamoTableConfig.SortKeyType, AWSDynamoTableConfig.ReadCapacityUnits, AWSDynamoTableConfig.WriteCapacityUnits, AWSDynamoTableConfig.StreamEnabled);
            
            var tableExists = AWSDynamoDBTable.TableExists();
            if (!tableExists)
                AWSDynamoDBTable.ExecuteCreateTable();

            dynamoDBTable = Table.LoadTable(client, AWSDynamoTableConfig.TableName);
        }

        public async static Task<DynamoDBContext> GetDynamoDBContextAsync(RegionEndpoint regionEndpoint, string accessKey, string secretKey, AWSDynamoTableConfig tableConfig)
        {
            DynamoDBContext ddbc = new DynamoDBContext(regionEndpoint, accessKey, secretKey, tableConfig);
            await ddbc.CreateTableAsync();
            return ddbc;
        }

        public async static Task<DynamoDBContext> GetDynamoDBContextAsync(BasicAWSCredentials credentials, RegionEndpoint regionEndpoint, AWSDynamoTableConfig tableConfig)
        {
            DynamoDBContext ddbc = new DynamoDBContext(credentials, regionEndpoint, tableConfig);
            await ddbc.CreateTableAsync();
            return ddbc;
        }

        private async Task CreateTableAsync()
        {
            AWSDynamoDBTable = new AWSDynamoDBTable(client, AWSDynamoTableConfig.TableName, AWSDynamoTableConfig.KeyName, AWSDynamoTableConfig.KeyType, AWSDynamoTableConfig.SortKeyName, AWSDynamoTableConfig.SortKeyType, AWSDynamoTableConfig.ReadCapacityUnits, AWSDynamoTableConfig.WriteCapacityUnits, AWSDynamoTableConfig.StreamEnabled);

            var tableExists = await AWSDynamoDBTable.TableExistsAsync();
            if (!tableExists)
                await AWSDynamoDBTable.ExecuteCreateTableAsync();

            dynamoDBTable = Table.LoadTable(client, AWSDynamoTableConfig.TableName);
        }


        public void Insert<T>(T tObject)
        {
            var document = AWSDocumentConverter.ToDocument(tObject);
            dynamoDBTable.PutItem(document);
        }

        public async Task InsertAsync<T>(T tObject)
        {
            var document = AWSDocumentConverter.ToDocument(tObject);
            await dynamoDBTable.PutItemAsync(document);
        }

        private Document Get(Primitive id, Primitive sortKey = null, bool consistentRead = true, List<string> attributesToGet = null)
        {
            GetItemOperationConfig config = new GetItemOperationConfig();
            if (attributesToGet != null)
                config.AttributesToGet = attributesToGet;

            config.ConsistentRead = consistentRead;
            
            if(sortKey != null)
                return dynamoDBTable.GetItem(id, sortKey, config);
            else
                return dynamoDBTable.GetItem(id, config);
        }

        private async Task<Document> GetAsync(Primitive id, Primitive sortKey = null, bool consistentRead = true, List<string> attributesToGet = null)
        {
            GetItemOperationConfig config = new GetItemOperationConfig();
            if (attributesToGet != null)
                config.AttributesToGet = attributesToGet;
            config.ConsistentRead = consistentRead;

            return await dynamoDBTable.GetItemAsync(id, config);
        }

        public T Get<T>(Primitive id, Primitive sortKey = null, bool consistentRead = true, List<string> attributesToGet = null)
        {
            var document = Get(id, sortKey, consistentRead, attributesToGet);
            return AWSDocumentConverter.ToObject<T>(document);
        }

        public async Task<T> GetAsync<T>(Primitive id, Primitive sortKey = null, bool consistentRead = true, List<string> attributesToGet = null)
        {
            var document = await GetAsync(id, sortKey, consistentRead, attributesToGet);
            return AWSDocumentConverter.ToObject<T>(document);
        }

        public void DeleteDocument(Primitive id)
        {
            dynamoDBTable.DeleteItem(id);
        }

        public async Task DeleteDocumentAsync(Primitive id)
        {
           await dynamoDBTable.DeleteItemAsync(id);
        }


        public Document PartialUpdateCommand<T>(T tObject, ReturnValues returnValues = ReturnValues.AllNewAttributes)
        {
            var document = AWSDocumentConverter.ToDocument(tObject);

            UpdateItemOperationConfig config = new UpdateItemOperationConfig
            {
                ReturnValues = returnValues
            };

            return dynamoDBTable.UpdateItem(document, config);
        }

        public async Task<Document> PartialUpdateCommandAsync<T>(T tObject, ReturnValues returnValues = ReturnValues.AllNewAttributes)
        {
            var document = AWSDocumentConverter.ToDocument(tObject);

            UpdateItemOperationConfig config = new UpdateItemOperationConfig
            {
                ReturnValues = returnValues
            };

            return await dynamoDBTable.UpdateItemAsync(document, config);
        }

        public void BatchInsert<T>(List<T> batchOfObjects)
        {
            var batchWrite = dynamoDBTable.CreateBatchWrite();

            foreach (var tObject in batchOfObjects)
            {
                var document = AWSDocumentConverter.ToDocument(tObject);
                batchWrite.AddDocumentToPut(document);
            }
            batchWrite.Execute();
        }

        public async Task BatchInsertAsync<T>(List<T> batchOfObjects)
        {
            var batchWrite = dynamoDBTable.CreateBatchWrite();

            foreach (var tObject in batchOfObjects)
            {
                var document = AWSDocumentConverter.ToDocument(tObject);
                batchWrite.AddDocumentToPut(document);
            }
            await batchWrite.ExecuteAsync();
        }

        public void BatchDelete(List<Primitive> idList, List<Primitive> sortKeyList = null)
        {
            if (sortKeyList != null && idList.Count != sortKeyList.Count)
                throw new ArgumentException("sortKeyList not same length as idList");

            var batchWrite = dynamoDBTable.CreateBatchWrite();

            if (sortKeyList != null)
            {
                for (int i = 0; i < idList.Count; i++)
                {
                    batchWrite.AddKeyToDelete(idList[i], sortKeyList[i]);
                }
            }
            else
            {
                for (int i = 0; i < idList.Count; i++)
                {
                    batchWrite.AddKeyToDelete(idList[i]);
                }
            }

            batchWrite.Execute();
        }

        public async Task BatchDeleteAsync(List<Primitive> idList, List<Primitive> sortKeyList = null)
        {
            if (sortKeyList != null && idList.Count != sortKeyList.Count)
                throw new ArgumentException("sortKeyList not same length as idList");

            var batchWrite = dynamoDBTable.CreateBatchWrite();

            if(sortKeyList != null)
            {
                for (int i = 0; i < idList.Count; i++)
                {
                    batchWrite.AddKeyToDelete(idList[i], sortKeyList[i]);
                }
            }
            else
            {
                for (int i = 0; i < idList.Count; i++)
                {
                    batchWrite.AddKeyToDelete(idList[i]);
                }
            }
            

            await batchWrite.ExecuteAsync();
        }

        private List<Document> BatchGet(List<Primitive> idList, List<Primitive> sortKeyList = null, bool consistentRead = true, List<string> attributesToGet = null)
        {
            if (sortKeyList != null && idList.Count != sortKeyList.Count)
                throw new ArgumentException("sortKeyList not same length as idList");

            var batchGet = dynamoDBTable.CreateBatchGet();

            batchGet.ConsistentRead = consistentRead;
            if (attributesToGet != null)
                batchGet.AttributesToGet = attributesToGet;

            if (sortKeyList != null)
            {
                for (int i = 0; i < idList.Count; i++)
                {
                    batchGet.AddKey(idList[i], sortKeyList[i]);
                }
            }
            else
            {
                for (int i = 0; i < idList.Count; i++)
                {
                    batchGet.AddKey(idList[i]);
                }
            }

            batchGet.Execute();

            return batchGet.Results;
        }

        private async Task<List<Document>> BatchGetAsync(List<Primitive> idList, List<Primitive> sortKeyList = null, bool consistentRead = true, List<string> attributesToGet = null)
        {
            if (sortKeyList != null && idList.Count != sortKeyList.Count)
                throw new ArgumentException("sortKeyList not same length as idList");

            var batchGet = dynamoDBTable.CreateBatchGet();

            batchGet.ConsistentRead = consistentRead;
            if (attributesToGet != null)
                batchGet.AttributesToGet = attributesToGet;

            if (sortKeyList != null)
            {
                for (int i = 0; i < idList.Count; i++)
                {
                    batchGet.AddKey(idList[i], sortKeyList[i]);
                }
            }
            else
            {
                for (int i = 0; i < idList.Count; i++)
                {
                    batchGet.AddKey(idList[i]);
                }
            }

            await batchGet.ExecuteAsync();

            return batchGet.Results;
        }

        public List<T> BatchGet<T>(List<Primitive> idList, List<Primitive> sortKeyList = null, bool consistentRead = true, List<string> attributesToGet = null)
            where T : new()
        {
            List<T> tList = new List<T>();
            var documents = BatchGet(idList, sortKeyList, consistentRead, attributesToGet);

            foreach (var document in documents)
            {
                T t = new T();
                t = (T)AWSDocumentConverter.ToObject<T>(document);
                tList.Add(t);
            }

            return tList;
        }

        public async Task<List<T>> BatchGetAsync<T>(List<Primitive> idList, List<Primitive> sortKeyList = null, bool consistentRead = true, List<string> attributesToGet = null)
            where T : new()
        {
            List<T> tList = new List<T>();
            var documents = await BatchGetAsync(idList, sortKeyList, consistentRead, attributesToGet);

            foreach (var document in documents)
            {
                T t = new T();
                t = AWSDocumentConverter.ToObject<T>(document);
                tList.Add(t);
            }

            return tList;
        }

        private IEnumerable<Document> Scan(List<ScanFilterCondition> scanFilterConditions, bool consistentRead = true, List<string> attributesToGet = null )
        {
            ScanOperationConfig scanOperationConfig = GetScanOperationConfig(scanFilterConditions, consistentRead, attributesToGet);

            Search search = dynamoDBTable.Scan(scanOperationConfig);
            
            List<Document> documentList = new List<Document>();
            do
            {
                documentList = search.GetNextSet();

                foreach (var document in documentList)
                    yield return document;
            } while (!search.IsDone);
        }

        public IEnumerable<T> Scan<T>(List<ScanFilterCondition> scanFilterConditions, bool consistentRead = true, List<string> attributesToGet = null)
        {
            foreach (var document in Scan(scanFilterConditions, consistentRead, attributesToGet))
            {
                var result = AWSDocumentConverter.ToObject<T>(document);
                yield return result;
            }
        }

        private IEnumerable<Document> ScanAsyncTest(List<ScanFilterCondition> scanFilterConditions, bool consistentRead = true, List<string> attributesToGet = null)
        {
            ScanOperationConfig scanOperationConfig = GetScanOperationConfig(scanFilterConditions, consistentRead, attributesToGet);

            Search search = dynamoDBTable.Scan(scanOperationConfig);

            List<Document> documentList = new List<Document>();
            do
            {
                documentList = search.GetNextSetAsync().Result;

                foreach (var document in documentList)
                    yield return document;
            } while (!search.IsDone);
        }

        private IEnumerable<T> ScanAsyncTest<T>(List<ScanFilterCondition> scanFilterConditions, bool consistentRead = true, List<string> attributesToGet = null)
        {
            foreach (var document in ScanAsyncTest(scanFilterConditions, consistentRead, attributesToGet))
            {
                var result = AWSDocumentConverter.ToObject<T>(document);
                yield return result;
            }
        }

        private async Task<List<T>> ScanAsync<T>(List<ScanFilterCondition> scanFilterConditions, bool consistentRead = true, List<string> attributesToGet = null)
        {
            ScanOperationConfig scanOperationConfig = GetScanOperationConfig(scanFilterConditions, consistentRead, attributesToGet);

            Search search = dynamoDBTable.Scan(scanOperationConfig);

            List<T> resultList = new List<T>();
            List<Document> documentList = new List<Document>();
            do
            {
                documentList = await search.GetNextSetAsync();
                foreach(var document in documentList)
                {
                    resultList.Add(AWSDocumentConverter.ToObject<T>(document));
                }
           
            } while (!search.IsDone);

            return resultList;
        }

        private ScanOperationConfig GetScanOperationConfig(List<ScanFilterCondition> scanFilterConditions, bool consistentRead = true, List<string> attributesToGet = null)
        {
            ScanFilter scanFilter = new ScanFilter();

            foreach (var scanFilterCondition in scanFilterConditions)
            {
                if (scanFilterCondition.Type == ScanFilterConditionType.one)
                    scanFilter.AddCondition(scanFilterCondition.AttributeName, scanFilterCondition.Condition);
                if (scanFilterCondition.Type == ScanFilterConditionType.two)
                    scanFilter.AddCondition(scanFilterCondition.AttributeName, scanFilterCondition.ScanOperator, scanFilterCondition.AttributeValues);
                if (scanFilterCondition.Type == ScanFilterConditionType.three)
                    scanFilter.AddCondition(scanFilterCondition.AttributeName, scanFilterCondition.ScanOperator, scanFilterCondition.DynamoDBEntry);
            }

            ScanOperationConfig scanOperationConfig = new ScanOperationConfig()
            {
                ConsistentRead = consistentRead,
                Filter = scanFilter
            };
            if (attributesToGet != null)
                scanOperationConfig.AttributesToGet = attributesToGet;

            return scanOperationConfig;
        }

        public async Task<List<T>> QueryAsync<T>(List<QueryFilterCondition> queryFilterConditions, bool consistentRead = true, List<string> attributesToGet = null)
        {
            QueryOperationConfig queryOperationConfig = GetQueryOperationConfig(queryFilterConditions, consistentRead, attributesToGet);

            Search search = dynamoDBTable.Query(queryOperationConfig);

            List<T> resultList = new List<T>();
            List<Document> documentList = new List<Document>();
            do
            {
                documentList = await search.GetNextSetAsync();
                foreach (var document in documentList)
                {
                    resultList.Add(AWSDocumentConverter.ToObject<T>(document));
                }

            } while (!search.IsDone);

            return resultList;
        }

        public List<T> Query<T>(List<QueryFilterCondition> queryFilterConditions, bool consistentRead = true, List<string> attributesToGet = null)
        {
            QueryOperationConfig queryOperationConfig = GetQueryOperationConfig(queryFilterConditions, consistentRead, attributesToGet);

            Search search = dynamoDBTable.Query(queryOperationConfig);

            List<T> resultList = new List<T>();
            List<Document> documentList = new List<Document>();
            do
            {
                documentList = search.GetNextSet();
                foreach (var document in documentList)
                {
                    resultList.Add(AWSDocumentConverter.ToObject<T>(document));
                }

            } while (!search.IsDone);

            return resultList;
        }


        private QueryOperationConfig GetQueryOperationConfig(List<QueryFilterCondition> QueryFilterConditions, bool consistentRead = true, List<string> attributesToGet = null)
        {
            QueryFilter queryFilter = new QueryFilter();

            foreach (var queryFilterCondition in QueryFilterConditions)
            {
                if (queryFilterCondition.Type == QueryFilterConditionType.one)
                    queryFilter.AddCondition(queryFilterCondition.AttributeName, queryFilterCondition.Condition);
                if (queryFilterCondition.Type == QueryFilterConditionType.two)
                    queryFilter.AddCondition(queryFilterCondition.AttributeName, queryFilterCondition.QueryOperator, queryFilterCondition.AttributeValues);
                if (queryFilterCondition.Type == QueryFilterConditionType.three)
                    queryFilter.AddCondition(queryFilterCondition.AttributeName, queryFilterCondition.QueryOperator, queryFilterCondition.DynamoDBEntry);
            }

            QueryOperationConfig queryOperationConfig = new QueryOperationConfig()
            {
                ConsistentRead = consistentRead,
                Filter = queryFilter
            };
            if (attributesToGet != null)
                queryOperationConfig.AttributesToGet = attributesToGet;

            return queryOperationConfig;
        }




        private UpdateItemRequest GetUpdateItemRequestForAtomicCounter(Primitive id, string attribute, int value)
        {
            AttributeValue keyAttributeValue;
            if(this.AWSDynamoTableConfig.KeyType == typeof(string))
            {
                keyAttributeValue = new AttributeValue { S = id };
            }
            else
            {
                keyAttributeValue = new AttributeValue { N = id };
            }

            return new UpdateItemRequest
            {
                TableName = AWSDynamoTableConfig.TableName,
                Key = new Dictionary<string, AttributeValue>
                {
                    { AWSDynamoTableConfig.KeyName, keyAttributeValue }
                },
                AttributeUpdates = new Dictionary<string, AttributeValueUpdate>()
                {
                    {
                        attribute,
                        new AttributeValueUpdate { Action = "ADD", Value = new AttributeValue { N = value.ToString() } }
                    },
                },
            };
        }

        private UpdateItemRequest GetUpdateItemRequestForAtomicCounter(Primitive id, Primitive sortKey, string attribute, int value)
        {
            AttributeValue keyAttributeValue;
            if (this.AWSDynamoTableConfig.KeyType == typeof(string))
            {
                keyAttributeValue = new AttributeValue { S = id };
            }
            else
            {
                keyAttributeValue = new AttributeValue { N = id };
            }

            AttributeValue sortKeyAttributeValue;
            if (this.AWSDynamoTableConfig.SortKeyType == typeof(string))
            {
                sortKeyAttributeValue = new AttributeValue { S = sortKey };
            }
            else
            {
                sortKeyAttributeValue = new AttributeValue { N = sortKey };
            }

            return new UpdateItemRequest
            {
                TableName = AWSDynamoTableConfig.TableName,
                Key = new Dictionary<string, AttributeValue>
                {
                    { AWSDynamoTableConfig.KeyName, keyAttributeValue },
                    { AWSDynamoTableConfig.SortKeyName,  sortKeyAttributeValue }
                },
                AttributeUpdates = new Dictionary<string, AttributeValueUpdate>()
                {
                    {
                        attribute,
                        new AttributeValueUpdate { Action = "ADD", Value = new AttributeValue { N = value.ToString() } }
                    },
                },
            };
        }


        public void AtomicCounter(Primitive id, string attribute, int value)
        {
            var request = GetUpdateItemRequestForAtomicCounter(id, attribute, value);

            client.UpdateItem(request);
        }

        public void AtomicCounter(Primitive id, Primitive sortKey, string attribute, int value)
        {
            var request = GetUpdateItemRequestForAtomicCounter(id, sortKey, attribute, value);

            client.UpdateItem(request);
        }

        public async Task AtomicCounterAsync(Primitive id, string attribute, int value)
        {
            var request = GetUpdateItemRequestForAtomicCounter(id, attribute, value);

            await client.UpdateItemAsync(request);
        }
        public async Task AtomicCounterAsync(Primitive id, Primitive sortKey, string attribute, int value)
        {
            var request = GetUpdateItemRequestForAtomicCounter(id, sortKey, attribute, value);

            await client.UpdateItemAsync(request);
        }
        private UpdateItemOperationConfig GetUpdateItemOperationConfigForConditionalUpdate(string condAttribute, object expectedValue)
        {
            var splittedAttributes = condAttribute.Split('.');

            var specificAttributeToCheck = condAttribute;
            string first = "";
            string last = condAttribute;
            if (splittedAttributes.Length > 1)
            {
                last = splittedAttributes.Last();
                first = condAttribute.Remove(condAttribute.Length - last.Length);
            }

            Expression expr = new Expression();
            expr.ExpressionStatement = first + "#Cond = :Cond";
            expr.ExpressionAttributeNames["#Cond"] = last;
            if (expectedValue.GetType() == typeof(int))
                expr.ExpressionAttributeValues[":Cond"] = (int)expectedValue;
            if (expectedValue.GetType() == typeof(string))
                expr.ExpressionAttributeValues[":Cond"] = (string)expectedValue;

            UpdateItemOperationConfig config = new UpdateItemOperationConfig()
            {
                ConditionalExpression = expr,
                ReturnValues = ReturnValues.None
            };

            return config;
        }
        public void ConditionalUpdate<T>(T tObject, string condAttribute, object expectedValue)
        {
            var config = GetUpdateItemOperationConfigForConditionalUpdate(condAttribute, expectedValue);
            dynamoDBTable.UpdateItem(AWSDocumentConverter.ToDocument(tObject), config);
        }
        public async void ConditionalUpdateAsync<T>(T tObject, string condAttribute, object expectedValue)
        {
            var config = GetUpdateItemOperationConfigForConditionalUpdate(condAttribute, expectedValue);
            await dynamoDBTable.UpdateItemAsync(AWSDocumentConverter.ToDocument(tObject), config);
        }

        

        public AWSDynamoDBStream GetAWSDynamoDBStream(AWSDynamoDBIteratorType type)
        {
            return new AWSDynamoDBStream(this.BasicAWSCredentials, this.RegionEndpoint, this.AWSDynamoTableConfig.TableName, type);
        }
        
    }
}
