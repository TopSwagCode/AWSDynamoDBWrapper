using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace AWSDynamoDBFramework
{
    public class AWSDynamoDBTable
    {
        private string TableName { get; set; }
        private Type KeyType { get; set; }
        private string KeyName { get; set; }
        private Type SortKeyType { get; set; }
        private string SortKeyName { get; set; }
        public int ReadCapacityUnits { get; set; }
        public int WriteCapacityUnits { get; set; }

        private bool EnableStream { get; set; }

        private AmazonDynamoDBClient client;

        public AWSDynamoDBTable(AmazonDynamoDBClient client, string tableName, string keyName, Type keyType, int readCapacityUnits, int writeCapacityUnits, bool enableStream = false)
        {
            this.client = client;
            this.TableName = tableName;
            this.KeyName = keyName;
            this.KeyType = keyType;
            this.ReadCapacityUnits = readCapacityUnits;
            this.WriteCapacityUnits = writeCapacityUnits;
            this.EnableStream = enableStream;
        }

        public AWSDynamoDBTable(AmazonDynamoDBClient client, string tableName, string keyName, Type keyType, string sortKeyName, Type sortKeyType, int readCapacityUnits, int writeCapacityUnits, bool enableStream = false)
        {
            this.client = client;
            this.TableName = tableName;
            this.KeyName = keyName;
            this.KeyType = keyType;
            this.SortKeyName = sortKeyName;
            this.SortKeyType = sortKeyType;
            this.ReadCapacityUnits = readCapacityUnits;
            this.WriteCapacityUnits = writeCapacityUnits;
            this.EnableStream = enableStream;
        }

        public void UpdateTable()
        {
            client.UpdateTable(new UpdateTableRequest()
            {
                TableName = this.TableName,
                ProvisionedThroughput = new ProvisionedThroughput(ReadCapacityUnits, WriteCapacityUnits)
            });
        }

        public async Task UpdateTableAsync()
        {
            await client.UpdateTableAsync(new UpdateTableRequest()
            {
                TableName = this.TableName,
                ProvisionedThroughput = new ProvisionedThroughput(ReadCapacityUnits, WriteCapacityUnits)
            });
        }

        private CreateTableRequest GetCreateTableRequest()
        {
            var attributeDefinitions = new List<AttributeDefinition>();
            attributeDefinitions.Add(new AttributeDefinition
            {
                AttributeName = KeyName,
                AttributeType = KeyType == typeof(string) ? "S" : "N"
            });

            var keySchema = new List<KeySchemaElement>();
            keySchema.Add(new KeySchemaElement
            {
                AttributeName = KeyName,
                KeyType = "HASH"
            });

            if (this.SortKeyType != null)
            {
                attributeDefinitions.Add(new AttributeDefinition
                {
                    AttributeName = SortKeyName,
                    AttributeType = SortKeyType == typeof(string) ? "S" : "N"
                });
                keySchema.Add(new KeySchemaElement
                {
                    AttributeName = SortKeyName,
                    KeyType = "RANGE"
                });

            }

            var createTableRequest = new CreateTableRequest
            {
                TableName = TableName,

                AttributeDefinitions = attributeDefinitions,
                KeySchema = keySchema,
                ProvisionedThroughput = new ProvisionedThroughput
                {
                    ReadCapacityUnits = ReadCapacityUnits,
                    WriteCapacityUnits = WriteCapacityUnits
                }
            };

            if (EnableStream)
            {
                var streamSpecification = new StreamSpecification()
                {
                    StreamEnabled = true,
                    StreamViewType = StreamViewType.NEW_AND_OLD_IMAGES,
                };

                createTableRequest.StreamSpecification = streamSpecification;
            }

            return createTableRequest;
        }

        public void ExecuteCreateTable()
        {
            try
            {
                var createTableRequest = GetCreateTableRequest();

                var response = client.CreateTable(createTableRequest);

                WaitTillTableCreated(client, TableName, response);

            }
            catch (ResourceInUseException) // Table is being used (created) by other process.
            {
                WaitTillTableCreated(client, TableName);
            }    
        }

        public bool TableExists()
        {
            try
            {
                var res = client.DescribeTable(new DescribeTableRequest
                {
                    TableName = TableName
                });

                if (res == null || res.Table == null)
                    return false;

                var statusCode = res.HttpStatusCode;
                var ready = res.Table.TableStatus;

                return ready == TableStatus.ACTIVE;
            }
            catch
            {
                return false;
            }
        }

        private static void WaitTillTableCreated(AmazonDynamoDBClient client, string tableName, CreateTableResponse response = null)
        {
            string status = "NOTACTIVE";
    
            if(response != null)
            {
                var tableDescription = response.TableDescription;
                status = tableDescription.TableStatus;
            }
            
            Console.WriteLine(tableName + " - " + status);

            // Let us wait until table is created. Call DescribeTable.
            while (status != "ACTIVE")
            {
                System.Threading.Thread.Sleep(5000); // Wait 5 seconds.
                try
                {
                    var res = client.DescribeTable(new DescribeTableRequest
                    {
                        TableName = tableName
                    });
                    Console.WriteLine("Table name: {0}, status: {1}", res.Table.TableName,
                              res.Table.TableStatus);
                    status = res.Table.TableStatus;
                }

                // Try-catch to handle potential eventual-consistency issue.
                catch (ResourceNotFoundException)
                { }
            }
        }

        private void DeleteTable(bool waitTillTableDeleted = false)
        {
            try
            {
                var deleteTableResponse = client.DeleteTable(new DeleteTableRequest()
                {
                    TableName = TableName
                });
                if(waitTillTableDeleted)
                    WaitTillTableDeleted(client, TableName, deleteTableResponse);
            }
            catch (ResourceNotFoundException)
            {
                // There is no such table.
            }
        }

        private static void WaitTillTableDeleted(AmazonDynamoDBClient client, string tableName, DeleteTableResponse response)
        {
            var tableDescription = response.TableDescription;

            string status = tableDescription.TableStatus;

            Console.WriteLine(tableName + " - " + status);

            // Let us wait until table is created. Call DescribeTable
            try
            {
                while (status == "DELETING")
                {
                    System.Threading.Thread.Sleep(5000); // wait 5 seconds

                    var res = client.DescribeTable(new DescribeTableRequest
                    {
                        TableName = tableName
                    });
                    Console.WriteLine("Table name: {0}, status: {1}", res.Table.TableName,
                              res.Table.TableStatus);
                    status = res.Table.TableStatus;
                }

            }
            catch (ResourceNotFoundException)
            {
                // Table deleted.
            }
        }


        public async Task ExecuteCreateTableAsync()
        {
            try
            {
                var createTableRequest = GetCreateTableRequest();

                var response = await client.CreateTableAsync(createTableRequest);

                await WaitTillTableCreatedAsync(client, TableName, response);

            }
            catch (ResourceInUseException) // Table is being used (created) by other process.
            {
                await WaitTillTableCreatedAsync(client, TableName);
            }
        }

        public async Task<bool> TableExistsAsync()
        {
            try
            {
                var res = await client.DescribeTableAsync(new DescribeTableRequest
                {
                    TableName = TableName
                });

                if (res == null || res.Table == null)
                    return false;

                var statusCode = res.HttpStatusCode;
                var ready = res.Table.TableStatus;

                return ready == TableStatus.ACTIVE;
            }
            catch
            {
                return false;
            }
        }

        private async Task WaitTillTableCreatedAsync(AmazonDynamoDBClient client, string tableName, CreateTableResponse response = null)
        {
            string status = "NOTACTIVE";

            if (response != null)
            {
                var tableDescription = response.TableDescription;
                status = tableDescription.TableStatus;
            }

            Console.WriteLine(tableName + " - " + status);

            // Let us wait until table is created. Call DescribeTable.
            while (status != "ACTIVE")
            {
                await Task.Delay(5000);

                try
                {
                    var res = await client.DescribeTableAsync(new DescribeTableRequest
                    {
                        TableName = tableName
                    });
                    Console.WriteLine("Table name: {0}, status: {1}", res.Table.TableName,
                              res.Table.TableStatus);
                    status = res.Table.TableStatus;
                }

                // Try-catch to handle potential eventual-consistency issue.
                catch (ResourceNotFoundException)
                { }
            }
        }

        private async Task DeleteTableAsync()
        {
            try
            {
                var deleteTableResponse = await client.DeleteTableAsync(new DeleteTableRequest()
                {
                    TableName = TableName
                });

                await WaitTillTableDeletedAsync(client, TableName, deleteTableResponse);
            }
            catch (ResourceNotFoundException)
            {
                // There is no such table.
            }
        }

        private async Task WaitTillTableDeletedAsync(AmazonDynamoDBClient client, string tableName,
                         DeleteTableResponse response)
        {
            var tableDescription = response.TableDescription;

            string status = tableDescription.TableStatus;

            Console.WriteLine(tableName + " - " + status);

            // Let us wait until table is created. Call DescribeTable
            try
            {
                while (status == "DELETING")
                {
                    await Task.Delay(5000); // wait 5 seconds

                    var res = await client.DescribeTableAsync(new DescribeTableRequest
                    {
                        TableName = tableName
                    });
                    Console.WriteLine("Table name: {0}, status: {1}", res.Table.TableName,
                              res.Table.TableStatus);
                    status = res.Table.TableStatus;
                }

            }
            catch (ResourceNotFoundException)
            {
                // Table deleted.
            }
        }
    }
}
