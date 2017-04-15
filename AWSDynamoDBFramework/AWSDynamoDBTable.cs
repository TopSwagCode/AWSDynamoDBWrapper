using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AWSDynamoDBFramework
{
    public class AWSDynamoDBTable
    {
        private string TableName { get; set; }
        private Type KeyType { get; set; }
        private string KeyName { get; set; }
        public int ReadCapacityUnits { get; set; }
        public int WriteCapacityUnits { get; set; }

        private AmazonDynamoDBClient client;

        public AWSDynamoDBTable(AmazonDynamoDBClient client, string tableName, string keyName, Type keyType, int readCapacityUnits, int writeCapacityUnits)
        {
            this.client = client;
            this.TableName = tableName;
            this.KeyName = keyName;
            this.KeyType = keyType;
            this.ReadCapacityUnits = readCapacityUnits;
            this.WriteCapacityUnits = writeCapacityUnits;
        }

        public void UpdateTable(bool waitUntilTableCreated = false)
        {
            var test = client.UpdateTable(new UpdateTableRequest()
            {
                TableName = this.TableName,
                ProvisionedThroughput = new ProvisionedThroughput(ReadCapacityUnits, WriteCapacityUnits)
            });
            var test2 = test.ResponseMetadata;
        }

        public void ExecuteCreateTable(bool waitUntilTableCreated = false)
        {
            TableExists();
            var response = client.CreateTable(new CreateTableRequest
            {
                TableName = TableName,
                AttributeDefinitions = new List<AttributeDefinition>()
                              {
                                  new AttributeDefinition
                                  {
                                      AttributeName = KeyName,
                                      AttributeType = KeyType == typeof(string) ? "S" : "N"
                                  }
                              },
                KeySchema = new List<KeySchemaElement>()
                              {
                                  new KeySchemaElement
                                  {
                                      AttributeName = KeyName,
                                      KeyType = "HASH"
                                  }
                              },
                ProvisionedThroughput = new ProvisionedThroughput
                {
                    ReadCapacityUnits = ReadCapacityUnits,
                    WriteCapacityUnits = WriteCapacityUnits
                }
            });

            if(waitUntilTableCreated)
                WaitTillTableCreated(client, TableName, response);
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

        private static void WaitTillTableCreated(AmazonDynamoDBClient client, string tableName, CreateTableResponse response)
        {
            var tableDescription = response.TableDescription;

            string status = tableDescription.TableStatus;

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

        private static void WaitTillTableDeleted(AmazonDynamoDBClient client, string tableName,
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
    }
}
