using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AWSDynamoDBFramework
{
    public class AWSDynamoTableConfig
    {
        public string TableName { get; private set; }
        public Type KeyType { get; private set; }
        public string KeyName { get; private set; }
        public Type SortKeyType { get; set; }
        public string SortKeyName { get; set; }
        public int ReadCapacityUnits { get; private set; }
        public int WriteCapacityUnits { get; private set; }
        public bool StreamEnabled { get; set; }

        public AWSDynamoTableConfig(string tableName, Type keyType = null, string keyName = "Id", int readCapacityUnits = 5, int writeCapacityUnits = 5, bool streamEnabled = false)
        {
            this.TableName = tableName;
            this.KeyName = keyName;
            this.KeyType = keyType != null ? keyType : typeof(string);
            this.WriteCapacityUnits = writeCapacityUnits;
            this.ReadCapacityUnits = readCapacityUnits;
            this.StreamEnabled = streamEnabled;
        }

        public AWSDynamoTableConfig(string tableName, Type keyType = null, string keyName = "Id", Type sortKeyType = null, string sortKeyName = "SortKey", int readCapacityUnits = 5, int writeCapacityUnits = 5, bool streamEnabled = false)
        {
            this.TableName = tableName;
            this.KeyName = keyName;
            this.KeyType = keyType != null ? keyType : typeof(string);
            this.SortKeyName = sortKeyName;
            this.SortKeyType = keyType != null ? keyType : typeof(string);
            this.WriteCapacityUnits = writeCapacityUnits;
            this.ReadCapacityUnits = readCapacityUnits;
            this.StreamEnabled = streamEnabled;
        }
    }
}
