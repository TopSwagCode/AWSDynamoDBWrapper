using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;

namespace AWSDynamoDBFramework
{
    public class ScanFilterCondition
    {
        public string AttributeName { get; set; }
        public ScanOperator ScanOperator { get; set; }  
        public Condition Condition { get; set; }
        public DynamoDBEntry[] DynamoDBEntry { get; set; }
        public List<AttributeValue> AttributeValues { get; set; }
        public ScanFilterConditionType Type { get; set; }

        public ScanFilterCondition(string attributeName, Condition condition)
        {
            this.AttributeName = attributeName;
            this.Condition = condition;
            this.Type = ScanFilterConditionType.one;
        }

        public ScanFilterCondition(string attributeName, ScanOperator scanOperator, List<AttributeValue> attributeValues)
        {
            this.AttributeName = attributeName;
            this.Type = ScanFilterConditionType.two;
            this.ScanOperator = scanOperator;
            this.AttributeValues = attributeValues;
        }

        public ScanFilterCondition(string attributeName, ScanOperator scanOperator, params DynamoDBEntry[] values)
        {
            this.AttributeName = attributeName;
            this.Type = ScanFilterConditionType.three;
            this.ScanOperator = scanOperator;
            this.DynamoDBEntry = values;
        }
    }

    public enum ScanFilterConditionType
    {
        one,
        two,
        three
    }
}
