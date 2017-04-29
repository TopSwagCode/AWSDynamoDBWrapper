using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;

namespace AWSDynamoDBFramework
{
    public class QueryFilterCondition
    {
        public string AttributeName { get; set; }
        public QueryOperator QueryOperator { get; set; }
        public Condition Condition { get; set; }
        public DynamoDBEntry[] DynamoDBEntry { get; set; }
        public List<AttributeValue> AttributeValues { get; set; }
        public QueryFilterConditionType Type { get; set; }

        public QueryFilterCondition(string attributeName, Condition condition)
        {
            this.AttributeName = attributeName;
            this.Condition = condition;
            this.Type = QueryFilterConditionType.one;
        }

        public QueryFilterCondition(string attributeName, QueryOperator QueryOperator, List<AttributeValue> attributeValues)
        {
            this.AttributeName = attributeName;
            this.Type = QueryFilterConditionType.two;
            this.QueryOperator = QueryOperator;
            this.AttributeValues = attributeValues;
        }

        public QueryFilterCondition(string attributeName, QueryOperator QueryOperator, params DynamoDBEntry[] values)
        {
            this.AttributeName = attributeName;
            this.Type = QueryFilterConditionType.three;
            this.QueryOperator = QueryOperator;
            this.DynamoDBEntry = values;
        }

    }

    public enum QueryFilterConditionType
    {
        one,
        two,
        three
    }
}
