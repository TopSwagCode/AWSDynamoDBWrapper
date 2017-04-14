using AWSDynamoDBFramework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Samples.ProductModel
{
    public class Category : AWSDocumentConverter
    {
        public string Name { get; set; }
        public string Description { get; set; }

        public Category()
        {

        }
    }
}
