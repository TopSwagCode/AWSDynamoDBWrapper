using AWSDynamoDBFramework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Samples.ProductModel
{
    public class Product : AWSDocumentConverter
    {
        public int ProductId { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public List<Product> RelatedProducts { get; set; }
        public List<Category> Categories { get; set; }

        public Product() 
        {

        }
    }
}
