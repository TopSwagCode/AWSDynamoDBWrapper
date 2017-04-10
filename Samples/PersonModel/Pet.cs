using AWSDynamoDBFramework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Samples.PersonModel
{
    public class Pet : AWSDocumentConverter
    {
        public string Name { get; set; }
        public int Age { get; set; }
        public List<Job> PetJobs { get; set; }
        public Pet()
        {

        }
    }
}
