using AWSDynamoDBFramework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Samples.PersonModel
{
    public class Job
    {
        public string JobName { get; set; }
        public int Salary { get; set; }
        public int Seniority { get; set; }

        public Job()
        {

        }
    }
}
