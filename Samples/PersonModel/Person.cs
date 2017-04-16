using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AWSDynamoDBFramework;

namespace Samples.PersonModel
{
    public class Person
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public int? Age { get; set; }
        public List<string> FavFruits { get; set; }
        public bool? Human { get; set; }
        public List<int> LuckyNumbers { get; set; }
        public List<Pet> Pets { get; set; }
        public Pet Pet { get; set; }
        public Person Father { get; set; }
        public Person Mother { get; set; }
        public Job Job { get; set; }
        public Person()
        {

        }
    }
}
