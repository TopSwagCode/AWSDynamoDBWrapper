using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon;
using AWSDynamoDBFramework;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Samples.PersonModel;

namespace Samples
{
    class Program
    {
        static void Main(string[] args)
        {

            // Create a DynamoDBContext
            DynamoDBContext ddbc = new DynamoDBContext(
                "Person",
                RegionEndpoint.EUWest1,
                "##################", // Access key
                "#########################################"); // Secret key

            // Insert 1 person
            ddbc.Insert(PersonFactory.getSpecificPerson());

            // Get 1 person
            var person = ddbc.Get<Person>("uniqueid");

            // Batch Insert 100 Persons
            List<Person> persons = new List<Person>();
            for (int i = 0; i < 100; i++)
            {
                persons.Add(PersonFactory.getRandomPerson());
            }
            ddbc.BatchInsert(persons);

            // Scan for persons with age greater than 3500
            ScanFilterCondition sfc = new ScanFilterCondition("Age", ScanOperator.GreaterThan, 1000);
            var result = ddbc.Scan<Person>(new List<ScanFilterCondition>() {sfc}).ToList();

            // Delete all people in scan result.
            var idList = result.Select(x => x.Id).ToList();
            ddbc.BatchDelete(idList);

            // Count age up by 2 atomicly (Also works with negative numbers)
            ddbc.AtomicCounter("uniqueid", "Age", 2);

            // Conditional Update. Update Person's job only if great-grandfather's name is Troels       
            person.Job = new Job()
            {
                JobName = "CEO",
                Salary = 9878984,
                Seniority = 12
            };

            ddbc.ConditionalUpdate(person, "Father.Mother.Father.Name", "Troels");

            Console.WriteLine("Press enter to exit...");

            Console.ReadLine();
        }
    }
}
