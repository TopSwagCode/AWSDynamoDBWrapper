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
using System.Threading;

namespace Samples
{
    class Program
    {
        static void Main(string[] args)
        {
            AWSDynamoTableConfig config = new AWSDynamoTableConfig("Persons", typeof(string), "Id", 5, 5);

            // Create a DynamoDBContext
            DynamoDBContext ddbc = new DynamoDBContext(
                RegionEndpoint.EUWest1,
                "##################",                           // Access key
                "#########################################",    // Secret key
                config                                          // Table config
                );                                       

            Console.WriteLine("Press enter to exit...");

            Console.ReadLine();
        }

        public static void UpdateTable(DynamoDBContext ddbc)
        {
            ddbc.AWSDynamoDBTable.ReadCapacityUnits = 15;
            ddbc.AWSDynamoDBTable.WriteCapacityUnits = 15;
            ddbc.AWSDynamoDBTable.UpdateTable();
        }

        public static void PersonStream(DynamoDBContext ddbc)
        {
            AWSDynamoTableConfig config = new AWSDynamoTableConfig("PersonEvents", typeof(string), "EventID", 1, 5);
            // Create a DynamoDBContext
            DynamoDBContext ddbcEvents = new DynamoDBContext(
                RegionEndpoint.EUWest1,
                "##################",                           // Access key
                "#########################################",    // Secret key
                config                                          // Table config
                );

            var stream = ddbc.GetAWSDynamoDBStream(AWSDynamoDBIteratorType.LATEST);

            while (true)
            {
                Thread.Sleep(5000);
                Console.WriteLine("- - - - - - - - - - - - - - - - ");
                Console.WriteLine("Getting records");
                var records = stream.GetRecords<Person>();
                foreach (var record in records)
                {
                    Console.WriteLine("EventName: " + record.EventName + " || Person ID: " + record.NewImage.Id + " || SequenceNumber: " + record.SequenceNumber);
                    ddbcEvents.Insert(record);
                }
            }
        }

        public static void PersonExample(DynamoDBContext ddbc)
        {

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
            var result = ddbc.Scan<Person>(new List<ScanFilterCondition>() { sfc }).ToList();

            // Delete all people in scan result.
            var idList = result.Select(x => x.Id).ToList();
            ddbc.BatchDelete(idList);

            // Count age up by 2 atomicly (Also works with negative numbers)
            ddbc.AtomicCounter("uniqueid", "Age", 2);

            // Conditional Update. Update Person's job only if great-grandfather's name is Troels       
            Person personCondUpdate = new Person() { Id = "uniqueid" };
            personCondUpdate.Job = new Job()
            {
                JobName = "CEO",
                Salary = 9878984,
                Seniority = 12
            };

            ddbc.ConditionalUpdate(personCondUpdate, "Father.Mother.Father.Name", "Troels");

        }


    }
}
