using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon;
using AWSDynamoDBFramework;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using System.Threading;
using Samples.PersonModel;

namespace Datapump
{
    class Program
    {
        static void Main(string[] args)
        {
            AWSDynamoTableConfig config = new AWSDynamoTableConfig("Persons", typeof(string), "Id", 5, 5);
            // Create a DynamoDBContext
            DynamoDBContext ddbc = new DynamoDBContext(
                RegionEndpoint.EUWest1,
                "###################",                     // Access key
                "#######################################", // Secret key
                config                                     // Table config
                );

            while (true)
            {
                Thread.Sleep(1000);
                var person = PersonFactory.getRandomPerson();
                ddbc.Insert(person);
                Console.WriteLine("Inserting person: " + person.Id);
            }


        }
    }
}
