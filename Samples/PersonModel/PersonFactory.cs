using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Samples.PersonModel
{
    public class PersonFactory
    {

        private static Random random = new Random();
        public static string RandomString(int length)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
            return new string(Enumerable.Repeat(chars, length)
              .Select(s => s[random.Next(s.Length)]).ToArray());
        }

        public static int RandomNumber(int start, int end)
        {
            return random.Next(start, end);
        }

        static float RandomFloat()
        {
            double mantissa = (random.NextDouble() * 2.0) - 1.0;
            double exponent = Math.Pow(2.0, random.Next(-126, 128));
            return (float)(mantissa * exponent);
        }

        static DateTime RandomDay()
        {
            DateTime start = new DateTime(1995, 1, 1);
            int range = (DateTime.Today - start).Days;
            return start.AddDays(random.Next(range));
        }

        public static Person getRandomPerson()
        {
            Random rnd = new Random();

            Person p = new Person()
            {
                Id = RandomNumber(0, 1000000),
                Name = RandomString(6),
                BirthDate = RandomDay(),
                Age = RandomNumber(2000, 5000),
                FavFruits = new List<string>() { RandomString(5), RandomString(5)},
                LuckyNumbers = new List<int>() { RandomNumber(2,4), RandomNumber(2,5)},
                FloatMyBoat = RandomFloat(),
                Pet = new Pet()
                {
                    Name = RandomString(6),
                    Age = RandomNumber(1, 13),
                    PetJobs = new List<Job>() { new Job()
                    {
                        JobName = RandomString(10),
                    } }
                },
                Job = new Job()
                {
                    JobName = RandomString(15),
                    Salary = RandomNumber(1000, 10000),
                    Seniority = RandomNumber(0, 10)
                },
                Father = new Person()
                {
                    Name = RandomString(6),
                    Age = RandomNumber(50, 90),
                    Father = new Person()
                    {
                        Name = RandomString(6),
                        Age = RandomNumber(90, 120)
                    },
                    Mother = new Person()
                    {
                        Name = RandomString(6),
                        Age = RandomNumber(90, 120)
                    }
                },
                Mother = new Person()
                {
                    Name = RandomString(6),
                    Age = RandomNumber(50, 90),
                    Father = new Person()
                    {
                        Name = RandomString(6),
                        Age = RandomNumber(90, 120)
                    },
                    Mother = new Person()
                    {
                        Name = RandomString(6),
                        Age = RandomNumber(90, 120)
                    }
                }
            };

            return p;
        }

        public static Person getSpecificPerson()
        {
            var father = new Person()
            {
                Age = 40,
                FavFruits = new List<string>()
                {
                    "Bannnanana", "Apple"
                },
                Human = true,
                LuckyNumbers = new List<int>()
                {
                    1,
                    2,
                    3,
                    5,
                    7,
                    11,
                    13,
                    17,
                    19
                },
                Name = "Brian",
                Pet = new Pet()
                {
                    Age = 22,
                    Name = "Zed"
                },
                Mother = new Person()
                {
                    Id = "124314",
                    Name = "Iben",
                    Father = new Person()
                    {
                        Name = "Troels"
                    }
                }
            };

            var person = new Person()
            {
                Id = id,
                Age = 20,
                BirthDate = new DateTime(1987, 02, 17),
                Name = "Test Person",
                Human = false,
                FloatMyBoat = RandomFloat(),
                FavFruits = new List<string>()
                {
                    "Bannnanana", "Apple"
                },
                Pets = new List<Pet>()
                {
                    new Pet()
                    {
                        Age = 3,
                        Name = "Wuffi",
                        PetJobs = new List<Job>()
                        {
                            new Job() { JobName = "Police Dog"},
                            new Job() { JobName = "Drug searcher"}
                        }
                    },
                    new Pet() {Age = 5, Name = "Tutti"}
                },
                LuckyNumbers = new List<int>()
                {
                    1,
                    2,
                    3,
                    5,
                    7,
                    11,
                    13,
                    17,
                    19
                },
                Pet = new Pet()
                {
                    Age = 12,
                    Name = "Mitzi"
                },
                Father = father,
                Job = new Job()
                {
                    JobName = "Software Dev",
                    Salary = 1336,
                    Seniority = 2
                }
            };

            return person;
        }
    }
}
