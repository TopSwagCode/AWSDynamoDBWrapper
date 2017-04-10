# AWSDynamoDBWrapper

A small project for making the use of DynamoDB in .NET projects easier.    
After looking at the [AWS Mapping example](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBContext.ArbitraryDataMapping.html) I thougt there had to be an easier way.    

In the sample project I have a data structure looking like this:

Person:
* Id (String)
* Name (String)
* Age (Int?)
* FavFruits (List<String>)
* Human (Bool?)
* Pet (Pet)
* Pets (List<Pet>)
* Father (Person)
* Mother (Person)
* Job (Job)

Pet:
* Name (String)
* Age (Int)
* PetJobs (List<Job>)

Job:
* JobName (String)
* Salary (Int)
* Seniority (Int)

A good structure with some depth giving alot of work making DimensionTypeConverter for DynamoDB.

To get starting just make a DynamoDBContext.
~~~~~~.NET
// Create a DynamoDBContext
DynamoDBContext ddbc = new DynamoDBContext(
    "Person", // Table name
    RegionEndpoint.EUWest1, // Region
    "##############", // Access key
    "#########################################"); // Secret key
~~~~~~

To insert a object simply implement the abstact class to your classes like so:
~~~~~~.NET
public class Person : AWSDocumentConverter
{
    ....
}

public class Job : AWSDocumentConverter
{
    ....
}

public class Pet : AWSDocumentConverter
{
    ....
}
~~~~~~

This will enable you to use DynamoDB like so:

~~~~~~.NET
// Insert a person
Person p = new Person(){
    Id = "something",
    Name = "Harry Twotter",
    .....
}

ddbc.Insert(person);
~~~~~~

~~~~~~.NET
// Get a person by id
var person = ddbc.Get<Person>("uniqueid");
~~~~~~

~~~~~~.NET
// Insert alot of persons at the same time. Use Batch insert.
List<Person> persons = new List<Person>();

.... // Add alot of persons

ddbc.BatchInsert(persons);
~~~~~~

~~~~~~.NET
// Get a list of all person with age greater than 30.
ScanFilterCondition sfc = new ScanFilterCondition("Age", ScanOperator.GreaterThan, 30);
var result = ddbc.Scan<Person>(new List<ScanFilterCondition>() {sfc}).ToList();
~~~~~~

~~~~~~.NET
// Atomic counter of attribute in DynamoDB
// id, attribute, value
ddbc.AtomicCounter("uniqueid", "Age", -21);
~~~~~~

~~~~~~.NET
// Conditional Update in DynamoDB. Eg. only update if Person Father's Mother's Father name is "Troels" (great-grandfather name).
ddbc.ConditionalUpdate(person, "Father.Mother.Father.Name", "Troels");
~~~~~~