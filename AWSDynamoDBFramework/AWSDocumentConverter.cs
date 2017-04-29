using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.DocumentModel;

namespace AWSDynamoDBFramework
{
    public class AWSDocumentConverter
    {
        public static Document ToDocument<T>(T tObject, bool removeNulls = true, bool removeZeros = true)
        {
            var dictionary = tObject.GetType()
                .GetProperties(BindingFlags.Instance | BindingFlags.Public)
                .ToDictionary(prop => prop.Name, prop => prop.GetValue(tObject, null));
            if (removeNulls)
                dictionary = dictionary.Where(x => x.Value != null).ToDictionary(y => y.Key, y => y.Value);

            var document = new Document();
            foreach (var entry in dictionary)
            {
                if (entry.Value == null)
                {
                    continue;
                }

                var type = entry.Value.GetType();

                if (type == typeof(List<Object>))
                {
                    Console.WriteLine("WE HAVE A LIST"); // Look away. Nothing to see here. Please stop reading. Come on.... DO you feel better for reading this? Was it worth your time? Really have nothing better to do?                                                          COME ON. IT's Done. Nothing left. I promise. Not like there is any gold at the end of this comment. Please go on. We both have better things to be doing right now. I could be writting alot of usefull code, but instead we are both stuck reading this comment. Hope your happy now. I am not stopping before you are! Wonder if anyone is still reading. Not like anything usefull is coming later. Wonder how many skipped the first part. Please just go on with your day / night. THIS AINT OVER TO I SAY IT'S OVER!
                }

                if (type == typeof(string))
                {
                    document.Add(entry.Key, (string)entry.Value);
                }
                else if (type == typeof(int) || type == typeof(int?))
                {
                    if (removeZeros)
                    {
                        if ((int)entry.Value != 0)
                        {
                            document.Add(entry.Key, (int)entry.Value);
                        }
                    }
                    else
                    {
                        document.Add(entry.Key, (int)entry.Value);
                    }
                }
                else if (type == typeof(bool) || type == typeof(bool?))
                {
                    document.Add(entry.Key, new DynamoDBBool((bool)entry.Value));
                }
                else if (type == typeof(List<string>))
                {
                    document.Add(entry.Key, (List<string>)entry.Value);
                }
                else if (type == typeof(List<int>))
                {
                    var dynamoDBList = new DynamoDBList(new List<DynamoDBEntry>());

                    foreach (var i in (List<int>)entry.Value)
                    {
                        dynamoDBList.Add(i);
                    }
                    document.Add(entry.Key, dynamoDBList);
                }
                else if (type == typeof(DateTime))
                {
                    document.Add(entry.Key, (DateTime)entry.Value);
                }
                else if (type == typeof(float) || type == typeof(float?))
                {
                    if (removeZeros)
                    {
                        if ((float)entry.Value != 0)
                        {
                            document.Add(entry.Key, (float)entry.Value);
                        }
                    }
                    else
                    {
                        document.Add(entry.Key, (float)entry.Value);
                    }
                }
                else if (type == typeof(double) || type == typeof(double?))
                {
                    if (removeZeros)
                    {
                        if ((double)entry.Value != 0)
                        {
                            document.Add(entry.Key, (double)entry.Value);
                        }
                    }
                    else
                    {
                        document.Add(entry.Key, (double)entry.Value);
                    }
                }
                else if (type == typeof(uint?) || type == typeof(uint?))
                {
                    if (removeZeros)
                    {
                        if ((uint)entry.Value != 0)
                        {
                            document.Add(entry.Key, (uint)entry.Value);
                        }
                    }
                    else
                    {
                        document.Add(entry.Key, (uint)entry.Value);
                    }
                }
                else if (type.BaseType != null && type.BaseType.IsAssignableFrom(typeof(T)))
                {
                    if (type.BaseType == typeof(Object) && type.Name == "List`1") // If is list
                    {
                        var dynamoDbList = new DynamoDBList(new List<Document>());
                        var genericList = entry.Value as IEnumerable;
                        if (genericList != null)
                            foreach (var test in genericList)
                            {
                                var dbEntry = test;
                                if (dbEntry != null) dynamoDbList.Add(AWSDocumentConverter.ToDocument(dbEntry, removeNulls));
                            }

                        document.Add(entry.Key, dynamoDbList);
                    }
                    else if (type.BaseType != null)
                    {
                        var docEntry = entry.Value;
                        if (docEntry != null) document.Add(entry.Key, AWSDocumentConverter.ToDocument(docEntry, removeNulls));
                    }
                }
            }
            return document;
        }


        private static PropertyInfo TestObjects(string key, Type test)
        {
            var propertyinfo = test.GetProperties().SingleOrDefault(x => x.Name == key);

            return propertyinfo;
        }

        public static T ToObject<T>(Document document, string className = "", string classNamespace = "")
        {
            return (T)ToObjectTest<T>(document, className, classNamespace);

        }

        private static object ToObjectTest<T>(Document document, string className = "", string classNamespace = "")
        {
            var thisNamespace = !string.IsNullOrEmpty(classNamespace) ? classNamespace : typeof(T).Namespace;

            var classObject = Activator.CreateInstance(typeof(T));
            var classPropertyInfo = classObject.GetType().GetProperties().FirstOrDefault(x => x.Name == className);
            
            if(classPropertyInfo != null)
            {
                var propertyType = classPropertyInfo.PropertyType;
                classObject = Activator.CreateInstance(propertyType);
            }

            if (document != null)
                foreach (var entry in document)
                {
                    var propertyInfo = TestObjects(entry.Key, classObject.GetType());
                    

                    if (propertyInfo != null)
                    {
                        if (propertyInfo.PropertyType == typeof(string))
                        {
                            propertyInfo.SetValue(classObject, entry.Value.AsString());
                        }
                        else if (propertyInfo.PropertyType == typeof(int) || propertyInfo.PropertyType == typeof(int?))
                        {
                            propertyInfo.SetValue(classObject, entry.Value.AsInt());
                        }
                        else if (propertyInfo.PropertyType == typeof(bool) || propertyInfo.PropertyType == typeof(bool?))
                        {
                            propertyInfo.SetValue(classObject, entry.Value.AsBoolean());
                        }
                        else if (propertyInfo.PropertyType == typeof(List<string>))
                        {
                            propertyInfo.SetValue(classObject, entry.Value.AsListOfString());
                        }
                        else if (propertyInfo.PropertyType == typeof(List<int>))
                        {
                            List<int> integerList = new List<int>();
                            foreach (var dynamoDbEntry in entry.Value.AsListOfDynamoDBEntry())
                            {
                                integerList.Add(dynamoDbEntry.AsInt());
                            }
                            propertyInfo.SetValue(classObject, integerList);
                        }
                        else if (propertyInfo.PropertyType == typeof(float) || propertyInfo.PropertyType == typeof(float?))
                        {
                            propertyInfo.SetValue(classObject, (float)entry.Value.AsDouble()); // Needs testing.
                        }
                        else if (propertyInfo.PropertyType == typeof(double) || propertyInfo.PropertyType == typeof(double?))
                        {
                            propertyInfo.SetValue(classObject, entry.Value.AsDouble()); // Needs testing.
                        }
                        else if (propertyInfo.PropertyType == typeof(uint))
                        {
                            propertyInfo.SetValue(classObject, entry.Value.AsUInt()); // Needs testing.
                        }
                        else if (propertyInfo.PropertyType == typeof(long) || propertyInfo.PropertyType == typeof(long?))
                        {
                            propertyInfo.SetValue(classObject, entry.Value.AsLong()); // Needs testing.
                        }
                        else if (propertyInfo.PropertyType == typeof(ulong) || propertyInfo.PropertyType == typeof(ulong?))
                        {
                            propertyInfo.SetValue(classObject, entry.Value.AsULong()); // Needs testing.
                        }
                        else if (propertyInfo.PropertyType == typeof(char) || propertyInfo.PropertyType == typeof(char?))
                        {
                            propertyInfo.SetValue(classObject, entry.Value.AsChar()); // Needs testing.
                        }
                        else if (propertyInfo.PropertyType == typeof(DateTime) || propertyInfo.PropertyType == typeof(DateTime?))
                        {
                            propertyInfo.SetValue(classObject, entry.Value.AsDateTime()); // Needs testing.
                        }
                        else
                        {
                            Type valueType = entry.Value.GetType();
                            
                            if (valueType == typeof(DynamoDBList))
                            {
                                var dynamoDbList = entry.Value as DynamoDBList;
                                var subType = propertyInfo.PropertyType.GetGenericArguments()[0];
                                var subClassName = subType.Name;

                                var genericList = typeof(List<>);
                                var stringList = genericList.MakeGenericType(subType);
                                var list = Activator.CreateInstance(stringList);

                                var addMethod = stringList.GetMethod("Add");

                                foreach (var dbEntry in dynamoDbList.Entries)
                                {
                                    var tmpDoc = dbEntry as Document;
                                    var subObject = AWSDocumentConverter.ToObjectTest<T>(tmpDoc, subClassName);
                                    addMethod.Invoke(list, new object[] { subObject });
                                }

                                propertyInfo.SetValue(classObject, list);

                            }
                            else if (valueType == typeof(Document))
                            {
                                var tmpDoc = entry.Value as Document;
                                var subDocuement = AWSDocumentConverter.ToObjectTest<T>(tmpDoc, propertyInfo.PropertyType.Name,
                                    propertyInfo.PropertyType.Namespace);
                                propertyInfo.SetValue(classObject, subDocuement);
                            }
                        }
                    }
                }
            return classObject;
        }
    }
}
