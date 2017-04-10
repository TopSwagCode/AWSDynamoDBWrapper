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
    public abstract class AWSDocumentConverter
    {
        public Document ToDocument(bool removeNulls = true, bool removeZeros = true)
        {
            var dictionary = this.GetType()
                .GetProperties(BindingFlags.Instance | BindingFlags.Public)
                .ToDictionary(prop => prop.Name, prop => prop.GetValue(this, null));
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
                    Console.WriteLine("WE HAVE A LIST");
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
                else if (type == typeof(float) || type == typeof(float?))
                {
                    document.Add(entry.Key, (float?)entry.Value);
                }
                else if (type == typeof(double))
                {
                    document.Add(entry.Key, (double)entry.Value);
                }
                else if (type == typeof(uint?) || type == typeof(uint?))
                {
                    document.Add(entry.Key, (uint)entry.Value);
                }
                else if (type.BaseType != null && type.BaseType.IsAssignableFrom(typeof(AWSDocumentConverter)))
                {
                    if (type.BaseType == typeof(Object) && type.Name == "List`1") // If is list
                    {
                        var dynamoDbList = new DynamoDBList(new List<Document>());
                        var genericList = entry.Value as IEnumerable;
                        if (genericList != null)
                            foreach (var test in genericList)
                            {
                                var dbEntry = test as AWSDocumentConverter;
                                if (dbEntry != null) dynamoDbList.Add(dbEntry.ToDocument(removeNulls));
                            }

                        document.Add(entry.Key, dynamoDbList);
                    }
                    else if (type.BaseType == typeof(AWSDocumentConverter)) // Of Class uses AWSDocumentConverter
                    {
                        var docEntry = entry.Value as AWSDocumentConverter;
                        if (docEntry != null) document.Add(entry.Key, docEntry.ToDocument(removeNulls));
                    }
                }
            }
            return document;
        }


        private PropertyInfo TestObjects(string key, Type test)
        {
            var propertyinfo = test.GetProperties().SingleOrDefault(x => x.Name == key);

            return propertyinfo;
        }

        public object ToObject(Document document, string className = "", string classNamespace = "")
        {
            var thisType = !string.IsNullOrEmpty(className) ? className : this.GetType().Name;
            var thisNamespace = !string.IsNullOrEmpty(classNamespace) ? classNamespace : this.GetType().Namespace;

            Type t = AppDomain.CurrentDomain.GetAssemblies()
                                .SelectMany(a => a.GetTypes())
                                .Where(qt => qt.Name == className)
                                .FirstOrDefault();

            var options = AppDomain.CurrentDomain.GetAssemblies()
                                .SelectMany(a => a.GetTypes())
                                .Where(qt => qt.Name == className).ToList();

            var classObject2 = t != null ? t : Activator.CreateInstance(this.GetType());
            
            var classObject = Activator.CreateInstance(this.GetType());
            var classPropertyInfo = classObject.GetType().GetProperties().FirstOrDefault(x => x.Name == className);
            
            // If className is a property. Use this class instead.
            // Might be a problem if Person class doesn't contain for instance Rank class, but the Job class does. 
            // Might need to deliver the subclass to next method call.
            if(classPropertyInfo != null)
            {
                var propertyType = classPropertyInfo.PropertyType;
                classObject = Activator.CreateInstance(propertyType);
            }
            string namespacetest = $"{thisNamespace}.{thisType}";

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
                        else if (propertyInfo.PropertyType == typeof(float))
                        {
                            // TODO: Needs to implemented
                        }
                        else if (propertyInfo.PropertyType == typeof(double))
                        {
                            // TODO: Needs to implemented
                        }
                        else if (propertyInfo.PropertyType == typeof(uint))
                        {
                            // TODO: Needs to implemented
                        }
                        else
                        {
                            Type valueType = entry.Value.GetType();



                            if (valueType == typeof(DynamoDBList))
                            {
                                var dynamoDbList = entry.Value as DynamoDBList;
                                var subType = propertyInfo.PropertyType.GetGenericArguments()[0];
                                var subClassName = subType.Name;

                                // Reflection way of working with list //
                                var genericList = typeof(List<>);
                                var stringList = genericList.MakeGenericType(subType);
                                var list = Activator.CreateInstance(stringList);

                                var addMethod = stringList.GetMethod("Add");

                                foreach (var dbEntry in dynamoDbList.Entries)
                                {
                                    var tmpDoc = dbEntry as Document;
                                    var subObject = this.ToObject(tmpDoc, subClassName);
                                    addMethod.Invoke(list, new object[] { subObject });
                                }

                                propertyInfo.SetValue(classObject, list);

                            }
                            else if (valueType == typeof(Document))
                            {
                                var tmpDoc = entry.Value as Document;
                                var subDocuement = this.ToObject(tmpDoc, propertyInfo.PropertyType.Name,
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
