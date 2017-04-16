using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AWSDynamoDBFramework
{
    public class AWSEventRecord<T>
    {
        public string EventID { get; set; }
        public string EventName { get; set; }
        public string EventSource { get; set; }
        public string EventVersion { get; set; }
        public string SequenceNumber { get; set; }
        public T NewImage { get; set; }
        public T OldImage { get; set; }

        public AWSEventRecord() { }
    }
}
