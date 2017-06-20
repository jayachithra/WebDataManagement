using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Imbd.Models
{
    public class ShortActorInfo
    {
        public long Id { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public int MovieCount { get; set; }
    }
}
