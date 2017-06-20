using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Imdb_Cassandra.Models
{
    public class ActorForMovie
    {
        public long Id { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string Gender { get; set; }
        public string CharacterName { get; set; }
    }
}
