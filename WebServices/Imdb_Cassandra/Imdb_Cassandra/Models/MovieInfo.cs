using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Imdb_Cassandra.Models
{
    public class MovieInfo
    {
        public long Id { get; set; }
        public string Title { get; set; }
        public int Year { get; set; }
    }
}
