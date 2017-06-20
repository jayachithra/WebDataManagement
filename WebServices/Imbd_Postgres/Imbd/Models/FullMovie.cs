using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Imbd.Models
{
    public class FullMovie
    {
        public MovieInfo MovieInformation { get; set; }
        public IEnumerable<GenreInfo> Genres { get; set; }
        public IEnumerable<KeywordsInfo> Keywords { get; set; }
        public IEnumerable<ActorForMovie> Actors { get; set; }
    }
}
