using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Imbd.Models
{
    public class FullActor
    {
        public ActorInfo ActorDetails { get; set; }
        public IEnumerable<MovieInfo> MovieDetails { get; set; }
    }
}
