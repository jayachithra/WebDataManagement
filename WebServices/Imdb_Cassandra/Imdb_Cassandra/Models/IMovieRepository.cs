using Microsoft.AspNetCore.Mvc;

namespace Imdb_Cassandra.Models
{
    public interface IMovieRepository
    {
        ActionResult GetMovieInformation(long id);
        ActionResult GetMovieInformationFaster(string[] title);
        ActionResult GetMovieInformation(string[] title);
        ActionResult GetActorInformation(long id);
        ActionResult GetActorInformation(string title);
        ActionResult GetGenreInformation(string name, int? beginYear, int? endYear);
    }
}
