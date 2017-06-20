using Microsoft.AspNetCore.Mvc;

namespace Imbd.Models
{
    public interface IMoviesRepository
    {
        ActionResult GetMovieInformation(long id);
        ActionResult GetMovieInformation(string title);
        ActionResult GetActorInformation(long id);
        ActionResult GetActorInformation(string name);
        ActionResult GetShortActorInformation(long id);
        ActionResult GetShortActorInformation(string name);
        ActionResult GetGenreInformation(string genreName, int? year);
    }
}
