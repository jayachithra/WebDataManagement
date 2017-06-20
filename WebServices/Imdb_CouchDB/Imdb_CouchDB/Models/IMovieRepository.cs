using Microsoft.AspNetCore.Mvc;
using System.Threading.Tasks;

namespace Imdb_CouchDB.Models
{
    public interface IMovieRepository
    {
        Task<ActionResult> GetGenreInformationAsync(string name, long year);
        Task<ActionResult> GetGenreInformationAsync(string name, int beginYear, int endYear);
        Task<ActionResult> GetActorInformationAsync(long id, string name);
        Task<ActionResult> GetMovieInformationAsync(long id);
        Task<ActionResult> GetMovieInformationAsync(string title);
        Task<ActionResult> GetMovieInformationFromPartialTitleAsync(string partialTitle);
    }
}
