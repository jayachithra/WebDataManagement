using Dapper;
using Imbd.Models;
using Microsoft.AspNetCore.Mvc;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
 
namespace Imbd.Controllers
{
    [Route("api/[controller]")]
    public class MovieController : IMoviesRepository
    {
        #region Private Methods
                private static NpgsqlConnection OpenConnection()
                {
                    // PostgeSQL-style connection string
                    string connstring = System.String.Format("Server={0};Port={1};" +
                        "User Id={2};Password={3};Database={4};",
                        "127.0.0.1", "5432", "postgres",
                        "root", "imdb_dump");
                    // Making connection with Npgsql provider
                    NpgsqlConnection conn = new NpgsqlConnection(connstring);
                    conn.Open();
                    return conn;
                }
        #endregion

        #region Public Methods

        #region Movie Related

        [HttpGet("id/{Id}")]
        public ActionResult GetMovieInformation(long id)
        {
            NpgsqlConnection conn = OpenConnection();

            string sql_movie = string.Format("SELECT idmovies as Id, title, year FROM movies where idmovies = {0}", id);
            var movieInfo = conn.Query<MovieInfo>(sql_movie).ToList().FirstOrDefault();

            string sql_genres = string.Format("SELECT g.idgenres as Id, g.genre as Genre from public.genres g where g.idgenres in (select mg.idgenres from public.movies_genres mg where mg.idmovies = {0})", id);
            var genreInfo = conn.Query<GenreInfo>(sql_genres).ToList();

            string sql_keywords = string.Format(@"select k.idkeywords as Id, k.keyword from public.keywords k
                                                  where k.idkeywords in (select mk.idkeywords from public.movies_keywords mk where mk.idmovies = {0})", id);
            var keywordsInfo = conn.Query<KeywordsInfo>(sql_keywords).ToList();

            string sql_actors = string.Format(@"select a.idactors as Id, a.fname as FirstName, a.lname as LastName, a.gender, aka.name as CharacterName from public.actors a, public.aka_names aka
                                                where a.idactors in (select ai.idactors from public.acted_in ai where ai.idmovies = {0})
                                                and a.idactors  = aka.idactors", id);
            var actorInfo = conn.Query<ActorForMovie>(sql_actors);
            FullMovie fullMovieInformation = new FullMovie()
            {
                MovieInformation = movieInfo,
                Genres = genreInfo,
                Keywords = keywordsInfo,
                Actors = actorInfo
            };
            JsonResult result = new JsonResult(fullMovieInformation);
            conn.Close();
            return result;
        }

        [HttpGet("title/{title}")]
        public ActionResult GetMovieInformation(string title)
        {
            List<FullMovie> moviesForTitle = new List<FullMovie>();
            NpgsqlConnection conn = OpenConnection();

            string sql_movie = string.Format("SELECT idmovies as Id, title, year FROM movies where title like \'% {0} %\' ", title);
            var movieInfo = conn.Query<MovieInfo>(sql_movie).ToList();
            foreach(MovieInfo movie in movieInfo) {
                string sql_genres = string.Format("SELECT g.idgenres as Id, g.genre as Genre from public.genres g where g.idgenres in (select mg.idgenres from public.movies_genres mg where mg.idmovies = {0})", movie.Id);
                var genreInfo = conn.Query<GenreInfo>(sql_genres).ToList();

                string sql_keywords = string.Format(@"select k.idkeywords as Id, k.keyword from public.keywords k
                                                  where k.idkeywords in (select mk.idkeywords from public.movies_keywords mk where mk.idmovies = {0})", movie.Id);
                var keywordsInfo = conn.Query<KeywordsInfo>(sql_keywords).ToList();

                string sql_actors = string.Format(@"select a.idactors as Id, a.fname as FirstName, a.lname as LastName, a.gender, aka.name as CharacterName from public.actors a, public.aka_names aka
                                                where a.idactors in (select ai.idactors from public.acted_in ai where ai.idmovies = {0})
                                                and a.idactors  = aka.idactors", movie.Id);
                var actorInfo = conn.Query<ActorForMovie>(sql_actors).ToList();
                FullMovie fullMovieInformation = new FullMovie()
                {
                    MovieInformation = movie,
                    Genres = genreInfo,
                    Keywords = keywordsInfo,
                    Actors = actorInfo
                };
                moviesForTitle.Add(fullMovieInformation);
            }
            JsonResult result = new JsonResult(moviesForTitle);
            conn.Close();
            return result;
        }

        #endregion

        #region Actor Related

        [HttpGet("actor/id/{Id}")]
        public ActionResult GetActorInformation(long id)
        {
            NpgsqlConnection conn = OpenConnection();
            string sql_actor = string.Format("SELECT idactors as Id, fname as FirstName, lname as LastName, gender from public.actors where idactors = {0}", id);
            var actorInfo = conn.Query<ActorInfo>(sql_actor).ToList().FirstOrDefault();

            string sql_movies = string.Format(@"select m.idmovies as id, m.title, m.year  
                                                from public.movies m where m.idmovies in 
                                                (select ai.idmovies from acted_in ai where ai.idactors  = {0})", id);
            var moviesInfo = conn.Query<MovieInfo>(sql_movies).ToList();

            FullActor actor = new FullActor()
            {
                ActorDetails = actorInfo,
                MovieDetails = moviesInfo
            };
            JsonResult result = new JsonResult(actor);
            conn.Close();
            return result;
        }

        [HttpGet("actor/name/{name}")]
        public ActionResult GetActorInformation(string name)
        {
            List<FullActor> allActorsMatchingName = new List<FullActor>();
            NpgsqlConnection conn = OpenConnection();
            string sql_actor = string.Format(@"SELECT idactors as Id, fname as FirstName, lname as LastName, gender 
                                                from public.actors where (fname ~* '{0}' or lname ~* '{0}')", name);
            var actorInfo = conn.Query<ActorInfo>(sql_actor).ToList();
            foreach (ActorInfo actor in actorInfo)
            {
                string sql_movies = string.Format(@"select m.idmovies as id, m.title, m.year  
                                                from public.movies m where m.idmovies in 
                                                (select ai.idmovies from acted_in ai where ai.idactors  = {0})", actor.Id);
                var moviesInfo = conn.Query<MovieInfo>(sql_movies).ToList();

                FullActor fullActor = new FullActor()
                {
                    ActorDetails = actor,
                    MovieDetails = moviesInfo
                };
                allActorsMatchingName.Add(fullActor);
            }
            
            JsonResult result = new JsonResult(allActorsMatchingName);
            conn.Close();
            return result;
        }

        [HttpGet("shortactor/id/{Id}")]
        public ActionResult GetShortActorInformation(long id)
        {
            NpgsqlConnection conn = OpenConnection();
            string sql_actor = string.Format(@" SELECT a.idactors as Id, a.fname as FirstName, a.lname as LastName, p.moviesCount
                                                from public.actors a, 
 	                                                 (select count(m.idmovies) as moviesCount 
                                                      from public.movies m where m.idmovies in 
                                                            (select ai.idmovies from acted_in ai where ai.idactors  = 1) 
                                                     ) p
                                                where a.idactors = 1 ");
            var actorInfo = conn.Query<ShortActorInfo>(sql_actor).ToList().FirstOrDefault();
            JsonResult result = new JsonResult(actorInfo);
            conn.Close();
            return result;
        }

        [HttpGet("shortactor/name/{name}")]
        public ActionResult GetShortActorInformation(string name)
        {
            List<ShortActorInfo> allActorsMatchingName = new List<ShortActorInfo>();
            NpgsqlConnection conn = OpenConnection();
            string sql_actor = string.Format(@"SELECT idactors as Id, fname as FirstName, lname as LastName
                                                from public.actors where (fname ~* '{0}' or lname ~* '{0}')", name);
            var actorInfo = conn.Query<ActorInfo>(sql_actor).ToList();
            foreach (ActorInfo actor in actorInfo)
            {
                string sql_movies = string.Format(@"select count(m.idmovies) as moviesCount  
                                                from public.movies m where m.idmovies in 
                                                (select ai.idmovies from acted_in ai where ai.idactors  = {0})", actor.Id);
                var moviesInfo = (IDictionary<string, object>)conn.Query(sql_movies).ToList().FirstOrDefault();

                ShortActorInfo shortActor = new ShortActorInfo()
                {
                    Id = actor.Id,
                    FirstName = actor.FirstName,
                    LastName = actor.LastName,
                    MovieCount = Int32.Parse(moviesInfo["moviescount"].ToString())
                };
                allActorsMatchingName.Add(shortActor);
            }

            JsonResult result = new JsonResult(allActorsMatchingName);
            conn.Close();
            return result;
        }

        #endregion

        #region Genre Related

        [HttpGet("genre")]
        public ActionResult GetGenreInformation(string name, int? year)
        {
            NpgsqlConnection conn = OpenConnection();
            string sql_genre = null;
            if (year != null)
            {
                sql_genre = string.Format(@"select distinct(m.idmovies) as Id, m.title as Title, m.year as Year
                                               from public.movies m , public.genres g 
                                               where m.idmovies in 
	                                                    (select distinct(mg.idmovies) from movies_genres mg 
                                                         where mg.idgenres  = (select gg.idgenres from genres gg where gg.genre ~* '{0}'))
                                               and m.year = {1}
                                               LIMIT 100", name, year);
            }
            else
            {
                sql_genre = string.Format(@"select distinct(m.idmovies) as Id, m.title as Title, m.year as Year
                                               from public.movies m , public.genres g 
                                               where m.idmovies in 
	                                                    (select distinct(mg.idmovies) from movies_genres mg 
                                                         where mg.idgenres  = (select gg.idgenres from genres gg where gg.genre ~* '{0}'))
                                               LIMIT 100", name);
            }
            var movieInfoForGenre = conn.Query<MovieInfo>(sql_genre).ToList();
            JsonResult result = new JsonResult(movieInfoForGenre);
            conn.Close();
            return result;
        }

        #endregion  

        #endregion
    }
}
