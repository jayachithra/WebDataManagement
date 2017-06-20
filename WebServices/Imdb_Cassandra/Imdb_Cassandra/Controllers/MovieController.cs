using Cassandra;
using Imdb_Cassandra.Models;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Imdb_Cassandra.Controllers
{
    [Route("api/[controller]")]
    public class MovieController : IMovieRepository
    {
        [HttpGet("id/{Id}")]
        public ActionResult GetMovieInformation(long id)
        {
            // Connect to the demo keyspace on our cluster running at 127.0.0.1
            Cluster cluster = Cluster.Builder().AddContactPoint("127.0.0.1").Build();
            ISession session = cluster.Connect("imdb");
            string query = string.Format("SELECT id,title,year,location,language,actors_name FROM tbl_movies_by_id WHERE id={0}", id);
            Row result = session.Execute(query).First();

            IEnumerable<ActorForMovie> actorsForMovie = null;
            IEnumerable<string> r = result["actors_name"] as IEnumerable<string>;
            if(actorsForMovie != null && actorsForMovie.Count() > 0)
            {
                actorsForMovie = new List<ActorForMovie>();
                actorsForMovie = r.Select(a => new ActorForMovie
                {
                    FirstName = a
                });
            }
            FullMovie movie = new FullMovie
            {
                MovieInformation = new MovieInfo
                {
                    Id = result["id"] == null ? 0 : Int32.Parse(result["id"].ToString()),
                    Title = result["title"] == null ? null : result["title"].ToString(),
                    Year = result["year"] == null ? 0 : Int32.Parse(result["year"].ToString())
                },
                Actors = actorsForMovie
            };
            JsonResult json_result = new JsonResult(movie);
            return json_result;
        }

        [HttpGet("title/{Title}")]
        public ActionResult GetMovieInformation(string[] title)
        {
            // Connect to the demo keyspace on our cluster running at 127.0.0.1
            Cluster cluster = Cluster.Builder().AddContactPoint("127.0.0.1").Build();
            ISession session = cluster.Connect("imdb");
            string longestWord = title.OrderByDescending(s => s.Length).First();
            string query = string.Format("SELECT id,title,year,location,language,actors_name FROM tbl_movies_by_word WHERE word='{0}'", longestWord);
            List<Row> result = session.Execute(query).ToList();
            List<FullMovie> allMovies = new List<FullMovie>();

            foreach(Row row in result)
            {
                int count = 0;
                for (int i = 0; i < title.Length; i++)
                {
                    if (row["title"] != null && row["title"].ToString().ToLower().Contains(title[i].ToLower()))
                        count++;
                }
                if(count == title.Length)
                {
                    IEnumerable<ActorForMovie> actorsForMovie = null;
                    IEnumerable<string> r = row["actors_name"] as IEnumerable<string>;
                    if (actorsForMovie != null && actorsForMovie.Count() > 0)
                    {
                        actorsForMovie = new List<ActorForMovie>();
                        actorsForMovie = r.Select(a => new ActorForMovie
                        {
                            FirstName = a
                        });
                    }
                    FullMovie movie = new FullMovie
                    {
                        MovieInformation = new MovieInfo
                        {
                            Id = row["id"] == null ? 0 : Int32.Parse(row["id"].ToString()),
                            Title = row["title"] == null ? null : row["title"].ToString(),
                            Year = row["year"] == null ? 0 : Int32.Parse(row["year"].ToString())
                        },
                        Actors = actorsForMovie
                    };
                    allMovies.Add(movie);
                }               
            }
            JsonResult json_result = new JsonResult(allMovies);
            return json_result;
        }

        [HttpGet("faster/title/{Title}")]
        public ActionResult GetMovieInformationFaster(string[] title)
        {
            // Connect to the demo keyspace on our cluster running at 127.0.0.1
            Cluster cluster = Cluster.Builder().AddContactPoint("127.0.0.1").Build();
            ISession session = cluster.Connect("imdb");
         
            string query = string.Format("SELECT id,title,year,location,language,actors_name FROM tbl_movies_by_word WHERE word='{0}'", title);
            List<Row> result = session.Execute(query).ToList();
            List<FullMovie> allMovies = new List<FullMovie>();

            foreach (Row row in result)
            {
                    IEnumerable<ActorForMovie> actorsForMovie = null;
                    IEnumerable<string> r = row["actors_name"] as IEnumerable<string>;
                    if (actorsForMovie != null && actorsForMovie.Count() > 0)
                    {
                        actorsForMovie = new List<ActorForMovie>();
                        actorsForMovie = r.Select(a => new ActorForMovie
                        {
                            FirstName = a
                        });
                    }
                    FullMovie movie = new FullMovie
                    {
                        MovieInformation = new MovieInfo
                        {
                            Id = row["id"] == null ? 0 : Int32.Parse(row["id"].ToString()),
                            Title = row["title"] == null ? null : row["title"].ToString(),
                            Year = row["year"] == null ? 0 : Int32.Parse(row["year"].ToString())
                        },
                        Actors = actorsForMovie
                    };
                    allMovies.Add(movie);
            }
            JsonResult json_result = new JsonResult(allMovies);
            return json_result;
        }

        [HttpGet("actor/id/{Id}")]
        public ActionResult GetActorInformation(long id)
        {
            Cluster cluster = Cluster.Builder().AddContactPoint("127.0.0.1").Build();
            ISession session = cluster.Connect("imdb");

            string query = string.Format("SELECT id, aka, fname, lname, mname, gender, movies_title FROM tbl_actors_by_id WHERE id = {0}", id);
            Row result = session.Execute(query).First();
            IEnumerable<string> movies = result["movies_title"] as IEnumerable<string>;
            IEnumerable<MovieInfo> moviesInfo = new List<MovieInfo>();
            moviesInfo = movies.Select(m => new MovieInfo()
            {
                Title = m
            });
            FullActor actor = new FullActor
            {
                ActorDetails = new ActorInfo
                {
                    Id = Int32.Parse(result["id"].ToString()),
                    FirstName = result["fname"] == null ? null : result["fname"].ToString(),
                    LastName = result["lname"] == null ? null : result["lname"].ToString(),
                    Gender = result["gender"] == null ? null : result["gender"].ToString()
                },
                MovieDetails = moviesInfo
            };
            JsonResult json_result = new JsonResult(actor);
            return json_result;
        }

        [HttpGet("actor/name/{Name}")]
        public ActionResult GetActorInformation(string name)
        {
            Cluster cluster = Cluster.Builder().AddContactPoint("127.0.0.1").Build();
            ISession session = cluster.Connect("imdb");

            string query = string.Format("SELECT id, aka, fname, lname, mname, gender, movies_title FROM tbl_actors_by_name WHERE name = '{0}'", name);
            Row result = session.Execute(query).First();
            IEnumerable<string> movies = result["movies_title"] as IEnumerable<string>;
            IEnumerable<MovieInfo> moviesInfo = new List<MovieInfo>();
            moviesInfo = movies.Select(m => new MovieInfo()
            {
                Title = m
            });

            FullActor actor = new FullActor
            {
                ActorDetails = new ActorInfo
                {
                    Id = Int32.Parse(result["id"].ToString()),
                    FirstName = result["fname"] == null ? null : result["fname"].ToString(),
                    LastName = result["lname"] == null ? null : result["lname"].ToString(),
                    Gender = result["gender"] == null ? null : result["gender"].ToString()
                },
                MovieDetails = moviesInfo
            };
            JsonResult json_result = new JsonResult(actor);
            return json_result;
        }

        [HttpGet("genre")]
        public ActionResult GetGenreInformation(string name, int? beginYear, int? endYear)
        {
            Cluster cluster = Cluster.Builder().AddContactPoint("127.0.0.1").Build();
            ISession session = cluster.Connect("imdb");
            string query = null;
            if (beginYear == null && endYear == null)
            {
                query = string.Format("SELECT * FROM tbl_genres_by_name_and_year WHERE genre='{0}'", name);
            }
            else if (beginYear != null)
            {
                if(endYear == null)
                    query = string.Format("SELECT * FROM tbl_genres_by_name_and_year WHERE genre='{0}' AND year={1}", name, beginYear);
                else
                    query = string.Format("select * from tbl_genres_by_name_and_year where year >= {0} and year <= {1} allow filtering;", beginYear, endYear);
            }
            List<Row> result = session.Execute(query).ToList();
            
            JsonResult json_result = new JsonResult(result);
            return json_result;
        }
    }
}
