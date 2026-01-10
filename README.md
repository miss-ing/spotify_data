# spotify_data

This project aims to design a database for a spotify user's track streams and create a pipeline using the spotify user's downloaded Spotify Extended Streaming History (SESH) along with Spotify's Web API. My personal SESH is also queried and visualised. Pandas was used for reading and converting json files and cleaning of data. Python was used for Web API requests. SQL Alchemy was used to convert the cleaned dataframes into a .db file. 


Introduction:

Spotify users can request their Spotify Extended Streaming History from Spotify which lists every stream as a json object in json files. For context, my SESH included 54 files. For more information on the layout of the json files visit https://support.spotify.com/uk/article/understanding-your-data/. Each object has a value for the URI of the content streamed. A URI is the unique reference identifier which has the content type and ID of the content eg. spotify:track:6rqhFgbbKwnb9MLmUQDhG6 for more info visit https://developer.spotify.com/documentation/web-api/concepts/spotify-uris-ids

Some of the most valuable data here is the track URI, track name, album name and artist name. One issue found whilst exploring the data is that the track name, album name and artist name are not unique and multiple variations can be found for each track URI. This is presumerably due to the values being updated and the new values are presented in the object for example an artist that has changed their Spotify name will show the old name in earlier streams. Another could be multiple tracks released slightly differently leading to the same track URI. Therefore Spotify's Web API was used to gather current information on each track by feeding it the track URIs. This gives access to the track's current name and also the track's album and artist URI and ID which are unique and unchanged. Feeding these album and artist URIs to the API also returns more information. 


## Workflow

1. turn SESH jsons into pandas dataframe
2. extract unique track_uris
3. use spotify web API to get track info including artist and album uris in jsons
4. extract unique artist and album uris
5. feed back into spotify web API to get artist and album info in json 
6. clean jsons into pandas data frames
7. create sql database from pandas data frames
8. analyse 


