# spotify_data

This project aims to create a pipeline turning a spotify user's downloaded Spotify Extended Streaming History into a database for analysis.


## intended workflow

1. turn SESH jsons into pandas dataframe
2. extract unique track_uris
3. use spotify web API to get track info including artist and album uris in jsons
4. extract unique artist and album uris
5. feed back into spotify web API to get artist and album info in json 
6. clean jsons into pandas data frames
7. create sql database from pandas data frames
8. analyse 
