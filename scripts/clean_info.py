from pathlib import Path
import pandas as pd
import os
from spot.clean_data import dictcolumn, many_to_many

spotify_dir = Path(os.getcwd())
sesh_dir = Path(spotify_dir)/"data"/"Spotify Extended Streaming History"
env_path = Path(spotify_dir )/ "my_client_credentials.env"
output_dir = Path(spotify_dir)/"data"/"api_data"



#----------TRACK TABLES -----------

track_info = pd.read_json(output_dir/"track_uri_info.json")



#create M:N track:artist df
tracks_artists = many_to_many(track_info,column = 'artists', key='id')
tracks_artists.rename(columns={
    'id':'track_id',
    'artists':'artist_id'
}, inplace = True)


#create tracks table 
track_info = track_info[['id','name','album','artists','duration_ms','explicit','disc_number','track_number','external_ids']]
track_info.rename(columns = {
    'id':'track_id', 
    'album': 'album_id', 
    'name':'track_name', 
    'artists':'primary_artist_id',
    'external_ids':'isrc_id'
},inplace= True)
#extract ids from objects
track_info['primary_artist_id'] = track_info['primary_artist_id'].apply(lambda x: x[0]['id'])
track_info['album_id'] = track_info['album_id'].str['id']
#turn external_id into isrc if there is one 
track_info['isrc_id'] =dictcolumn(track_info['isrc_id'],'isrc')


#----------ARTIST TABLES -----------
artist_info = pd.read_json(output_dir/"artist_uri_info.json")

#create artists_genres table
artists_genres = many_to_many(artist_info,column = 'genres')
artists_genres.rename(columns = {
    'id':'artist_id',
    'genres':'genre'
},inplace=True)

#create artists table
artist_info = artist_info[['id','name','followers','popularity']]
artist_info.rename(columns={
    'id':'artist_id',
    'name':'artist_name',
},inplace = True)
#extract total followers from followers column 
artist_info['followers'] = dictcolumn(artist_info['followers'], 'total')




#----------ALBUM TABLES -----------
album_info = pd.read_json(output_dir/"album_uri_info.json")

#create M:N album:artist df
albums_artists = many_to_many(album_info,column = 'artists', key='id')
albums_artists=albums_artists.rename(columns={
    'id':'album_id',
    'artists':'artist_id'})

#create albums tracks
album_info = album_info[['id','name','artists','release_date','external_ids','label','popularity']]
album_info.rename(columns ={
    'id':'album_id',
    'name':'album_name',
    'artists':'primary_artist_id',
    'release_date':'release_year',
    'external_ids':'upc_id'
},inplace= True)
album_info['primary_artist_id'] = album_info['primary_artist_id'].apply(lambda x: x[0]['id'])
#turn external_id into upc if there is one 
album_info['upc_id'] =dictcolumn(album_info['upc_id'],'upc')
#extract year from release date
album_info['release_year'] = album_info['release_year'].apply(lambda date: date.split('-')[0])


