from pathlib import Path
import pandas as pd
import os
from spot.clean_data import dictcolumn, many_to_many
from spot.load_sesh import sesh_jsons_to_pd 
from spot.create_db import get_engine
from spot.create_tables import metadata, tracks, albums, artists, tracks_artists, albums_artists, artists_genres, streams
 
from sqlalchemy import insert 

spotify_dir = Path(os.getcwd())
sesh_dir = Path(spotify_dir)/"data"/"Spotify Extended Streaming History"
env_path = Path(spotify_dir )/ "my_client_credentials.env"
output_dir = Path(spotify_dir)/"data"/"api_data"



#----------TRACK TABLES -----------

track_info = pd.read_json(output_dir/"track_uri_info.json")



#create M:N track:artist df
tracks_artists_df = many_to_many(track_info,column = 'artists', key='id')
tracks_artists_df.rename(columns={
    'id':'track_id',
    'artists':'artist_id'
}, inplace = True)


#create tracks table 
track_df = track_info[['id','name','album','artists','duration_ms','explicit','disc_number','track_number','external_ids']].copy()
track_df.rename(columns = {
    'id':'track_id', 
    'album': 'album_id', 
    'name':'track_name', 
    'artists':'primary_artist_id',
    'external_ids':'isrc_id'
},inplace= True)
#extract ids from objects
track_df['primary_artist_id'] = track_df['primary_artist_id'].apply(lambda x: x[0]['id'])
track_df['album_id'] = track_df['album_id'].str['id']
#turn external_id into isrc if there is one 
track_df['isrc_id'] =dictcolumn(track_df['isrc_id'],'isrc')


#----------ARTIST TABLES -----------
artist_info = pd.read_json(output_dir/"artist_uri_info.json")

#create artists_genres table
artists_genres_df = many_to_many(artist_info,column = 'genres')
artists_genres_df.rename(columns = {
    'id':'artist_id',
    'genres':'genre'
},inplace=True)

#create artists table
artist_df = artist_info[['id','name','followers','popularity']].copy()
artist_df.rename(columns={
    'id':'artist_id',
    'name':'artist_name',
},inplace = True)
#extract total followers from followers column 
artist_df['followers'] = dictcolumn(artist_df['followers'], 'total')




#----------ALBUM TABLES -----------
album_info = pd.read_json(output_dir/"album_uri_info.json")

#create M:N album:artist df
albums_artists_df = many_to_many(album_info,column = 'artists', key='id')
albums_artists_df.rename(columns={
    'id':'album_id',
    'artists':'artist_id'}, inplace = True)

#create albums tracks
album_df = album_info[['id','name','artists','release_date','external_ids','label','popularity']].copy()
album_df.rename(columns ={
    'id':'album_id',
    'name':'album_name',
    'artists':'primary_artist_id',
    'release_date':'release_year',
    'external_ids':'upc_id'
},inplace= True)
album_df['primary_artist_id'] = album_df['primary_artist_id'].apply(lambda x: x[0]['id'])
#turn external_id into upc if there is one 
album_df['upc_id'] =dictcolumn(album_df['upc_id'],'upc')
#extract year from release date
album_df['release_year'] = album_df['release_year'].apply(lambda date: date.split('-')[0]).astype(int)





#------STREAMS-------

stream_df = sesh_jsons_to_pd(sesh_dir)

stream_df = stream_df[ stream_df['track_uri'].notna()]

stream_df['track_id'] = stream_df['track_uri'].apply(lambda uri: uri.split(':')[-1])


#check for multiple connection countries per ip address 
ip_multiple= (stream_df.groupby('ip_addr', sort = False)['conn_country'].unique()).reset_index(name='countries')
ip_multiple['count'] = ip_multiple['countries'].apply(len)
ip_multiple = ip_multiple[ip_multiple['count']>1]

#check that the only issue is unknown country for some ip addresses 
ip_multiple['country'] = ip_multiple['countries'].apply(lambda arr: (arr[arr != 'ZZ']))
if (ip_multiple['country'].apply(len) == 1).all() == False:
    raise Exception('contradicting ip address country')

#create map and replace stream_df with correct countries 
ip_multiple['country'] = ip_multiple['country'].str[0]
ip_map = dict(zip(ip_multiple['ip_addr'], (ip_multiple['country'])))
stream_df["conn_country"] = stream_df["ip_addr"].map(ip_map).fillna(stream_df["conn_country"])


stream_df = stream_df[['timestamp','track_id','ms_played','reason_start','reason_end','shuffle','skipped','incognito_mode','ip_addr', 'conn_country','platform']] # ,'offline_timestamp'

stream_df['timestamp'] = pd.to_datetime(stream_df['timestamp'])

stream_df.insert(1,'date',stream_df['timestamp'].dt.date)
stream_df.insert(2,'time',stream_df['timestamp'].dt.time)
stream_df.insert(3,'day_of_week',stream_df['timestamp'].dt.day_name())

del stream_df['timestamp']

stream_df.insert(0, 'stream_id', stream_df.index +1)
stream_df.rename(columns = {
    'ip_addr':'ip_address',
    'conn_country':'country'}, inplace = True)





# ----- 

output_dir= Path(spotify_dir)/"data"/'db'/"spotify.db"
engine = get_engine(output_dir) 

with engine.begin() as conn: 
    metadata.drop_all(conn)
    metadata.create_all(conn)
    
    conn.execute(insert(tracks), track_df.to_dict(orient="records"))
    conn.execute(insert(albums), album_df.to_dict(orient="records"))
    conn.execute(insert(artists), artist_df.to_dict(orient="records"))
    conn.execute(insert(tracks_artists), tracks_artists_df.to_dict(orient="records"))
    conn.execute(insert(albums_artists), albums_artists_df.to_dict(orient="records"))
    conn.execute(insert(artists_genres), artists_genres_df.to_dict(orient="records"))
    conn.execute(insert(streams), stream_df.to_dict(orient="records"))
    
    