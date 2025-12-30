from pathlib import Path
import pandas as pd
from spot.web_api import get_access_token, get_client_credentials, get_several_json
import os 

#FILE INPUT
spotify_dir = Path(os.getcwd())
sesh_dir = Path(spotify_dir)/"data"/"Spotify Extended Streaming History"
env_path = Path(spotify_dir )/ "my_client_credentials.env"
output_dir = Path(spotify_dir)/"data"/"api_data"
track_json_path = Path(output_dir)/"track_uri_info.json"


#load track_info json into dataframe
track_info = pd.read_json(track_json_path)


#extract_unique_uris
unique_album_uris = track_info['album'].str['uri'].unique()
print(len(unique_album_uris))

all_artist_uris = track_info['artists'].apply(lambda x: [artist['uri'] for artist in x])
all_artist_uris = all_artist_uris.explode()
unique_artist_uris = all_artist_uris.unique()
print(len(unique_artist_uris))


client_credentials = get_client_credentials(env_path )

access_token = (get_access_token(client_credentials))

get_several_json(unique_artist_uris, access_token, output_dir)
get_several_json(unique_album_uris, access_token, output_dir)

