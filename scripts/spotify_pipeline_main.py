from spot.load_sesh import sesh_jsons_to_pd, extract_track_uris
from spot.web_api import get_client_credentials, get_access_token, get_several_json
from pathlib import Path

#FILE INPUTS:
spotify_data_dir = Path(__file__).resolve().parent
sesh_dir = Path(spotify_data_dir)/"Spotify Extended Streaming History"
env_path = Path(spotify_data_dir )/ "my_client_credentials.env"
output_dir = Path(spotify_data_dir)/"intermediate_data"


df = sesh_jsons_to_pd(sesh_dir)
unique_track_uris= extract_track_uris(df)

# web_api 
client_credentials = get_client_credentials(env_path )

access_token = (get_access_token(client_credentials))


get_several_json(unique_track_uris, access_token, output_dir)

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

