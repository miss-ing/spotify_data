from pathlib import Path
from source_code.load_sesh import sesh_jsons_to_pd, extract_track_uris

spotify_data_dir = Path(__file__).resolve().parent
sesh_dir = Path(spotify_data_dir)/"Spotify Extended Streaming History"


df = sesh_jsons_to_pd(sesh_dir)
df.info()

unique_track_uris= extract_track_uris(df)

print(len(unique_track_uris))

