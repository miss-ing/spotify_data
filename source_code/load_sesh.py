import os
from pathlib import Path
import pandas as pd



def sesh_jsons_to_pd(sesh_dir):
    
    """
    Load all streaming history audio json files into one pandas DataFrame 
    DataFrame must have streams in chronological order 
    Renames columns 

    Parameters:
    sesh_dir : str
        Path to the Spotify Extended Streaming History folder

    Returns:
    pandas.DataFrame
        combined audio file streams
        
    """

    if not sesh_dir.exists():
        raise FileNotFoundError(f"Spotify Extended Streaming History not found: {sesh_dir}")
    
    # get list of audio json paths
    files = os.listdir(sesh_dir) 
    audio_files = [f for f in files if f.startswith("Streaming_History_Audio")]
    audio_files.sort(key=lambda x: int(x.rsplit('_', 1)[1].split('.')[0]))
    audio_file_path = [Path(sesh_dir)/file for file in audio_files]
        
    #combine all json files into a pandas dataframe
    df = pd.concat(pd.read_json(path) for path in audio_file_path)

    #check jsons have been combiend in the correct order and datetime is ascending
    df['ts']= pd.to_datetime(df['ts'])

    if (df['ts'].is_monotonic_increasing) == False:
        raise ValueError('Streaming history not in chronological order')
    
    df.rename(columns = {
    'ts':'timestamp',
    'master_metadata_track_name':'track_name',
    'master_metadata_album_artist_name':'artist_name',
    'master_metadata_album_album_name':'album_name',
    'spotify_track_uri':'track_uri',
    'spotify_episode_uri':'episode_uri'
    },inplace=True)

    
    return df 

def sesh_only_track_df(df):
    ''' 
    Remove duplictes and non track streams from raw data frame

    Parameters:
    df : pandas.DataFrame
        raw dataframe
    
    Returns:
    pandas.DataFrame
        cleaned dataframe
    '''

    #remove non track streams
    df = df[df['track_uri'].notna()]
    df = df.dropna(axis=1, how='all')

    #ignore offline timestamp when removing duplicates as contains 
    non_null_cols = list(df.columns)
    non_null_cols.remove('offline_timestamp')
    
    if df[non_null_cols].isnull().values.any():
        raise ValueError("other columns contains null values")

    df.drop_duplicates(subset= non_null_cols, inplace=True)

    return df 


def extract_track_uris(df):
    '''
    Extracts unique uris from track_uri column

    Parameter:
    pandas.DataFrame
        Spotify Extended Streaming History 

    Returns:
    1D-array
        unique track_uris 
    '''
    if 'track_uri' not in df.columns:
        raise KeyError('track_uri column not found')
    
    df = df[df['track_uri'].notna()]

    unique_track_uris = df['track_uri'].unique()

    return unique_track_uris

    






#df['track_name'] = df['track_name'].astype(str).str.strip().str.lower()
#df['artist_name']= df['artist_name'].astype(str).str.strip().str.lower()
#df['album_name']= df['album_name'].astype(str).str.strip().str.lower()





