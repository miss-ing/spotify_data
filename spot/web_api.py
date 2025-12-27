import os
import requests
import json
import time
import numpy as np
from pathlib import Path
from dotenv import load_dotenv
from pathlib import Path


def get_client_credentials(env_path : Path) -> dict:
    '''
    Gets client credentials from .env file 
    
    :param env_path: path to .env file with client_id and client_secret for Spotify Web API app
    :type env_path: Path
    :return: dictionary with client_id and client_secret keys
    :rtype: dict
    '''

    load_dotenv(dotenv_path=env_path)

    client_id = os.getenv('client_id')
    client_secret = os.getenv("client_secret")

    return {'client_id':client_id, 'client_secret' : client_secret}


def get_access_token(client_credentials : dict) -> str:
    '''
    Generates access code for Spotify Web API

    :param client_credentials: client_id and client_secret for Spotify Web API app
    :type client_credentials: dict
    :return: access token valid for 1 hour
    :rtype: str
    '''

    #get client credentials 
    client_id = client_credentials['client_id']
    client_secret = client_credentials['client_secret']

    
    
    #send post request 
    access_url = "https://accounts.spotify.com/api/token"
    access_h = {"Content-Type": "application/x-www-form-urlencoded"}
    access_d = {"grant_type":"client_credentials",
            "client_id":client_id,
            "client_secret":client_secret}

    access_response = requests.post(url = access_url, headers = access_h, data = access_d)

    #get access token from json object
    try:
        access_response.raise_for_status()
        access_token = access_response.json()["access_token"]
        return access_token
    except requests.exceptions.HTTPError as e:
        raise requests.exceptions.HTTPError(f"HTTP error occurred: {e}") from e


def get_several_json(unique_uris : np.ndarray, access_token : str, output_path: Path) -> Path:
    '''
    Sends batch requests for track, album or artist information to Spotify Web API
    
    :param unique_uris: array of all unique uris 
    :type unique_uris: np.ndarray
    :param access_token: access token for Spotify Web API
    :type access_token: str
    :param output_path: json output file location
    :type output_path: Path
    :return: path to new json file
    :rtype: Path
    '''
    #get id type and list of ids 
    id_type = unique_uris[0].split(':')[1]
    unique_uris = unique_uris.astype(str)
    unique_ids = np.array([s.rsplit(":", 1)[-1] for s in unique_uris.astype(str)])

    #split ids into batches with maximum size specified by Spotify Web API
    if id_type == 'album':
        batch_size = 20 
    else:
        batch_size = 50

    num_batches = int(np.ceil(len(unique_ids) / batch_size))
    batches = np.array_split(unique_ids, num_batches)


    #send batch get requests 
    get_url = f"https://api.spotify.com/v1/{id_type}s?"
    get_headers = {"Authorization": f"Bearer {access_token}"}
    all_info = []
    i=0

    for batch in batches:
        batch = ','.join(list(batch))
        get_params = {"ids": batch}
        info_response= requests.get(url = get_url, params = get_params, headers = get_headers)
        i+=1
        #extract info for each uri and add to all_info
        try:
            info_response.raise_for_status()
            info = info_response.json()[f"{id_type}s"]
            all_info.extend(info)
        except requests.exceptions.HTTPError as e:
            print("Request failed:", e, 'batch number: ',i)
        #sleep to not exceed rate limits 
        time.sleep(0.2)

    
    #create pretty printed json file of all_info
    output_file = f"{id_type}_uri_info.json"
    output_file_path = Path(output_path)/output_file
  
    with open((output_file_path), "w", encoding="utf-8") as f: 
        json.dump(all_info, f, indent= 2, ensure_ascii=False) 

    print(f"Saved info for {len(all_info)} {id_type}s in {output_file}")

    return output_file_path
