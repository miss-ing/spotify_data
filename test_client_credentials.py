from dotenv import load_dotenv
import os
from pathlib import Path


env_file = Path(__file__).parent / "client_credentials_example.env"

load_dotenv(dotenv_path=env_file)


client_id = os.getenv('client_id')
client_secret = os.getenv("client_secret")

print(client_id, client_secret)