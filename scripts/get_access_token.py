from spot.web_api import get_client_credentials


print(get_client_credentials("my_client_credentials.env"))
access_token = (get_access_token(client_credentials))
print(access_token)