# Databricks notebook source
# MAGIC %pip install spotipy

# COMMAND ----------

#The below code is to set up Spotipy for querying the API endpoint
import spotipy, requests
from spotipy.oauth2 import SpotifyClientCredentials

cid ="78ef2c1930e347f2a527ea4e2f08b6e9" 
secret = "0510098790d149e8af6b880782f6f19b"

#client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret)
#sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)


AUTH_URL = 'https://accounts.spotify.com/api/token'

# POST
auth_response = requests.post(AUTH_URL, {
    'grant_type': 'client_credentials',
    'client_id': cid,
    'client_secret': secret,
})

# convert the response to JSON
auth_response_data = auth_response.json()

# save the access token
access_token = auth_response_data['access_token']

headers = {
    'Authorization': 'Bearer {token}'.format(token=access_token)
}

# COMMAND ----------

# timeit library to measure the time needed to run this code
import timeit
start = timeit.default_timer()

# create empty lists where the results are going to be stored
artist_name = []
track_name = []
popularity = []
track_id = []
base_url = "https://api.spotify.com/v1/search"
for i in range(0,100,50):
    track_results = requests.get(base_url, headers=headers, params={'q': 'year:2018', 'type': 'track', 'limit': 50, 'offset': i})
    track_results = track_results.json()
    print(track_results['tracks']['items'])
    for i, t in enumerate(track_results['tracks']['items']):
        artist_name.append(t['artists'][0]['name'])
        track_name.append(t['name'])
        track_id.append(t['id'])
        popularity.append(t['popularity'])
      

stop = timeit.default_timer()
print ('Time to run this code (in seconds):', stop - start)

# COMMAND ----------

import pandas as pd

df_tracks = pd.DataFrame({'artist_name':artist_name,'track_name':track_name,'track_id':track_id,'popularity':popularity})
print(df_tracks.shape)
df_tracks.head()

# COMMAND ----------

# group the entries by artist_name and track_name and check for duplicates

grouped = df_tracks.groupby(['artist_name','track_name'], as_index=True).size()
print(grouped)

# COMMAND ----------

# duplicate entries will be dropped
df_tracks.drop_duplicates(subset=['artist_name','track_name'], inplace=True)
