import requests
from graphql import build_client_schema, graphql_sync, print_schema, parse


# Define your GraphQL query

query = """

{grid(start: 1693180801, end: 1693267199, station: FIP_GROOVE, includeTracks: true){

        ... on TrackStep {

          id

          track {

            id

            title

            albumTitle

            mainArtists

            productionDate

          }

        }

}

}

"""



# URL of the GraphQL endpoint

url = 'https://openapi.radiofrance.fr/v1/graphql?x-token=ed301a0a-cae5-4391-863d-4727c0ef4990'



# Make a POST request with the query

response = requests.post(url, json={'query': query})



# Get the JSON response

data = response.json()



# Print the response

print(data)

# Load the response to a panda Dataframe, unnest it, clean it and load it to CSV for inspection
import json

import pandas as pd



#Load response to panda df

df = pd.DataFrame(data)



# Unnest the 'info' JSON column using json_normalize

df = pd.json_normalize(data, record_path=['data','grid'])



# transform track.mainArtists to string and clean it

cleanArtists = df["track.mainArtists"].astype(str).str.replace("['", '').str.replace("']", '')



print(cleanArtists)



# add clean column back to the df

df['Artists'] = cleanArtists



print(df)



# drop dirty original column

clean_table = df.drop(columns=['track.mainArtists'])



print(clean_table)



#Save clean table to CSV

clean_table.to_csv('FIP_GROOVE.csv', index=False)

# Install Spotipy library to interact with Spotify API

# Client ID and secret > Add to file outside of Notebook when done
pip install requests spotipy
import spotipy

from spotipy.oauth2 import SpotifyClientCredentials



# Replace with your Spotify API credentials

client_id = '6633ab91f37b451589f6b7b2710f21f8'

client_secret = '2ba8a1b4a58e4dd1bb2b4059b97cff33'



# Authenticate

client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)

sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

# Parse the results from the cleantable df to the spotipy search endpoint. Retrieve the songs URI and load them to spotify_uri dataframe
import pandas as pd



spotify_uri = []



for index, row in clean_table.iterrows():

    # Step 4: Construct the data payload

    query = f"{row['track.title']} {row['Artists']} {row['track.productionDate']}"

    

    # Step 5: Make the API request

    try:

        response = sp.search(q=query, type=('track', 'artist', 'year'), limit=1)

        

        # Step 6: Handle the API response

        for idx, track in enumerate(response['tracks']['items']):

            #print(f"{idx + 1}: {track['uri']}")

            spotify_uri.append(track['uri'])

    except Exception as e:

        print(f"An error occurred for row {index}: {str(e)}")



# Now, spotify_uri contains the Spotify URIs

print(spotify_uri)
# Call the Create_Playlist and create a playlist that's called "My Month FIP Groove Playlist"
import datetime

import spotipy

from spotipy.oauth2 import SpotifyOAuth



# Initialize the Spotify API client with proper credentials

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id = '6633ab91f37b451589f6b7b2710f21f8',

                                               client_secret = '2ba8a1b4a58e4dd1bb2b4059b97cff33',

                                               redirect_uri='YOUR_REDIRECT_URI',

                                               scope='playlist-modify-public'))



# Get the current month

current_month = datetime.datetime.now().strftime("%B")



# Create a playlist with the current month in the name

sp.user_playlist_create(user='e2ra', name=f"My {current_month} FIP Groove Playlist", public=True, collaborative=False, description='A selection of tracks played on FIP Groove / Une sélection de titres joués sur FIP Groove - Playlist générée automatiquement via Radio France API, Spotify API et quelques lignes de code')

import datetime



#get current date

current_date = datetime.datetime.now()



#get current month

current_month = current_date.month



sp.user_playlist_create(user='e2ra', name=f"My {current_month} FIP Groove Playlist", public=True, collaborative=False, description='A selection of tracks played on FIP Groove / Une selection de titre joués sur FIP Groove - Playlist générée automatiquement via Radio France API, Spotify API et quelques lignes de code')
# Parse the results from the spotify_uri dataframe to the spotipy add tracks to playlist
