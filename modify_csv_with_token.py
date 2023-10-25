import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

# Autenticación con Spotify
client_id = 'ecdb60776fc44274b471b7b2c6f77030'
client_secret = '86cb162d833e465581ee9ca3622467f7'
sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=client_id, client_secret=client_secret))

print("Iniciando el proceso...")

# Leer el archivo CSV con encoding 'ISO-8859-1'
df = pd.read_csv('Spotify_Dataset_V3.csv', delimiter=';', encoding='ISO-8859-1')  # Procesar solo las primeras 50 canciones

print(df)
genres = []
popularities = []
image_urls = []

for index, track_id in enumerate(df['track_id']):
    try:
        print(f"Procesando canción {index + 1} con track_id {track_id}...")
        
        # Obtener detalles de la canción
        track_details = sp.track(track_id)
        print(f"Detalles de la canción {index + 1} obtenidos.")
        
        # Obtener detalles del álbum
        album_details = sp.album(track_details['album']['id'])
        print(f"Detalles del álbum para la canción {index + 1} obtenidos.")
        
        # Añadir datos a las listas
        genres.append(album_details['genres'])
        popularities.append(track_details['popularity'])
        image_urls.append(album_details['images'][0]['url'] if album_details['images'] else None)
        
    except Exception as e:
        print(f"Error al procesar track_id {track_id}: {e}")
        genres.append(None)
        popularities.append(None)
        image_urls.append(None)

df['Genero musical'] = genres
df['Reproducciones de la canción'] = popularities
df['URL de imágenes del álbum'] = image_urls

df.to_csv('Spotify_Dataset_V3_modified.csv', index=False, sep=';', encoding='ISO-8859-1')
df.to_excel('Spotify_Dataset_V3_modified.xlsx', index=False, sep=';', encoding='ISO-8859-1')
print("Proceso completado.")

