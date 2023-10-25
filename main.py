import sqlite3
import pandas as pd
import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import traceback

def first_run():
    # Conectar a la base de datos
    conn = sqlite3.connect('spotify.db')
    csv_df = pd.read_csv('Spotify_Dataset_V3.csv', delimiter=';', encoding='ISO-8859-1')
    # Agrego 3 columnas nuevas
    csv_df['Genero_musical'] = None
    csv_df['Reproducciones_de_la_cancion'] = None
    csv_df['URL_de_imagenes_del_album'] = None

    # Guardar los datos en la base de datos SQLite
    csv_df.to_sql('spotify_data', conn, if_exists='replace', index=False)
    conn.close()

def consultar():
    # Conectar a la base de datos SQLite
    conn = sqlite3.connect('spotify.db')
    
    # Crear un cursor para ejecutar consultas SQL
    cursor = conn.cursor()
    
    try:
        # Realizar una consulta SELECT de toda la tabla
        consulta = f"SELECT * FROM spotify_data;"
        cursor.execute(consulta)
        
        # Obtener los resultados en un DataFrame
        resultados = cursor.fetchall()
        columnas = [descripcion[0] for descripcion in cursor.description]
        df = pd.DataFrame(resultados, columns=columnas)
        
    except sqlite3.OperationalError as e:
        # Manejar el error si la tabla no existe
        print(f"Error: {e}")
        df = pd.DataFrame()
    
    # Cerrar la conexión a la base de datos
    conn.close()
    
    return df

def pendientes():
    # Conectar a la base de datos SQLite
    conn = sqlite3.connect('spotify.db')
    
    # Crear un cursor para ejecutar consultas SQL
    cursor = conn.cursor()

    consulta = f"SELECT * FROM spotify_data WHERE URL_de_imagenes_del_album IS NULL"
    cursor.execute(consulta)
        
    # Obtener los resultados en un DataFrame
    resultados = cursor.fetchall()
    columnas = [descripcion[0] for descripcion in cursor.description]
    df = pd.DataFrame(resultados, columns=columnas)
    conn.close()
    return df

def ejecutar(df, sp):
    for index, track_id in enumerate(df['track_id']):
        try:
            print(f"Procesando canción {index + 1} con track_id {track_id}...")
            
            # Obtener detalles de la canción
            track_details = sp.track(track_id)
            print(f"Detalles de la canción {index + 1} obtenidos.")
            
            # Obtener detalles del álbum
            
            print(f"Detalles del álbum para la canción {index + 1} obtenidos.")
 
            album_details = sp.album(track_details['album']['id'])

            album = album_details['genres']
            popu = track_details['popularity']
            image = album_details['images'][0]['url'] if album_details['images'] else None

            # Lo agrego al dataframe
            conn = sqlite3.connect('spotify.db')
            cursor = conn.cursor()
            consulta = f"UPDATE spotify_data SET Genero_musical = '{album}', Reproducciones_de_la_cancion = '{popu}', URL_de_imagenes_del_album = '{image}' WHERE track_id = '{track_id}'"
            cursor.execute(consulta)
            # Confirmar la actualización en la base de datos
            conn.commit()
            conn.close()
            print("DB actualizado")

        
        except Exception as e:
            print(f"Error al procesar track_id {track_id}: {e}")
            traceback.print_exc()

def main():
    """
        Cuando se trabe finalizar la ejecucion con: CRTL + C
        Luego volver a ejecutar con nuevos ClientID y Secrets o esperar a que se regeneren el tiempo de API
        La ejecucion retoma donde se dejo mientras la base de datos SPOTIFY.DB 
    """
    df = consultar()
    if df.empty:
        print("Primera ejecucion")
        first_run()
        df = consultar()
    
    df = pendientes()
    # Hago un select de los pendientes
    if not df.empty:
        client_id = 'e01840c7cee14a799b398e73b9a515bf'
        client_secret = 'bd1ca293ea0f46ef84f6a2371903c5a1'
        sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=client_id, client_secret=client_secret))
        df = pendientes()
        ejecutar(df, sp)

    # Una vez finalizo exporto
    df = consultar()
    df.to_csv('Spotify_Dataset_V3_modified.csv', index=False, sep=';', encoding='ISO-8859-1')
    df.to_excel('Spotify_Dataset_V3_modified.xlsx', index=False, encoding='ISO-8859-1')


main()
    


    



