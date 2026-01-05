import requests
import pandas as pd
from datetime import datetime
from storage.minio_client import MinioClient  # ton client MinIO existant

# URL de l'API adresse
BASE_URL = 'https://api-adresse.data.gouv.fr/search/'

# 1. Liste de villes françaises
villes = ['Paris', 'Lyon', 'Marseille', 'Toulouse', 'Nice', 
          'Nantes', 'Strasbourg', 'Montpellier', 'Bordeaux', 'Lille']

# 2. Initialiser le client MinIO
client = MinioClient()  # Assure-toi que upload_csv() fonctionne pour le bucket silver

# 3. Récupérer les informations via l'API
data_adresses = []

for ville in villes:
    try:
        params = {
            'q': ville,
            'limit': 1,  # première correspondance
        }
        response = requests.get(BASE_URL, params=params, timeout=5)
        response.raise_for_status()
        data = response.json()

        if data['features']:
            feature = data['features'][0]
            properties = feature['properties']
            geometry = feature['geometry']

            data_adresses.append({
                'ville': ville,
                'label': properties.get('label', ''),
                'code_postal': properties.get('postcode', ''),
                'commune': properties.get('city', ''),
                'latitude': geometry['coordinates'][1],
                'longitude': geometry['coordinates'][0]
            })
        else:
            print(f"Aucune adresse trouvée pour {ville}")
            data_adresses.append({
                'ville': ville,
                'label': None,
                'code_postal': None,
                'commune': None,
                'latitude': None,
                'longitude': None
            })

    except requests.exceptions.RequestException as e:
        print(f"Erreur lors de la récupération des données pour {ville} : {e}")
        continue

# 4. Créer un DataFrame
df = pd.DataFrame(data_adresses)

# 5. Générer le CSV en mémoire
csv_content = df.to_csv(index=False)

# 6. Nom du fichier avec date et heure
filename = f"adresses_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
object_path = f"adresses/{filename}"  # chemin dans le bucket

# 7. Upload dans le bucket silver
uri = client.upload_csv(csv_content, object_path)  # Assure-toi que upload_csv() cible silver

if uri:
    print(f"Fichier uploadé avec succès dans le bucket silver : {uri}")
else:
    print(f"Échec de l'upload du fichier : {filename}")
