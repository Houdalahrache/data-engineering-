from kafka import KafkaConsumer
import json
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime

# Récupérez les informations de connexion InfluxDB à partir des variables d'environnement
influxdb_url = "http://localhost:8086"
influxdb_token = "token"
influxdb_org = "org"
influxdb_bucket = "users"
kafka_topic = "users_created"

# Initialisez le client InfluxDB
influxdb_client = InfluxDBClient(url=influxdb_url, token=influxdb_token, username='', password='')

# Créez l'API d'écriture
write_api = influxdb_client.write_api(write_options=SYNCHRONOUS)

# Variables utilisées pour compter
cata = 0
catb = 0
catc = 0
count_male = 0
count_female = 0
total_users = 0
# Créez le consommateur Kafka
consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=['localhost:9092'],
    group_id='my-group',
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

# Parcourir les messages du topic Kafka
for message in consumer:
    data = json.loads(message.value)

    # Calculer l'âge à partir de la date d'anniversaire
    registered_date = datetime.fromisoformat(data["birthday"][:-1])
    age = (datetime.now() - registered_date).days // 365

    # Catégoriser l'âge
    if age < 18:
        age_category = "cat-a"
        cata += 1
    elif 18 <= age <= 60:
        age_category = "cat-b"
        catb += 1
    else:
        age_category = "cat-c"
        catc += 1
    # categoriser le genre

    if data["gender"] == "male":
            count_male += 1
    elif data["gender"] == "female":
            count_female += 1

     # Calculer les pourcentages
    total_count = count_male + count_female
    per_male = (count_male / total_count) * 100
    per_female = (count_female / total_count) * 100
    # transformer les données en float
    latitude = float(data["latitude"])
    longitude = float(data["longitude"])

    # calculer le nbre total des users
    total_users += 1

    # Créez un point InfluxDB
    point = Point("users") \
        .field("country", data["country"]) \
        .field("gender", data["gender"]) \
        .field("age_category", age_category) \
        .field("cata", cata) \
        .field("catb", catb) \
        .field("catc", catc) \
        .field("male_count", count_male) \
        .field("female_count", count_female) \
        .field("male_percentage", per_male) \
        .field("female_percentage", per_female) \
        .field("latitude_f", latitude) \
        .field("longitude_f", longitude) \
        .field("total_users", total_users)


    # Écrire le point dans InfluxDB
    write_api.write(bucket=influxdb_bucket, org=influxdb_org, record=point)

# Fermez la connexion API d'écriture à la fin
write_api.__del__()
