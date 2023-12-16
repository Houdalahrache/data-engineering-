from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'Houda',
    'start_date': datetime(2023, 12, 1, 11, 00)
}

def get_data():
    import requests

    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    # le fichier json contient resultats et info, mais on a pas besoin de info donc on va se contenter de resultat
    res = res['results'][0]
    return res

def format_data(res):
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']} {location['city']}, {location['state']}, {location['country']}"
    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    data['country'] = location['country']
    data['latitude'] = location['coordinates']['latitude']
    data['longitude'] = location['coordinates']['longitude']
    data['birthday'] = res['dob']['date']
    return data

def stream_data():
    import json
    from kafka import KafkaProducer


    res = get_data()
    res = format_data(res)
    #print(json.dumps(res, indent=3))
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)
    producer.send('users_created', json.dumps(res).encode('utf-8'))

stream_data()