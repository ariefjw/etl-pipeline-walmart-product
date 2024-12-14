'''
=================================================
Nama  : Arief Joko Wicaksono

Node ini digunakan untuk proses penyimpanan data yang sudah bersih ke dalam database mongoDB.
Node ini melakukan koneksi dengan database.
=================================================
'''
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import pandas as pd


#declaration variable
path ='/home/ariefjw/airflow/data/dags/' #path to location file
DB_NAME = 'milestones3' #database name
COLLECTION_NAME = 'walmart_product' #table name

#creating data_clean as pandas dataframe
data_clean = pd.read_csv(f'{path}P2M3_arief_joko_data_clean.csv')


def load(DB_NAME, COLLECTION_NAME, data_clean):
  '''
  Fungsi ini digunakan untuk memasukkan data yang sudah bersih kedalam database mongoDB.
  parameter:
    DB_NAMA : nama database di mongoDB
    COLLECTION_NAME : nama table untuk penyimpanan data
    data_clean : pandas dataframe hasil cleaning
  '''
  #configuration connection to mongoDB
  uri = "mongodb+srv://ariefjw:ariefjw@cluster0.cr8et.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
  client = MongoClient(uri, server_api=ServerApi('1'))
  db = client[DB_NAME]
  collection = db[COLLECTION_NAME]

  #push data to mongoDB
  records = data_clean.to_dict(orient="records")
  try:
    inserted = collection.insert_many(records)
    print(f'Inserted data to mongoDB: {inserted.inserted_ids}')
  except Exception as e:
    print(f'Error inserting data to mongoDB: {e}')

if __name__ == '__main__':
    load(DB_NAME, COLLECTION_NAME, data_clean)