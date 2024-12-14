'''
=================================================
Nama  : Arief Joko Wicaksono

Node ini digunakan untuk mengambil data dari sumber tertentu lalu menyimpan ke dalam local dalam bentuk file csv.
=================================================
'''
from pyspark.sql import SparkSession

#path ke lokasi file
path = '/home/ariefjw/airflow/data/dags/'

def load_data(path):
    '''
    Fungsi ini digunakan untuk mengekstrak data dari file berformat tsv yang akan dikonversi 
    menjadi dataframe menggunakan spark.
    parameter:
        path : lokasi file yang akan diextract
    return:
        data : data frame spark
    contoh penggunaan:
        load_data("/home/ariefjw/airflow/dags/sales.tsv")
    '''

    #create spark engine
    spark = SparkSession.builder.getOrCreate()

    #Create dataframe using spark engine 
    data = spark.read.csv(f'{path}walmart_product.tsv', sep=r'\t', header=True, inferSchema=True)

    #Convert spark dataframe to csv file with specific file path
    data.toPandas().to_csv(f'{path}P2M3_arief_joko_data_raw.csv', index=False)
    
    return data

if __name__ == '__main__':
    load_data(path)