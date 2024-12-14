'''
=================================================
Nama  : Arief Joko Wicaksono

Node ini melakukan cleaning terhadap raw data walmart_product agar siap untuk disimpan ke dalam  data base.
=================================================
'''

from pyspark.sql import SparkSession

#Creating variable data as spark dataframe for anrgument in tarnsform function
path = '/home/ariefjw/airflow/data/dags/'
spark = SparkSession.builder.getOrCreate()
data = spark.read.csv(f'{path}/P2M3_arief_joko_data_raw.csv', header=True, inferSchema=True)

def transform(data):
    '''
    Fungsi ini digunakan untuk melakukan transformasi terhadap dataframe. Tujuan transform untuk cleaning data agar siap untuk disimpan dalam database. Proses transform yang dilakukan merubahan nama kolom, perubahan format data, cek dan hapus missing values, menambah atau menghapus kolom
    parameter:
        data : dataframe dengan spark
    return:
        data_clean : dataframe hasil transform yang di konversi menjadi pandas dataframe
    contoh penggunaan:
        spark = SparkSession.builder.getOrCreate()
        data = spark.read.csv('/home/ariefjw/airflow/dags/sales.tsv', sep=r'\t', header=True, inferSchema=True)
        data_clean = transform(data)
    '''
  
    #Delete columns
    data_clean = data.drop('Product Model Number', 'Product Contents', 'Bsr', 'Expected Category Count', 'sku', 'upc', 'Product Rating')

    #Rename columns
    data_clean = data_clean.withColumnRenamed('Uniq Id','uniq_id')\
            .withColumnRenamed('Crawl Timestamp','crawl_timestamp')\
            .withColumnRenamed('Product Id','product_id')\
            .withColumnRenamed('Product Company Type Source','product_company_type_source')\
            .withColumnRenamed('Product Category Group Code','product_category_group_code')\
            .withColumnRenamed('Product Category Code','product_category_code')\
            .withColumnRenamed('Product Market Code','product_market_code')\
            .withColumnRenamed('Product Sector Code','product_sector_code')\
            .withColumnRenamed('Retailer','retailer')\
            .withColumnRenamed('Product Category','product_category')\
            .withColumnRenamed('Product Brand','product_brand')\
            .withColumnRenamed('Product Name','product_name')\
            .withColumnRenamed('Product Price','product_price')\
            .withColumnRenamed('Product Url','product_url')\
            .withColumnRenamed('Market','market')\
            .withColumnRenamed('Product Description','product_description')\
            .withColumnRenamed('Product Currency','product_currency')\
            .withColumnRenamed('Product Available Inventory','product_available_inventory')\
            .withColumnRenamed('Product Image Url','product_image_url')\
            .withColumnRenamed('Product Tags','product_tags')\
            .withColumnRenamed('Product Reviews Count','product_reviews_count')\
            .withColumnRenamed('Joining Key','joining_key')\
            .withColumnRenamed('Expected Brand Count','expected_brand_count')
    
    #fill missing values
    data_clean = data_clean.fillna('No Category', subset='product_category')\
            .fillna('No Brand', subset='product_brand')\
            .fillna('No Description', subset='product_description')\
            .fillna('No Image', subset='product_image_url')\
            .fillna('No Tags', subset='product_tags')

    #Convert to pandas dataframe
    data_clean = data_clean.toPandas()
    
    #Conver dataframe to csv file with specific file path
    data_clean.to_csv(f'{path}P2M3_arief_joko_data_clean.csv', index=False)

    return data_clean

if __name__ == '__main__':
    transform(data)