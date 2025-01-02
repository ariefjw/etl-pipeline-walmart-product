# ETL Pipeline Walmart Product

## Deskripsi
Proyek ini digunakan untuk otomatisasi proses Extract, Transform, Load (ETL) menggunakan Apache Airflow. Proyek ini menjalankan tiga node terpisah untuk mengekstrak, mentransformasi, dan memuat data produk Walmart. Proses ini dijadwalkan untuk berjalan setiap hari Sabtu antara pukul 09:10 hingga 09:30 dengan interval 10 menit.

## Struktur Proyek
- **dags/dags.py**: File utama yang berisi definisi DAG (Directed Acyclic Graph) untuk proses ETL.
- **extract.py**: Skrip untuk mengekstrak data dari sumber.
- **transform.py**: Skrip untuk mentransformasi data yang telah diekstrak.
- **load.py**: Skrip untuk memuat data ke dalam sistem tujuan.

## Prerequisites
Sebelum menjalankan proyek ini, pastikan Anda telah menginstal:
- Python 3.x
- Apache Airflow

Anda dapat menginstal Apache Airflow dengan perintah berikut:
```bash
pip install apache-airflow
```

## Cara Menggunakan
1. **Instalasi Airflow**: 
   - Ikuti petunjuk instalasi di [dokumentasi resmi Airflow](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html).

2. **Konfigurasi DAG**: 
   - Sesuaikan parameter dalam file `dags.py` sesuai kebutuhan Anda, termasuk `path` untuk skrip ekstraksi, transformasi, dan pemuatan.

3. **Jalankan Airflow**: 
   - Mulai scheduler dan web server Airflow dengan perintah:
   ```bash
   airflow scheduler
   airflow webserver
   ```

4. **Akses UI Airflow**: 
   - Buka browser dan akses UI Airflow di `http://localhost:8080` untuk memantau dan mengelola DAG.

5. **Jadwalkan DAG**: 
   - DAG akan berjalan secara otomatis sesuai dengan jadwal yang telah ditentukan.

## Contoh Penggunaan
Setelah DAG dijadwalkan, proses ETL akan berjalan secara otomatis. Anda dapat memantau status setiap task (extract, transform, load) melalui UI Airflow.

## Hasil
Setelah proses ETL selesai, data produk Walmart akan tersedia di sistem tujuan yang telah ditentukan dalam skrip `load.py`.
