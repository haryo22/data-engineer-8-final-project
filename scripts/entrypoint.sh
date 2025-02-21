#!/bin/bash

# Inisialisasi database airflow
airflow db init

# Mengkonfigurasi peran webserver
echo "AUTH_ROLE_PUBLIC = 'Admin'" >> webserver_config.py

# Menambahkan koneksi PostgreSQL utama
airflow connections add 'postgres_main' \
  --conn-type 'postgres' \
  --conn-login $POSTGRES_USER \
  --conn-password $POSTGRES_PASSWORD \
  --conn-host $POSTGRES_CONTAINER_NAME \
  --conn-port $POSTGRES_PORT \
  --conn-schema $POSTGRES_DB

# Menambahkan koneksi untuk data warehouse PostgreSQL
airflow connections add 'postgres_dw' \
  --conn-type 'postgres' \
  --conn-login $POSTGRES_USER \
  --conn-password $POSTGRES_PASSWORD \
  --conn-host $POSTGRES_CONTAINER_NAME \
  --conn-port $POSTGRES_PORT \
  --conn-schema $POSTGRES_DW_DB

# Mengatur koneksi ke Spark
export SPARK_FULL_HOST_NAME="spark://$SPARK_MASTER_HOST_NAME"
airflow connections add 'spark_main' \
  --conn-type 'spark' \
  --conn-host $SPARK_FULL_HOST_NAME \
  --conn-port $SPARK_MASTER_PORT

# Menjalankan webserver Airflow
airflow webserver