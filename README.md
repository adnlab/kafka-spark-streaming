# Eksekusi

menjalankan Kafka, Spark, dan producer di background.

```
docker-compose up -d --build
docker-compose ps
```

cek event yang dikirimkan ke broker.

```
docker logs -f producer
```

menjalankan spark job.

```
docker exec -it spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /app/streaming/spark_streaming_job.py
```

mengecek transaction yang valid.

```
docker exec -it kafka /opt/bitnami/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --bootstrap-server kafka:29092 --topic transactions_valid
docker exec -it kafka /opt/bitnami/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --bootstrap-server kafka:29092 --topic transactions_dlq
```


# Catatan

**`producer.py`**

make_event() akan membuat transaksi random untuk kolom-kolom yang ditentukan: user_id, amount, timestamp, source. event yang dihasilkan akan valid secara default dan akan diubah ke dalam beberapa kategori event yang invalid atau late jika hasil perhitungan module 20 dari urutan event berada dalam dictionary SPECIAL. 

producer akan berjalan terus menerus dan mengirimkan event tiap 1-2 detik ke broker dengan topic transactions.

**`spark_streaming_job.py`**

spark_streaming_job.py akan membaca data dari topic transactions lalu mengubah JSON menjadi kolom-kolom user_id, amount, timestamp, source. semua kolom dibaca sebagai string kemudian dilakukan pengecekan tipe. setiap event akan dicek dengan untuk mandatory field, type, range amount, source, dan late event jika event masuk Kafka lebih dari 3 menit setelah waktu di timestamp.

data diproses per micro-batch di process_batch dengan foreachBatch, pengecekan duplicate dilakukan di sini, lalu kolom is_valid ditambahkan. event valid dikirim ke topic transactions_valid dan yang invalid ke transactions_dlq. 

query kedua membaca transactions_valid dengan withWatermark("event_time", "3 minutes") dan menghitung jumlah transaksi per tumbling window 1 menit.