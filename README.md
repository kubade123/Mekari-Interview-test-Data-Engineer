# About

Code Challenge untuk role data engineer Mekari

## Implementasi script pada scheduler
![mekari drawio](https://github.com/kubade123/Mekari-Interview-test-Data-Engineer/assets/86041365/f668adb6-282b-45b4-be59-be922f7f1ddf)



## Schema Database
Database yang digunakan adalah PostgreSQL

DB Name = mekari_code_test

Schema:

1. staging
* employees
* timesheets
2. transformation
* dedup_employees
* employees_timesheets
3. final
* monthly_branch_salary

## Data Cleansing
1. Menyesuaikan nama kolom **employe_id** menjadi **employee_id** pada saat extract **employees.csv**
2. Deduplikasi record pada table employees dengan employee_id yang sama dengan cara mengambil salary yang tertinggi dengan asumsi ada kenaikan salary saat join
3. menghapus record pada table timesheets yang memiliki nilai null pada kolom checkin atau checkout
4. menghapus record pada table timesheets dengan date yang kurang dari join_date dan lebih dari resign_date

## Notes SQL

1. script initial digunakan untuk membuat tabel pertama kali
2. untuk path pada script pada folder staging menggunakan absolute path

## Notes Python

1. pada folder extract, script initial digunakan untuk membuat tabel pertama kali. script incremental digunakan untuk run secara daily dengan append row baru csv ke tabel serta membuat kolom dt_created.
2. pada folder transform, script incremental dapat digunakan pada scheduler yang run secara daily dengan memproses data dengan dt_created hari job tersebut dijalankan.
3. pada folder final, script incremental meng-append data increment tersebut. perlu diingat bahwa scriptnya hanya melakukan insert.

## Notes untuk db_config.py
Untuk db config dapat dibuat masing-masing pada tiap folder dengan nama **db_config.py** dengan isian:

```python
DB_PARAMS = {
    'dbname': 'mekari_code_test',
    'user': '',
    'password': '',
    'host': 'localhost',
    'port': '5432',
}

DB_PARAMS_2 = 'postgresql://username:password@localhost:5432/mekari_code_test'
```
