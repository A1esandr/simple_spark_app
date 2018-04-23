Для отладки использовался Spark 2.3, Hadoop 2.8

Локальное использование:

#Сборка пакета 

mvn package


#Запуск приложения 

PATH_TO_SPARK/bin/spark-submit --class "SimpleApp" --master local[4] target/simple_spark_app-1.0.jar FLAG LOGFILE APP_LOADED REGISTERED

FLAG - может иметь 2 значения:

-file для запуска загрузки данных из файла лога
при этом файл лога указывается в аргументе LOGFILE

-compute для запуска расчета процента пользователей, загрузивших приложение в течение недели после регистрации


Аргументы APP_LOADED REGISTERED имеют дефолтные значения:

APP_LOADED - events/app_loaded/*.parquet

REGISTERED - events/registered/*.parquet

#Примеры
Пример запуска загрузки данных (запись загруженных данных будет происходить в дефолтные папки):

```/usr/local/spark/bin/spark-submit --class "SimpleApp" --master local[4] target/simple_spark_app-1.0.jar -file /home/alex/events.json```

Пример запуска загрузки данных с указанием пути записи данных:

'''/usr/local/spark/bin/spark-submit --class "SimpleApp" --master local[4] target/simple_spark_app-1.0.jar -file /home/alex/events.json /home/alex/events/app_loaded/*.parquet /home/alex/events/registered/*.parquet'''

Пример запуска рассчета процента (загруженные данные ожидаются в дефолтных папках):

'''/usr/local/spark/bin/spark-submit --class "SimpleApp" --master local[4] target/simple_spark_app-1.0.jar -compute'''

Пример запуска рассчета процента с указанием пути загруженных данных:

'''/usr/local/spark/bin/spark-submit --class "SimpleApp" --master local[4] target/simple_spark_app-1.0.jar -compute /home/alex/events/app_loaded/*.parquet /home/alex/events/registered/*.parquet'''
