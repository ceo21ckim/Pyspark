import pyspark
from pyspark import SparkConf, SparkContext 
from pyspark.sql import SparkSession
import os 

spark = SparkSession.builder.appName('Basics').getOrCreate()

# make DataFrame
myRange = spark.range(1000).toDF('number')


# transformation
# 스파크의 핵심 구조는 immutable 이라고 한다. 
# 한번 생성하면 변경이 불가능하고, 변경을 하기 위해서는 spark에 알려주어야 한다. 
# 알려주는 명령을 트랜스포메이션[transformation] 이라고 칭한다.

divisBy2 = myRange.where('number % 2 = 0')

# action을 실행하지 않으면 spark는 실행이 안된다.
# action은 총 3가지가 존재한다. 
myRange.count()
divisBy2.count()

# example
file = 'pyspark/Spark The Definitive Guide/data/flight-data/csv/2015-summary.csv'

flightData2015 = spark.read.option('inferSchema', 'true').option('header', 'true').csv(file)

flightData2015.take(3)

flightData2015.sort('count').explain()

# 스파크는 셔플 수행 시 기본적으로 200개의 셔플 파티션을 생성하는데, conf.set을 통해 5개로 설정하여 출력 파티션 수를 줄인다.
spark.conf.set('spark.sql.shuffle.partitions', 5)

# count라는 columns을 sorting해 2개를 가져온다. 
flightData2015.sort('count').take(2)

# 2.10.1 DataFrame과 SQL
# Spark는 언어에 상관없이 같은 방식으로 transformation할 수 있다. 
# createOrReplaceTempView를 호출하면 DataFrame을 테이블이나 뷰로 만들 수 있다. 
flightData2015.createOrReplaceGlobalTempView('flight_data_2015')

dataFrameWay = flightData2015.groupBy('DEST_COUNTRY_NAME').count()

# sqlWay.explain()
dataFrameWay.explain()

# max를 찾아온다. 
from pyspark.sql.functions import max 
flightData2015.select(max('count')).take(1)

# PYTHON
from pyspark.sql.functions import desc

flightData2015.groupBy('DEST_COUNTRY_NAME').sum('count').withColumnRenamed('sum(count)', 'destination_total')\
    .sort(desc('destination_total')).limit(5).show()

flightData2015.groupBy('ORIGIN_COUNTRY_NAME').sum('count').sort(desc('sum(count)')).show(5)


flightData2015.groupBy('DEST_COUNTRY_NAME').sum('count').withColumnRenamed('sum(count)', 'destination_total').sort(desc('destination_total')).limit(5).explain()