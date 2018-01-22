# ex
import datetime

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *

df = pd.read_csv('para20180111.csv', delimiter='\t')
paramDicts = pd.Series(df.area.values, index=df.enodebid).to_dict()


def T_Net_Compare(table_name, area):
    spark = SparkSession \
        .builder \
        .appName('myApp') \
        .getOrCreate()
    uri = "mongodb://xxx" + table_name + "?authSource=admin"
    df = spark.read.format("com.mongodb.spark.sql").option('uri', uri).load()
    df.registerTempTable(table_name)
    query_date = (datetime.datetime.now() + datetime.timedelta(days=-7)).strftime('%Y-%m-%d')
    f1 = spark.sql("select X,Y,Count,Rsrp,CellId from " + table_name + " where StatDate > '" + query_date + "'")
    f2 = f1.rdd.filter(lambda s: paramDicts.get(int(s[4][:6])) == area) \
        .map(lambda s: ((s[0], s[1]), (s[3] / s[2], 1))) \
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
        .map(lambda x: ((x[0][0], x[0][1]), x[1][1] / x[1][0]))
    result = f2.sortBy(lambda x: x[0][0], True)
    result.foreach(lambda x: print(x))
    return result


# -------------------------------数据提取
import simplekml


# 栅格化
# 经度编号 = （经度 - 109.456006485399）/（0.00000972 × 级别）+ 1001 （向下取整）
# 纬度编号 = （纬度 - 20.1297900884702）/（0.00000896 × 级别）+ 1001（向下取整）
# 网格编号 = 经度编号×10000000 + 纬度编号
# 反栅格化
# 经度 = round((math.floor(x/1000000)-1001)*0.00000972*级别+109.456006485399+0.00000972*50/2, 7)
# 纬度 = round((x%1000000-1001)*0.00000896*级别+20.1297900884702+0.00000896*50/2, 7)
# 颜色划分
# 黄色 -100dbm到-105dbm
# 紫色 -105dbm到-110dbm
# 红色 -110dbm以下

# --------------------------------KML生成
def colorMap(value):
    # if not str(value).isdigit(): return False
    if value < -115: return simplekml.Color.red
    if value >= -115 and value < -105: return simplekml.Color.brown
    if value >= -105 and value < -95: return simplekml.Color.purple
    if value >= -95 and value < -85: return simplekml.Color.blue
    if value >= -85: return simplekml.Color.green
    return False


if __name__ == '__main__':
    # spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.0.0 test.py
    for areaName in set(paramDicts.values()):
        print(areaName,'***********************',paramDicts.get('868612'))
        schema = StructType([
            StructField("_1", IntegerType(), True),
            StructField("_2", IntegerType(), True),
            StructField("_3", FloatType(), True),
            StructField("_4", FloatType(), True),
            StructField("_5", StringType(), True),
        ])
        # position_telecom_combined = T_Net_Compare('position_telecom_combined').filter(lambda x: 0<x[0][0]<50000 and 0<x[0][1]<50000)
        # position_telecom_combined = T_Net_Compare('Position_test').filter(
        position_telecom_combined = T_Net_Compare('position_telecom_combined', areaName).filter(
            lambda x: 0 < x[0][0] < 50000 and 0 < x[0][1] < 50000)
        # position_telecom_combined.map(lambda x: (
        #     112 + x[0][0] * 0.00049, 22 + x[0][1] * 0.00045, float('%.2f' % (x[1] - 104)))).toDF().toPandas().to_csv(
        #     'telecom08.csv', header=None, index=None)
        if position_telecom_combined.isEmpty():
            print('empty rdd.......................')
            continue
        df = position_telecom_combined.map(lambda x: (
            112 + x[0][0] * 0.00049, 22 + x[0][1] * 0.00045, float('%.2f' % (x[1] - 104)))).toDF().toPandas()
        # 电信
        kml_telecom = simplekml.Kml()
        df.columns = ['lon', 'lat', 'telecom']
        telecom = df[['lon', 'lat', 'telecom']].to_dict(orient='records')
        for index, item in enumerate(telecom):
            print(item)
            # print('-------------')
            if colorMap(item['telecom']):
                print(index)
                pol = kml_telecom.newpolygon(name=str(index))
                p = [round(item['lon'], 7), round(item['lat'], 7)]
                delta = 0.00025
                pol.outerboundaryis = [
                    (p[0] - delta, p[1] + delta),
                    (p[0] + delta, p[1] + delta),
                    (p[0] + delta, p[1] - delta),
                    (p[0] - delta, p[1] - delta),
                    (p[0] - delta, p[1] + delta)
                ]
                pol.style.linestyle.color = simplekml.Color.green
                pol.style.linestyle.width = 0
                pol.style.polystyle.fill = 1
                pol.style.polystyle.color = simplekml.Color.changealphaint(255, colorMap(item['telecom']))
        kml_telecom.save(areaName + ".kml")
