from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from datetime import date
from sklearn.decomposition import pca
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
from sklearn import svm
import sklearn.preprocessing as pps
import numpy as np
import boto


'''X_cat = df.main.values

le = pps.LabelEncoder()
le.fit(np.unique(X_cat))
temp = le.transform(X_cat)
b = temp.shape[0]
X_temp = np.hstack((temp.reshape((b, 1)), X_num_scale))
enc = pps.OneHotEncoder(categorical_features=[0])
enc.fit(X_temp)
X = enc.transform(X_temp).toarray()
X_train, X_test, y_train, y_test = train_test_split(X,y)'''


def get_date():
    return str(date.today().year) + '_' + str(date.today().month) + '_' + str(
        date.today().day)


def write_data(current_date=get_date()):
    df = spark.read.json("s3a://finalprojectweather/2017/*/*/*/*")
    main_table = df.select('id', 'name', 'base', 'sys.type', 'sys.country',
                           'coord.*').distinct()
    second_table = df.selectExpr('id', 'sys.id as sys_id').distinct()
    third_table = df.select(explode(
                            df.weather).alias('tmp'),
                            'id', 'sys', 'visibility',
                            'wind').selectExpr('id',
                                               'tmp.description', 'tmp.icon',
                                               'tmp.id as weather_id',
                                               'tmp.main', 'sys.message',
                                               'sys.sunrise', 'sys.sunset',
                                               'visibility',
                                               'wind.*').distinct()
    main_table.write.parquet("s3://hardoopmapreduce/project/table1_%s" %
                             (current_date))
    second_table.write.parquet("s3://hardoopmapreduce/project/table2_%s" %
                               (current_date))
    third_table.write.parquet("s3://hardoopmapreduce/project/table3_%s" %
                              (current_date))


def read_data(current_date=get_date()):
    table1 = spark.read.parquet("s3://hardoopmapreduce/project/table1_%s" %
                                (current_date))
    table1.createOrReplaceTempView("table1")
    table2 = spark.read.parquet("s3://hardoopmapreduce/project/table2_%s" %
                                (current_date))
    table2.createOrReplaceTempView("table2")
    table3 = spark.read.parquet("s3://hardoopmapreduce/project/table3_%s" %
                                (current_date))
    table3.createOrReplaceTempView("table3")
    data = spark.sql('''SELECT case when a.id=5391959 then 1 else 0 end as id,
                      b.main, b.message, (b.sunset-b.sunrise) as daylighttime,
                      b.visibility, b.deg,b.speed FROM table1 as a join table3
                      as b on a.id = b.id''').distinct()
    df = data.toPandas()
    return df


def draw():
    df = read_data()
    df.dropna(inplace=True)
    y = df.id.values
    X_num = df[['message', 'daylighttime', 'visibility', 'deg', 'speed']]
    X_num_scale = pps.scale(X_num)
    model = pca.PCA(n_components=2)
    model.fit(X_num_scale)
    new_x = model.transform(X_num_scale)
    clf = svm.SVC(kernel='linear')
    clf.fit(new_x, y)
    min_x = np.min(new_x[:, 0])
    max_x = np.max(new_x[:, 0])
    w = clf.coef_[0]
    a = -w[0] / w[1]
    xx = np.linspace(min_x-1, max_x+1)
    yy = a * xx - (clf.intercept_[0]) / w[1]
    margin = 1 / np.sqrt(np.sum(clf.coef_ ** 2))
    yy_down = yy + a * margin
    yy_up = yy - a * margin
    plt.figure(1, figsize=(16, 9))
    plt.clf()
    plt.plot(xx, yy, 'k-')
    plt.plot(xx, yy_down, 'k--')
    plt.plot(xx, yy_up, 'k--')
    plt.scatter(clf.support_vectors_[:, 0], clf.support_vectors_[:, 1], s=10,
                facecolors='none', zorder=10)
    plt.scatter(new_x[:, 0], new_x[:, 1], c=y, zorder=10, cmap=plt.cm.Paired,
                label=['blue:SFO', 'brown:SEA'])
    plt.axis('tight')
    x_min = -1
    x_max = 1
    y_min = -1
    y_max = 1
    XX, YY = np.mgrid[x_min:x_max:200j, y_min:y_max:200j]
    Z = clf.predict(np.c_[XX.ravel(), YY.ravel()])
    Z = Z.reshape(XX.shape)
    plt.figure(1, figsize=(16, 9))
    plt.xticks(())
    plt.yticks(())
    plt.title('precision:{}'.format(clf.score(new_x, y)))
    x1, x2, y1, y2 = plt.axis()
    plt.axis((x1, x2, x1, x2))
    plt.legend(loc='upper left')
    a = get_date()
    plt.savefig('%s.png' % (a))


if __name__ == '__main__':
    sc = SparkContext()
    spark = SparkSession(sc)
    a = get_date()
    write_data()
    draw()
    conn = boto.connect_s3(host='s3.amazonaws.com')
    bucket = conn.get_bucket('hardoopmapreduce')
    file1 = bucket.new_key('%s.png' % (a))
    file1.set_contents_from_filename('%s.png' % (a), policy='public-read')
