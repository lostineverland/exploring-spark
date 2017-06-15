''' Quick script to generate data which conforms to the spec'''
from pyspark import SparkContext, SparkConf, SQLContext
import random, datetime


ISO_8601_HOURS = '%Y-%m-%dT%H'
day = datetime.timedelta(days=1)

domains = [ 
    "www.google.com",
    "www.yahoo.com",
    "www.amazon.com",
    "www.facebook.com",
    "www.espn.com",
    "www.pixar.com",
    "www.twitter.com",
    "www.pinterest.com",
    "www.cnn.com",
    "www.nytimes.com",
    "www.nytimes.com",
]

users = [ # subset of dariusk/corpora names
    "Jacob",
    "Michael",
    "Joshua",
    "Matthew",
    "Daniel",
    "Christopher",
    "Andrew",
    "Ethan",
    "Joseph",
    "William",
    "Anthony",
    "David",
    "Alexander",
    "Nicholas",
    "Ryan",
    "Tyler",
    "James",
    "John",
    "Jonathan",
    "Noah",
    "Brandon",
    "Christian",
    "Dylan",
    "Samuel",
    "Benjamin",
    "Nathan",
    "Zachary",
    "Logan",
    "Justin",
    "Gabriel",
    "Jose",
    "Austin",
    "Kevin",
    "Elijah",
    "Caleb",
    "Robert",
    "Thomas",
    "Jordan",
    "Cameron",
    "Jack",
]

def iso_8601_ts(Ndays):
    'a generator to traverse days timestamp'
    dt = datetime.datetime.now() - (day * Ndays)
    for i in range(Ndays):
        dt += day
        yield dt.strftime(ISO_8601_HOURS)

def random_entry(entries):
    'return a random value from a list'
    return random.sample(entries, 1)[0]

def random_url(domain):
    'return a url, given a domain, where the path is a random 4 digit integer'
    return '{0}/{1}'.format(domain, random.randint(1000,9999))


def generate_entry(ts):
    'Generate a row, with a 1/60 chance of timestamp collision'
    timestamp =  '{0}:{1:02g}'.format(ts, random.randint(0, 59))
    domain_name = random_entry(domains)
    page_url = random_url(domain_name)
    user = random_entry(users)
    return (timestamp, domain_name, page_url, user)

def build_tables(days=60, daily_rows=30):
    return reduce(
        lambda mem, dt: mem + map(generate_entry, [dt] * daily_rows),
        iso_8601_ts(days),
        [])

def main(sc, sqlContext):
    print('-'*10 + 'here' + '-'*10)
    # print(sc.getConf().getAll())
    pageviews = sc.parallelize(build_tables())
    df = sqlContext.createDataFrame(pageviews, ["timestamp", "domain_name", "page_url", "user"])
    # df = sc.parallelize(build_tables()).toDF(["timestamp", "domain_name", "page_url", "user"])
    # df.write.saveAsTable(name='pageviews', format='parquet')
    df.write.save('pageviews', format='parquet')
    # df = sqlContext.read.load('pageviews')

if __name__ == '__main__':
    print("main is executed")
    conf = SparkConf().setAppName("generate_data")
    # conf.set('hive.metastore.warehouse.dir', 'file:/spark-warehouse')
    # conf.set(u'spark.sql.catalogImplementation', 'hive'),
    # conf.set("hive.metastore.warehouse.dir", "file:/spark-warehouse")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    main(sc, sqlContext)

# Working w/ Parquet & S3
# S3_PATH = 's3a://mypageviews/year={year:04g}/month={month:02g}/day={day:02g}'.format(year=year, month=month, day=day)
# df = sqlContext.read.load(S3_PATH)
# df.repartition(1).saveAsParquetFile(S3_PATH + '-repartitioned')