''' Quick script to generate data which conforms to the spec'''
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

def main():
    from pyspark import SparkContext
    sc = SparkContext("local", "Generate Data")
    pageviews = sc.parallelize(build_tables()).toDF(["timestamp", "domain_name", "page_url", "user"])
    pageviews.write.saveAsTable(name='pageviews', format='parquet')
    # pageviews.write.save('pageviews/large')
    # pageviews = spark.read.load('pageviews/large')

if __name__ == '__main__':
    main()

