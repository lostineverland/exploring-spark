'''Update monthly table'''

from pyspark import SparkContext
from pyspark.sql.types import Row
from pyspark.sql import SparkSession as spark
from generateData import iso_8601_ts

# running as a job is failing due to a spark session issue
# sc = SparkContext("local", "update table")

SOURCE_TABLE = "pageviews"
REPORT_TABLE = "monthly_report"
REPORT_TABLE = "monthly"
DATA_FORMAT = "parquet"

makeKey = lambda r: '{user}:{domain_name}'.format(**r.asDict())

def include_existing_records(row, table):
    'when calculating the new records, pull first visits from old table'
    key = makeKey(row)
    new_val = table.get(key)
    if new_val:
        val = new_val.asDict()
        val['first'] = row.first
    else:
        val = row.asDict()
        val['cnt'] = 0
        val['weekly_visits'] = 0
    table[key] = Row(**val)

def get_new_table(iso_date):
    'query to generate results from the given iso_date'
    return spark.sql('''
        select count(*) as cnt
          , user
          , count(*) / 30 * 7 as weekly_visits
          , min(timestamp) as first
          , max(timestamp) as last
          , domain_name
        from {source_table}
        where timestamp > '{iso_date}'
        group by domain_name
          , user
        order by user, first
    '''.format(iso_date=iso_date, source_table=SOURCE_TABLE))

def store_new_values(new_values):
    new_table = sc.parallelize(new_values.values()).toDF()
    new_table.write\
        .saveAsTable(
            name=REPORT_TABLE,
            mode='overwrite',
            format=DATA_FORMAT)

def main(iso_date):
    '''Grab values from existing table, 
        generate new values, 
        update new values from existing table,
        save results'''
    old_table = spark.sql('select * from {0}'.format(REPORT_TABLE))
    new_table_rows = get_new_table(iso_date).collect()
    new_values = dict(zip(map(makeKey, new_table_rows), new_table_rows))
    for row in old_table.collect():
        include_existing_records(row, new_values)
        print 'updated {0}'.format(makeKey(row))
    store_new_values(new_values)

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("update table") \
        .getOrCreate()
    start_date = iso_8601_ts(30).next()
    main(start_date)