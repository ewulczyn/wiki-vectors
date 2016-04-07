from pyspark import SparkConf, SparkContext
import json
import argparse
import datetime
import os


"""
Given a hive table of requests grouped by IP, UA, XFF and day:
    1. convert hive data format to json
    2. break each days worth of request for each "client" into sessions
    3. remove Main-page and non-main namespace article from sessions
    4. Output [{'lang': wikipedia lang, 'title':article title, 'id': wikidata id}]
       for each session, ordered by time of request

Usage: 

spark-submit \
    --driver-memory 5g \
    --master yarn \
    --deploy-mode client \
    --num-executors 10 \
    --executor-memory 10g \
    --executor-cores 4 \
    --queue priority \
get_sessions.py \
    --release test \
    --lang en \
"""

def parse_requests(requests):
    ret = []
    for r in requests.split('||'):
        t = r.split('|')
        if (len(t) % 2) != 0: # should be list of (name, value) pairs and contain at least id,ts,title
            continue
        data_dict = {t[i]:t[i+1] for i in range(0, len(t), 2) }
        ret.append(data_dict)
    ret.sort(key = lambda x: x['ts']) # sort by time
    return ret


def parse_dates(requests):
    clean = []
    for r in requests:
        try:
            r['ts'] = datetime.datetime.strptime(r['ts'], '%Y-%m-%d %H:%M:%S')
            clean.append(r)
        except:
            pass
    return clean

def sessionize(requests):
    sessions = []
    session = [requests[0]]
    for r in requests[1:]:
        d = r['ts'] -  session[-1]['ts']
        if d > datetime.timedelta(minutes=30):
            sessions.append(session)
            session = [r,]
        else:
            session.append(r)

    sessions.append(session)
    return sessions 


def filter_backlist(requests):
    return [r for r in requests if r['id'] != 'Q5296' ] 

def scrub_dates(requests):
    for r in requests:
        del r['ts']
    return requests


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--request_db', default='a2v', help='hive db')
    parser.add_argument('--release', required=True, help='hive table')
    parser.add_argument('--lang', required=True, default = 'wikidata', help='wikidata will use all langs')



    args = vars(parser.parse_args())

    args['table'] = args['release'].replace('-', '_') + '_requests'

    input_dir  = '/user/hive/warehouse/%(request_db)s.db/%(table)s/*/*/*' % args
    output_dir ='/user/ellery/a2v/data/%(release)s/%(release)s_sessions_%(lang)s' % args
    print (os.system('hadoop fs -rm -r ' + output_dir))

    
    conf = SparkConf()
    conf.set("spark.app.name", 'a2v preprocess')
    sc = SparkContext(conf=conf, pyFiles=[])

    requests  = sc.textFile(input_dir) \
    .map(parse_requests)
    
    if args['lang'] != 'wikidata':
        requests = requests.map(lambda rs: [r for r in rs if r['lang'] == args['lang']])

    requests \
    .map(parse_dates) \
    .filter(lambda x: len(x) > 1) \
    .flatMap(sessionize) \
    .map(filter_backlist) \
    .filter(lambda x: len(x) > 1) \
    .filter(lambda x: len(x) < 30) \
    .map(scrub_dates) \
    .map(lambda x: json.dumps(x)) \
    .saveAsTextFile (output_dir, compressionCodecClass= "org.apache.hadoop.io.compress.GzipCodec")
