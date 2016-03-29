from db_utils import exec_hive_stat2
import dateutil
from db_utils import execute_hive_expression,get_hive_timespan
import argparse
import pandas as pd

"""
Usage:

python get_requests.py \
    --start 2016-02-01 \
    --stop  2016-02-01 \
    --release test \
    --priority
"""


def create_request_table(db, table):
    """
    Create a Table partitioned by day and host
    """
    query = """
    CREATE TABLE IF NOT EXISTS %(db)s.%(table)s (
        requests STRING
    )
    PARTITIONED BY (year INT, month INT, day INT)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    """

    params = {'db': db, 'table': table}
    exec_hive_stat2(query % params)

def add_day_to_request_table(day, table,  trace_db = 'a2v', redirect_db = 'a2v', wikidata_db = 'a2v', priority = True):

    """
    1. Take one day of webrequest logs
    2. Only consider page views
    3. Resolve redirects 
    4. Get Wikidata IDs for article corresponding to each page view
    5. Group by IP, UA, XFF
    """

    query = """
    INSERT OVERWRITE TABLE %(trace_db)s.%(trace_table)s
    PARTITION(year=%(year)d, month=%(month)d, day =%(day)d)
    SELECT
        CONCAT_WS('||', COLLECT_LIST(request)) AS requests
    FROM
        (SELECT
            client_ip,
            user_agent,
            x_forwarded_for,
            CONCAT('ts|', ts, '|id|', id, '|title|', title, '|lang|', lang ) AS request
            FROM
                (SELECT
                    client_ip,
                    user_agent,
                    x_forwarded_for,
                    ts,
                    pv2.lang,
                    pv2.title, 
                    id
                FROM
                    (SELECT
                        client_ip,
                        user_agent,
                        x_forwarded_for,
                        ts, 
                        pv1.lang,
                        CASE
                            WHEN rd_to IS NULL THEN raw_title
                            ELSE rd_to
                        END AS title
                    FROM
                        (SELECT
                            client_ip,
                            user_agent,
                            x_forwarded_for,
                            ts, 
                            normalized_host.project AS lang,
                            REGEXP_EXTRACT(reflect('java.net.URLDecoder', 'decode', uri_path), '/wiki/(.*)', 1) as raw_title
                        FROM
                            wmf.webrequest
                        WHERE 
                            is_pageview
                            AND normalized_host.project_class = 'wikipedia'
                            AND agent_type = 'user'
                            AND %(time_conditions)s
                            AND hour = 1
                            AND LENGTH(REGEXP_EXTRACT(reflect('java.net.URLDecoder', 'decode', uri_path), '/wiki/(.*)', 1)) > 0
                        ) pv1
                    LEFT JOIN
                        %(redirect_db)s.redirect r
                    ON pv1.raw_title = r.rd_from
                    AND pv1.lang = r.lang
                    ) pv2
                INNER JOIN
                    %(wikidata_db)s.wikidata_will w
                ON pv2.title = w.title
                AND pv2.lang = w.lang
                ) pv3
            ) pv4
    GROUP BY
        client_ip,
        user_agent,
        x_forwarded_for
    HAVING 
        COUNT(*) < 100
        AND COUNT(*) > 1
    """


    day_dt = dateutil.parser.parse(day)

    params = {  'time_conditions': get_hive_timespan(day, day, hour = False),
                'trace_db': trace_db,
                'redirect_db': redirect_db,
                'wikidata_db': wikidata_db,
                'trace_table': table,
                'year' : day_dt.year,
                'month': day_dt.month,
                'day': day_dt.day
                }

    exec_hive_stat2(query % params, priority = priority)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument( '--start', required=True,  help='start day')
    parser.add_argument( '--stop', required=True,help='start day')
    parser.add_argument('--db', default='a2v', help='hive db')
    parser.add_argument('--release', required=True, help='hive table')
    parser.add_argument('--priority', default=False, action="store_true",help='queue')


    args = parser.parse_args()
    db = args.db
    table = args.release + '_requests'

    create_request_table(args.db, table)

    start = args.start 
    stop  = args.stop
    days = [str(day) for day in pd.date_range(start,stop)] 
    for day in days:
        print('Adding Traces From: ', day)
        add_day_to_request_table(day, table, trace_db = db, redirect_db = db, wikidata_db = db, priority = args.priority)
