from db_utils import exec_hive_stat2
import dateutil
from db_utils import execute_hive_expression,get_hive_timespan
import argparse
import pandas as pd

"""
Usage:

python get_requests.py \
    --start 2016-03-01 \
    --stop  2016-03-01 \
    --release test \
    --priority \
    --min_count 1
"""

def get_requests(start, stop, table,  trace_db = 'a2v', prod_db = 'prod', priority = True, min_count=50):

    query = """
    SET mapreduce.input.fileinputformat.split.maxsize=200000000;



    -- get pageviews, resolve redirects, add wikidata ids

    DROP TABLE IF EXISTS %(trace_db)s.%(trace_table)s_pageviews;
    CREATE TABLE %(trace_db)s.%(trace_table)s_pageviews AS
    SELECT
        year,month,day,
        client_ip,
        user_agent,
        x_forwarded_for,
        ts,
        pv2.lang,
        pv2.title, 
        id
    FROM
        (SELECT
            year,month,day,
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
                year,month,day,
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
                AND webrequest_source = 'text'
                AND normalized_host.project_class = 'wikipedia'
                AND agent_type = 'user'
                AND %(time_conditions)s
                AND LENGTH(REGEXP_EXTRACT(reflect('java.net.URLDecoder', 'decode', uri_path), '/wiki/(.*)', 1)) > 0
            ) pv1
        LEFT JOIN
            %(prod_db)s.redirect r
        ON pv1.raw_title = r.rd_from
        AND pv1.lang = r.lang
        ) pv2
    INNER JOIN
        %(prod_db)s.wikidata_will w
    ON pv2.title = w.title
    AND pv2.lang = w.lang;


    DROP TABLE IF EXISTS %(trace_db)s.%(trace_table)s_editors;
    CREATE TABLE %(trace_db)s.%(trace_table)s_editors AS
    SELECT
        client_ip,
        user_agent,
        x_forwarded_for
    FROM
        wmf.webrequest
    WHERE  
        uri_query RLIKE 'action=edit' 
        AND %(time_conditions)s
    GROUP BY
        client_ip,
        user_agent,
        x_forwarded_for;


    DROP TABLE IF EXISTS %(trace_db)s.%(trace_table)s_reader_pageviews;
    CREATE TABLE %(trace_db)s.%(trace_table)s_reader_pageviews AS
    SELECT
        p.*
    FROM
        %(trace_db)s.%(trace_table)s_pageviews p
    LEFT JOIN
        %(trace_db)s.%(trace_table)s_editors e
    ON (
        p.client_ip = e.client_ip
        AND p.user_agent = e.user_agent
        AND p.x_forwarded_for = e.x_forwarded_for
        )
    WHERE
        e.client_ip is NULL
        AND e.user_agent is NULL
        AND e.x_forwarded_for is NULL;


    DROP TABLE IF EXISTS %(trace_db)s.%(trace_table)s_clients_per_item;
    CREATE TABLE %(trace_db)s.%(trace_table)s_clients_per_item AS
    SELECT
        id, 
        COUNT(*) as n
    FROM
        (SELECT
            client_ip,
            user_agent,
            x_forwarded_for,
            id
        FROM
            %(trace_db)s.%(trace_table)s_reader_pageviews
        GROUP BY
            client_ip,
            user_agent,
            x_forwarded_for,
            id
        ) a
    GROUP BY
        id;


    DROP TABLE IF EXISTS %(trace_db)s.%(trace_table)s_eligible_reader_pageviews;
    CREATE TABLE %(trace_db)s.%(trace_table)s_eligible_reader_pageviews AS
    SELECT pv.*
    FROM
        (SELECT
            p.*
        FROM
            %(trace_db)s.%(trace_table)s_reader_pageviews p
        JOIN
            %(trace_db)s.%(trace_table)s_clients_per_item c
        ON (p.id = c.id)
        WHERE
            c.n >= %(min_count)s
        ) pv
    LEFT JOIN
        (SELECT
            lang,
            page_title
        FROM
            %(prod_db)s.page_props
        WHERE
            propname = 'disambiguation'
            AND lang RLIKE '.*'
        GROUP BY
            lang,
            page_title
        ) d 
    ON
        (pv.lang = d.lang and pv.title = d.page_title)
    WHERE
        d.page_title IS NULL
        AND pv.title NOT RLIKE 'disambig'
        AND pv.title NOT RLIKE ':';
            


    DROP TABLE IF EXISTS %(trace_db)s.%(trace_table)s_requests;
    CREATE TABLE %(trace_db)s.%(trace_table)s_requests AS
    SELECT
        CONCAT_WS('||', COLLECT_LIST(request)) AS requests
    FROM
        (SELECT
            client_ip,
            user_agent,
            x_forwarded_for,
            CONCAT('ts|', ts, '|id|', id, '|title|', title, '|lang|', lang ) AS request
        FROM %(trace_db)s.%(trace_table)s_eligible_reader_pageviews      
        ) a
    GROUP BY
        client_ip,
        user_agent,
        x_forwarded_for
    HAVING 
        COUNT(*) <= 1000
        AND COUNT(*) > 1;

    DROP TABLE IF EXISTS %(trace_db)s.%(trace_table)s_eligible_reader_pageviews;
    DROP TABLE IF EXISTS %(trace_db)s.%(trace_table)s_clients_per_item;
    DROP TABLE IF EXISTS %(trace_db)s.%(trace_table)s_reader_pageviews;
    DROP TABLE IF EXISTS %(trace_db)s.%(trace_table)s_editors;
    DROP TABLE IF EXISTS %(trace_db)s.%(trace_table)s_pageviews;
    """

    params = {  'time_conditions': get_hive_timespan(start, stop, hour = False),
                'trace_db': trace_db,
                'prod_db': prod_db,
                'trace_table': table,
                'min_count': min_count
                }
    exec_hive_stat2(query % params, priority = priority)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument( '--start', required=True,  help='start day')
    parser.add_argument( '--stop', required=True,help='start day')
    parser.add_argument('--db', default='a2v', help='hive db')
    parser.add_argument('--release', required=True, help='hive table')
    parser.add_argument('--priority', default=False, action="store_true",help='queue')
    parser.add_argument('--min_count', default=50)

    args = parser.parse_args()
    print(args)
    db = args.db
    table = args.release.replace('-', '_')
    start = args.start 
    stop  = args.stop
    get_requests(   start,
                    stop,
                    table,
                    trace_db = db,
                    prod_db = 'prod', 
                    priority = args.priority,
                    min_count = args.min_count)
