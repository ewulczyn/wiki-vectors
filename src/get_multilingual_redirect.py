import argparse
from db_utils import exec_hive_stat2 as exec_hive
from sqoop_utils import sqoop_prod_dbs

def create_multilingual_redirect(db_name):
    """
    Create a Table partitioned by day and host
    """
    query = """
    DROP TABLE IF EXISTS %(db_name)s.redirect;
    CREATE TABLE %(db_name)s.redirect (
            rd_from STRING,
            rd_to STRING
        )
        PARTITIONED BY (lang STRING)
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
        STORED AS TEXTFILE
    """


    params = {'db_name': db_name}
    exec_hive(query % params)



def fill_multilingual_redirect(db_name, langs):
    query = """
    INSERT OVERWRITE TABLE %(db_name)s.redirect
    PARTITION(lang='%(lang)s')
    SELECT
        rd_from,
        rd_to
    FROM %(db_name)s.%(lang)s_redirect
    """
    for lang in langs:
        params = {'db_name': db_name, 'lang':lang}
        exec_hive(query % params)

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--db', default = 'a2v' )
    parser.add_argument('--langs', required = True,  help='comma seperated list of languages' )
    args = parser.parse_args()
    langs = args.langs.split(',')
    tables = ['page', 'redirect']
    db = args.db
    #sqoop_prod_dbs(db, langs, tables)
    create_multilingual_redirect(db)
    fill_multilingual_redirect(db, langs)