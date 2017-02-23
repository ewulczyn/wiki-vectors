import argparse
import os
import sys

"""
python etl.py \
--day 20160926 \
--langs en,ja,de,es,ru,fr,it,zh,pt,pl,tr,ar,nl,id,sv,ko,cs,fa,fi,vi
"""

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--day', required = False )
    parser.add_argument('--langs', required = False,  help='comma seperated list of languages')

    args = vars(parser.parse_args())
    print(args)


    if args['day']:
        cmd = """
        python /home/ellery/wmf/util/wikidata_utils.py \
            --day %(day)s \
            --download_dump
        """
        os.system(cmd % args)

        cmd = """
        spark-submit \
            --driver-memory 5g \
            --master yarn \
            --deploy-mode client \
            --num-executors 8 \
            --executor-memory 10g \
            --executor-cores 4 \
            --queue priority \
        /home/ellery/wmf/util/wikidata_utils.py \
            --day %(day)s \
            --extract_wills \
            --create_table \
            --db prod
        """
        os.system(cmd % args)

    if args['langs']:

        cmd = """
        python /home/ellery/wmf/util/get_multilingual_prod_db.py \
            --db prod \
            --langs %(langs)s \
            --tables page,redirect,page_props
        """
        os.system(cmd % args)

