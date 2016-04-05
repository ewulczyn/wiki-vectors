import argparse
import os


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--day', required = False )
    parser.add_argument('--langs', required = False,  help='comma seperated list of languages')
    args = vars(parser.parse_args())

    if 'day' in args:
        cmd = """
        python /home/ellery/wmf/util/wikidata_utils.py \
            --day %(day)s \
            --dowload_dump
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
            --db a2v
        """
        os.system(cmd % args)

    if 'langs' in args:

        cmd = """
        python /home/ellery/a2v/src/get_multilingual_redirect.py \
            --langs %(langs)s
        """
        os.system(cmd % args)

