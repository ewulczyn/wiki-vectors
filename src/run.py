import argparse
import os, sys

"""
python run.py \
    --get_requests \
    --get_sessions \
    --get_models \
    --start 2016-02-01 \
    --stop  2016-02-01 \
    --release test2 \
    --langs all,en \
    --dims 10,20


python run.py \
    --get_requests \
    --start 2016-02-01 \
    --stop  2016-02-01 \
    --release test2 

python run.py \
    --get_sessions \
    --release test2 \
    --langs all,en 

python run.py \
    --get_models \
    --release test2 \
    --langs all,en \
    --dims 10,20
"""


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--get_requests', default = False, action ='store_true')
    parser.add_argument('--get_sessions', default = False, action ='store_true' )
    parser.add_argument('--get_models', default = False, action ='store_true' )
    parser.add_argument('--start', required = False )
    parser.add_argument('--stop', required = False)
    parser.add_argument('--release', required = True)
    parser.add_argument('--langs', required = False)
    parser.add_argument('--dims', required = False)
    args = vars(parser.parse_args())

    if args['get_requests']:
        if 'start' in args and 'stop' in args:

            cmd = """
            python /home/ellery/a2v/src/get_requests.py \
            --start %(start)s \
            --stop  %(stop)s \
            --release %(release)s \
            --priority
            """
            os.system(cmd % args)
        else:
            print('need start and stop to get_requests')
            sys.exit()

    if args['get_sessions']:

        if 'langs' in args:

            
            os.system("hadoop fs -mkdir /user/ellery/a2v/data/%(release)s" % args)

            cmd = """
            spark-submit \
                --driver-memory 5g \
                --master yarn \
                --deploy-mode client \
                --num-executors 10 \
                --executor-memory 10g \
                --executor-cores 4 \
                --queue priority \
            /home/ellery/a2v/src/get_sessions.py \
                --release %(release)s \
                --lang %(lang)s
            """

            for lang in args['langs'].split(','):
                args['lang'] = lang
                os.system(cmd % args)

        else:
            print('need langs to get sessions')
            sys.exit()


    if args['get_models']:

        if 'langs' in args and 'dims' in args:
            os.system("mkdir /home/ellery/a2v/data/%(release)s" % args)

            cmd = """
            python /home/ellery/a2v/src/get_vectors.py \
                --release %(release)s \
                --lang %(lang)s \
                --dims %(dim)s \
                --field %(field)s
            """

            for lang in args['langs'].split(','):
                args['lang'] = lang
                if lang == 'all':
                    args['field'] = 'id'
                else:
                    args['field'] = 'title'

                for dim in args['dims'].split(','):
                    args['dim'] = dim 
                    os.system(cmd % args)
        else:
            print('need langs and dims to get models')
            sys.exit()