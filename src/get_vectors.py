import gensim
import scipy
import multiprocessing as mp
import time
import argparse
import subprocess
import json

"""
Given a file of sessions, train a word2vec model where articles::sessions and words::sentences
in the original formulation. We either learn embeddings for articles within a Wikipedia,
or for Wikidata items,depending on the lang parameter. When lang=wikidata, we learn Wikidata embeddings.

Usage:

python /home/ellery/a2v/src/get_vectors.py \
    --release test \
    --lang en \
    --field id \
    --dims 10 
""" 


class HDFSSentenceReader(object):
    def __init__(self, fname, field):
        self.fname = fname + '/*'
        self.field = field
    def __iter__(self):
        cat = subprocess.Popen(["hadoop", "fs", "-text",self.fname ], stdout=subprocess.PIPE)
        for line in cat.stdout:
            rs = json.loads(line.strip())
            yield [r[self.field] for r in rs]


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--release', required=True)
    parser.add_argument('--lang', required=True)
    parser.add_argument('--field', required=True)
    parser.add_argument('--dims', required=True)

    args = vars(parser.parse_args())

    for dim in args['dims'].split(','):
        args['dim'] = dim
        input_dir =  '/user/ellery/a2v/data/%(release)s/%(release)s_sessions_%(lang)s' % args
        m_output_dir = '/home/ellery/a2v/data/%(release)s/%(release)s_model_%(lang)s_%(dim)s' % args
        v_output_dir = '/home/ellery/a2v/data/%(release)s/%(release)s_%(lang)s_%(dim)s' % args
        
        sentences = HDFSSentenceReader(input_dir, args['field'])
        
        t1= time.time()

        model = gensim.models.Word2Vec( \
                sentences, \
                workers=10, \
                min_count=50, \
                size=int(args['dim'])
                )
        t2= time.time()
        print(t2-t1)

        model.save(m_output_dir)
        model.save_word2vec_format(v_output_dir)