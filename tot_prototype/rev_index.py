from pymongo import MongoClient
from stop_words import get_stop_words
from nltk.tokenize import RegexpTokenizer
from nltk.stem import WordNetLemmatizer

from context_managers import MongoDbService 
from collections import defaultdict


stop_words = get_stop_words('english')
wnl = WordNetLemmatizer()
tokenizer = RegexpTokenizer(r'\w+')

def strip_stop_words(msg):
    res = []
    for word in tokenizer.tokenize(msg):
        stem_word = wnl.lemmatize(word)
        clean_word = stem_word.strip().lower() 
        if clean_word in stop_words:
            continue
        res.append(clean_word)
    return res

def copy_words():
    with MongoDbService() as client:
        source = client['mysqlditc']
        dest = client['revindex']
        src_words = source.words
        dest_words = dest.words
        dest_words.remove()
        words = src_words.find()
        word_list = []
        for word in words:
            word_list.append(word)
        for word in word_list:
            print u'Processing: {}'.format(word)
            clean_list = strip_stop_words(word['word'])
            if not clean_list:
                continue
            word_clean = ''.join(clean_list)
            meaning = word['meaning']
            word_dict = dest_words.find_one({'word': word_clean})
            if word_dict:
                if word['meaning'] in word_dict.get('meanings', []):
                   pass
                else: 
                    dest_words.update({'word': word_clean}, {'$push': {'meanings': word['meaning']}})
            else:
                insert_dict = {}
                insert_dict['word'] = word_clean
                insert_dict['meanings'] = [meaning]
                dest_words.insert_one(insert_dict)


def build_rev_index():
    word_list = []
    with MongoDbService() as client:
        db = client['revindex']
        word_coll = db.words
        for word in word_coll.find():  
            word_list.append(word)

    for word in word_list:
        if word.get('revindex', None):
            continue

        ref_word_list = []
        for meaning in word['meanings']:
            words = strip_stop_words(meaning)
            ref_word_list.extend(words)

        for cw in set(ref_word_list):
            print u'Processing cleaned word: {}'.format(cw)
            with MongoDbService() as client:
                db = client['revindex']
                word_coll = db.words
                ref_word = word_coll.find_one({"word":cw})
                if ref_word:
                    word_coll.update({'_id': word['_id']}, {'$push': {'forward_ref': ref_word['_id']}})
                else:
                   print u'Missing: {} in DB'.format(cw)


def recommend(text):
    word_list = strip_stop_words(text)
    print u'Cleaned Words: {}'.format(word_list)
    with MongoDbService() as client:
        db = client['revindex']
        word_coll = db.words
        possible_words = defaultdict(int)
        db_words = []

        for w in word_list:
            db_word = word_coll.find_one({'word': w})
            if db_word:
                db_words.append(db_word)
        print u'Found words in DB: {}'.format(db_words)

        for w in db_words:
            for word in word_coll.find({'forward_ref': w['_id']}):
                possible_words[word['word']] += 1
        ranked = sorted(zip(possible_words, possible_words.values()), key=lambda x: x[1])
        return [x[0] for x in ranked]

