from peewee import *
from pymongo import MongoClient

mysql_db = MySQLDatabase('mysqldict', user='root')


class Entries(Model):
    class Meta:
        database = mysql_db

    word = CharField()
    wordtype = CharField()
    definition = TextField()


def migrate():
    mysql_db.connect()
    mongo_client = MongoClient('localhost', 27017)
    mongo_db = mongo_client['mysqlditc']
    words = mongo_db.words
    try:
        for word in Entries.select():
            word_dict = {
                "word": word.word,
                "meaning": word.definition, 
                "type": word.wordtype,
            }
            word_id = words.insert_one(word_dict).inserted_id
            print "Inserted Word ID: {}".format(word_id)
    except Exception as e:
        print e
    finally:
        mysql_db.close()

migrate()
