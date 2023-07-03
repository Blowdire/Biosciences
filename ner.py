from pandarallel import pandarallel
import json
from multiprocessing import Pool
from tqdm import tqdm, tqdm_notebook
from neo4j import GraphDatabase
import joblib
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import json
from pandas import DataFrame
tqdm.pandas()
pandarallel.initialize(progress_bar=True)

with open('./credentials.json', 'rb') as f:
    cred = json.load(f)


client = MongoClient(cred['mongo'], server_api=ServerApi('1'))
client.admin.command('ping')
database = client['biosciences']

article_collection = database['articles']

articles = article_collection.find()
articles_df = DataFrame(articles)
articles_df = articles_df[articles_df.abstract.notna()]


def do_ner(item):
    print(item)


if __name__ == '__main__':
    res = articles_df['abstract'].parallel_apply(do_ner)
