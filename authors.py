import json
from multiprocessing import Pool
from tqdm import tqdm, tqdm_notebook
from neo4j import GraphDatabase
import joblib
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import json
from pandas import DataFrame


with open('./credentials.json', 'rb') as f:
    cred = json.load(f)
URI = "neo4j+s://12fed840.databases.neo4j.io"
AUTH = ("neo4j", cred['neo4j'])
driver = GraphDatabase.driver(URI, auth=AUTH)
session = driver.session()

client = MongoClient(cred['mongo'], server_api=ServerApi('1'))
client.admin.command('ping')
database = client['biosciences']

article_collection = database['articles']

articles = article_collection.find()
articles_df = DataFrame(articles)
articles_df = articles_df[articles_df.abstract.notna()]


def create_entity_relations_workedwith(tx, entity, entity2):
    tx.run(
        "MERGE  (a:Author {name: $name, surname: $surname}) MERGE (b:Author {name: $name2, surname: $surname2}) MERGE (a)-[r:WorkedWith]->(b)", name=entity['name'], name2=entity2['name'], surname=entity['surname'], surname2=entity2['surname'])


def create_entity_relations_uni(tx, entity):
    tx.run(
        "MERGE  (a:Author {name: $name, surname: $surname}) MERGE (b:University {name: $uniname}) MERGE (a)-[r:affiliation]->(b)", name=entity['name'], surname=entity['surname'], uniname=entity['uni'])


if __name__ == '__main__':
    for index, row in tqdm(articles_df.iterrows(), total=len(articles_df)):
        for author in row[2]:
            if author['affiliation'] and author['firstname'] and author['lastname']:
              try:
                session.execute_write(
                    create_entity_relations_uni, {'name': author['firstname'], 'surname': author['lastname'], 'uni': author['affiliation']})
              except:
                 None
            for author2 in row[2]:
                if author['firstname'] and author['lastname']:
                  try:
                    session.execute_write(
                        create_entity_relations_workedwith, {'name': author['firstname'], 'surname': author['lastname'], }, {'name': author2['firstname'], 'surname': author2['lastname'], })
                  except:
                     None
