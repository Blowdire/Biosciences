from pandas import DataFrame
import json
from multiprocessing import Pool
from tqdm import tqdm
from neo4j import GraphDatabase
import joblib
with open('./credentials.json', 'rb') as f:
    cred = json.load(f)
URI = "neo4j+s://12fed840.databases.neo4j.io"
AUTH = ("neo4j", cred['neo4j'])
driver = GraphDatabase.driver(URI, auth=AUTH)
session = driver.session()


def create_entity(tx, entity):
    tx.run("MERGE  (a:"+entity['type']+" {name: $name})", name=entity['name'])


def create_entity_relations(tx, entity, entity2):
    tx.run("MATCH  (a: "+entity['type']+" {name: $name}) MATCH (b: "+entity2['type'] +
           "{name: $name2}) MERGE (a)-[r:coOccurs]->(b)", type1=entity['type'], name=entity['name'], name2=entity2['name'])


def create_rels(document):
    for item in document.ents:
        for item2 in document.ents:
            session.execute_write(
                create_entity_relations, {'name': item.text, 'type': item.label_}, {'name': item2.text, 'type': item2.label_})


doc_processed = joblib.load('./doc_processed.dump')

if __name__ == '__main__':
    
    with Pool(6) as p:
        r = list(tqdm(p.imap(create_rels, doc_processed),
                 total=len(doc_processed)))
