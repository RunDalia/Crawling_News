# -*- coding: utf-8 -*-
import configparser
import pymongo
import urllib


class MongoDB():
    def __init__(self):
        config = configparser.ConfigParser()
        config.read('./config/config.conf', encoding='UTF8')
        self.config_mob = config['MongoDB']
        self.port = self.config_mob['port']
        
        
    def mongodb_primary(self):
        config_mob = self.config_mob
        try:
            host1 = config_mob['hostname1']
            host2 = config_mob['hostname2']
            host3 = config_mob['hostname3']
        except :
            host1 = config_mob['hostname1']
        port = self.port
        username = urllib.parse.quote_plus(config_mob['username'])
        password = urllib.parse.quote_plus(config_mob['password'])
        try:
            conn = pymongo.MongoClient(f'mongodb://{username}:{password}@{host1}:{port},{host2}:{port},{host3}:{port}/?replicaSet=twodigit')
        except :
            conn = pymongo.MongoClient(f'mongodb://{username}:{password}@{host1}:{port}/')
        
        return conn
        
        
    def mongodb_connaction(self, data, conn):
        config_mob = self.config_mob
        db = conn[config_mob['db_name']]
        collection = db[config_mob['table_name']]
        collection.insert_one(data)
        return collection
    
    def mongodb_connaction_pre(self, data, conn):
        config_mob = self.config_mob
        db2 = conn[config_mob['db_name2']]
        collection = db2[config_mob['table_name2']]
        collection.insert_one(data)
        return collection
        
if __name__ == '__main__':
    start = MongoDB()
    
    
    
    