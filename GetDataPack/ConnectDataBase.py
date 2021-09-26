

import configparser
import pymssql as pysql
import os


class ConnectDataBase:
    def __init__(self):
        pass

    def connect_database(self,flag='JYDB-Test'):
        cf = configparser.ConfigParser()
        cf.read("D:\\AnacondaProject\\Barra\\GetDataPack\\DataBase.ini")
        database_info = cf.items(flag)
        database_info_dic = {loc[0]: loc[1] for loc in database_info}

        conn = pysql.connect(host=database_info_dic['host'], user=database_info_dic['user'],
                             password=database_info_dic['password'], database=database_info_dic['db'],
                             charset=database_info_dic['charset'])
        return conn

if __name__=="__main__":
    connect_data_base_demo = ConnectDataBase()
    connect_data_base_demo.connect_database()