import enum

import mysql.connector
import pymongo

from config import Config


class TrackerType(enum.Enum):
    M2M = 1
    NITOL = 2
    GP = 3


class MongoDbConnection:
    __mongodbClient = pymongo.MongoClient(
        "mongodb+srv://" + Config.MongoDb.USERNAME + ":" + Config.MongoDb.PASSWORD + "@cluster0-5f6ys.mongodb.net/test?retryWrites=true&w=majority")

    __dbList = __mongodbClient.list_database_names()
    if Config.MongoDb.DB_NAME not in __dbList:
        raise Exception("Database not exists: " + Config.MongoDb.DB_NAME)

    __currentDb = __mongodbClient[Config.MongoDb.DB_NAME]
    __collectionList = __currentDb.list_collection_names()

    if Config.MongoDb.COLLECTION_NAME_GP not in __collectionList:
        raise Exception("Collection not exists: " + Config.MongoDb.COLLECTION_NAME_GP)
    __collectionGp = __currentDb[Config.MongoDb.COLLECTION_NAME_GP]

    __collectionDict = {
        TrackerType.GP: __collectionGp
    }

    def get_collection(self, tracker_type):
        if not isinstance(tracker_type, TrackerType):
            raise TypeError('tracker_type must be an instance of TrackerType Enum')
        return self.__collectionDict[tracker_type]


class MySqlConnection:
    __mysqlClient = mysql.connector.connect(
        host=Config.MySql.HOST,
        user=Config.MySql.USER,
        passwd=Config.MySql.PASSWORD,
        database=Config.MySql.DATABASE
    )

    __cursorDict = __mysqlClient.cursor(dictionary=True)

    def get_all_trackers(self, tracker_type):
        if not isinstance(tracker_type, TrackerType):
            raise TypeError('tracker_type must be an instance of TrackerType Enum')
        sql = "SELECT * FROM truck_tracker INNER JOIN user_truck_details ON user_truck_details.id=truck_tracker.truck_id WHERE truck_tracker.is_deleted=false AND truck_tracker.tracker_type='" + tracker_type.name + "'"
        self.__cursorDict.execute(sql)
        return self.__cursorDict.fetchall()
