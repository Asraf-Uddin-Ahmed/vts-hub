import datetime
import json
import sys

import requests

from db_connection import MongoDbConnection, MySqlConnection, TrackerType


def lambda_handler(event, context):
    __tracker_type__ = TrackerType.GP
    __url__ = "https://vts.grameenphone.com/123.html"
    __payload_format__ = '{ "route": "get-vehicle-track-data", "processID": 1, "mapName": "osm", "vehicle_id": "%s" }'

    mongo_db_connection = MongoDbConnection()

    my_sql_connection = MySqlConnection()
    tracker_data_list = my_sql_connection.get_all_trackers(__tracker_type__)

    for tracker_data in tracker_data_list:
        print(tracker_data, file=sys.stderr)
        payload = __payload_format__ % tracker_data['vehicle_id']
        response = requests.post(__url__, data=payload)
        response_body = json.loads(response.text.strip())
        print(response_body)
        merged_result = {
            "truck_id": tracker_data["truck_id"],
            "truck_type_id": tracker_data["truck_type"],
            "truck_type": tracker_data["type"],
            "truck_size_id": tracker_data["truck_size"],
            "truck_size": float(tracker_data["size"]),
            "truck_registration_number": tracker_data["truck_registration_number"],
            "location": {
                "type": "Point",
                "coordinates": []
            },
            "created_at": datetime.datetime.utcnow()
        }
        if response_body["vehicleTrackData"]:
            location_data = response_body["vehicleTrackData"][0]
            merged_result["location"]["coordinates"] = [float(location_data["LONGS"]), float(location_data["LAT"])]
            merged_result["vehicle_loc"] = location_data["VEHICLE_LOC"]
            merged_result["vendor_id"] = location_data["ID"]
            merged_result["speed"] = location_data["SPEED"]
            merged_result["head"] = location_data["HEAD"]
            merged_result["time_stamp"] = datetime.datetime.strptime(
                location_data["TIME_STAMP"].lower().replace('am', '').replace('pm', '').strip(), "%d-%m-%Y %H:%M:%S")
            merged_result["engine"] = location_data["ENGIN"]
            merged_result["max_speed"] = location_data["MAX_SPEED"]
            merged_result["is_camera_install"] = location_data["IS_CAMERA_INSTALL"]
        mongo_db_connection.get_collection(__tracker_type__).insert_one(merged_result)


if __name__ == '__main__':
    lambda_handler(None, None)
