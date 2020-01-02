import argparse
from datetime import datetime
from math import radians, sin, atan2, sqrt, cos

from db_connection import TrackerType, MongoDbConnection


class DistanceCalculator:
    __radius_of_earth_in_km__ = 6373.0
    __tracker_type__ = TrackerType.GP

    __mongo_db_connection_gp_collection = MongoDbConnection().get_collection(__tracker_type__)

    def calculate_distance(self, start_time, end_time, truck_type, truck_size, truck_id):
        query = self.__generate_query(start_time, end_time, truck_type, truck_size, truck_id)
        print(query)
        truck_ids = self.__mongo_db_connection_gp_collection.find(query).distinct("truck_id")
        total_covered_distance = 0
        for current_truck_id in truck_ids:
            query_with_truck_id = self.__generate_query(start_time, end_time, truck_type, truck_size, current_truck_id)
            truck_locations = self.__mongo_db_connection_gp_collection.find(query_with_truck_id, {"location": 1}).sort(
                "created_at", 1)
            covered_distance = self.__get_distance_of_single_truck(truck_locations)
            total_covered_distance += covered_distance
            current_truck = self.__mongo_db_connection_gp_collection.find_one({"truck_id": current_truck_id})
            print("truck id =", current_truck_id, ", registration no =", current_truck["truck_registration_number"],
                  ", distance covered by =>", covered_distance, "KM")
        print("total covered distance for query =", total_covered_distance)

    def __get_distance_of_single_truck(self, truck_locations):
        previous_location = truck_locations[0]
        accumulated_distance = 0
        for truck_location in truck_locations:
            distance = self.__get_distance_from_location(previous_location, truck_location)
            # print(distance)
            accumulated_distance += distance
            previous_location = truck_location
        return accumulated_distance

    def __get_distance_from_location(self, location1, location2):
        coordinates1 = location1["location"]["coordinates"]
        coordinates2 = location2["location"]["coordinates"]
        return self.__get_distance(coordinates1[0], coordinates1[1], coordinates2[0], coordinates2[1])

    def __get_distance(self, longitude1, latitude1, longitude2, latitude2):
        lat1 = radians(latitude1)
        lon1 = radians(longitude1)
        lat2 = radians(latitude2)
        lon2 = radians(longitude2)

        diff_lon = lon2 - lon1
        diff_lat = lat2 - lat1

        a = sin(diff_lat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(diff_lon / 2) ** 2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))

        distance = self.__radius_of_earth_in_km__ * c
        return distance

    def __generate_query(self, start_time, end_time, truck_type, truck_size, truck_id):
        query = {}

        if start_time is not None:
            if not isinstance(start_time, datetime):
                raise TypeError('start_time must be an instance of ' + str(datetime))
            if "created_at" not in query.keys():
                query["created_at"] = {}
            query["created_at"]["$gte"] = start_time

        if end_time is not None:
            if not isinstance(end_time, datetime):
                raise TypeError('end_time must be an instance of ' + str(datetime))
            if "created_at" not in query.keys():
                query["created_at"] = {}
            query["created_at"]["$lte"] = end_time

        self.__load_field_value_to_query(query, "truck_type", truck_type, str)
        self.__load_field_value_to_query(query, "truck_size", truck_size, float)
        self.__load_field_value_to_query(query, "truck_id", truck_id, int)
        return query

    @staticmethod
    def __load_field_value_to_query(query, field_name, field_value, value_type):
        if field_value is not None:
            if not isinstance(field_value, value_type):
                raise TypeError(field_name + ' must be an instance of ' + str(value_type))
            query[field_name] = field_value


if __name__ == '__main__':
    __ap = argparse.ArgumentParser()
    __ap.add_argument("-s", "--start", required=False, help="Start date time (dd-mm-YYYY HH:MM:SS)")
    __ap.add_argument("-e", "--end", required=False, help="End date time (dd-mm-YYYY HH:MM:SS)")
    __ap.add_argument("-t", "--type", required=False, help="Truck type (string)")
    __ap.add_argument("-z", "--size", required=False, help="Truck size (float)")
    __ap.add_argument("-i", "--id", required=False, help="Truck id (integer)")
    __args = vars(__ap.parse_args())

    __start_str = __args['start'] if __args['start'] else None  # "24-12-2019 00:00:00"
    __start = datetime.strptime(__start_str, "%d-%m-%Y %H:%M:%S") if __start_str else None

    __end_str = __args['end'] if __args['end'] else None  # "24-12-2019 23:59:59"
    __end = datetime.strptime(__end_str, "%d-%m-%Y %H:%M:%S") if __end_str else None

    __truck_type = __args['type'] if __args['type'] else None

    __truck_size_str = __args['size'] if __args['size'] else None  # "3.0"
    __truck_size = float(__truck_size_str) if __truck_size_str else None

    __truck_id_str = __args['id'] if __args['id'] else None
    __truck_id = int(__truck_id_str) if __truck_id_str else None

    DistanceCalculator().calculate_distance(__start, __end, __truck_type, __truck_size, __truck_id)
