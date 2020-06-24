#!/usr/bin/env python3
from enum import Enum, unique
import sys

import fiona
import numpy as np
from netCDF4 import Dataset
from shapely.geometry import MultiPoint, Point
from shapely.ops import nearest_points
from twisted.internet import reactor, task, threads
from twisted.internet.endpoints import TCP4ServerEndpoint, serverFromString
from twisted.internet.protocol import Factory
from twisted.protocols.basic import LineReceiver
from twisted.python import log

from config import config


@unique
class GWGServerState(Enum):
    INITIALIZING = (1,)
    DISTRIBUTING = (2,)
    FINALIZING = 3


class GWGServerProtocol(LineReceiver):
    def connectionMade(self):
        if self.factory.state == GWGServerState.INITIALIZING:
            self.sendLine(b"INIT")
        elif self.factory.state == GWGServerState.DISTRIBUTING:
            try:
                val = next(self.factory.pool)
                point, id = val
                lat, lon = point
                export = "DATA:{}:{}:{}".format(lat, lon, id)
                self.sendLine(export.encode("utf-8"))
            except StopIteration:
                self.factory.transitionState()

        if self.factory.state == GWGServerState.FINALIZING:
            self.sendLine(b"COMPLETE")
            self.factory.shutdown()
        self.transport.loseConnection()


class GWGServerFactory(Factory):
    protocol = GWGServerProtocol

    def __init__(self):
        self.state = None
        self.clients = {}
        self.pool = None
        self.white_rabbit = False

    def transitionState(self, error=False):
        if error:
            self.state = GWGServerState.FINALIZING
        else:
            if self.state is None:
                self.state = GWGServerState.INITIALIZING
            elif self.state == GWGServerState.INITIALIZING:
                self.state = GWGServerState.DISTRIBUTING
            elif self.state == GWGServerState.DISTRIBUTING:
                self.state = GWGServerState.FINALIZING

    def initCompleted(self, result):
        self.pool = result
        self.transitionState()

    def shutdown(self):
        if not self.white_rabbit:
            self.white_rabbit = True
            task.deferLater(reactor, 30, reactor.stop)


def point_to_lat_lon(point):
    lat_i, lon_i = point
    lat = 89.75 - (lat_i / 2.0)
    lon = -179.75 + (lon_i / 2.0)
    return (lat, lon)


def find_valid_location_indexes(data, npfilter):
    # TODO remove hardcoded ranges
    valid_indexes = set()
    if data is not None:
        for lat in range(360):
            for lon in range(720):
                if data[lat, lon] != npfilter:
                    valid_indexes.add((lat, lon))
    return valid_indexes


def find_valid_points(entry):
    dataset = Dataset(entry["file"])
    future_points = []
    frame = dataset.variables[entry["netcdfVar"]][0, :, :]
    future_points = find_valid_location_indexes(frame, frame.fill_value)
    dataset.close()
    dataset = None
    return future_points


def load_map_points(points, prop, offset=0):
    mp = None
    with fiona.open(config["station_vector"], "r") as source:
        extracted_points = []
        ids = []
        for feature in source:
            if feature["geometry"]["type"] == "MultiPoint":
                extracted_points.extend(
                    [Point(p[0], p[1]) for p in feature["geometry"]["coordinates"]]
                )
                ids.extend(
                    [feature["properties"][prop]]
                    * len(feature["geometry"]["coordinates"])
                )
            if feature["geometry"]["type"] == "Point":
                extracted_points.append(
                    Point(
                        feature["geometry"]["coordinates"][0],
                        feature["geometry"]["coordinates"][1],
                    )
                )
                ids.append(feature["properties"][prop])
        mp = MultiPoint(extracted_points)
        str_points = [str(p) for p in extracted_points]
        prop_map = dict(zip(str_points, ids))
    return (prop_map, mp)


def get_data_from_nearest_point(target_point, prop_map, multipoint):
    lat_i, lon_i = target_point
    lat, lon = point_to_lat_lon(target_point)
    coords = Point(lon, lat)
    nearest = nearest_points(coords, multipoint)
    return (target_point, prop_map[str(nearest[1])])


def extract_data_from_point(data, conversions):
    # First check the data
    it = iter(data)
    target_len = len(next(it))
    if not all(len(point) == target_len for point in it):
        return False
    converted = np.empty_like(data)
    for i, f in enumerate(conversions):
        converted[i] = f(data[i])
    return converted


def initialize_data():
    print("Server data initializing.")
    all_valid_points = [find_valid_points(entry) for entry in config["mapping"]]
    valid_points = list(all_valid_points[0].intersection(*all_valid_points[1:]))
    print("> Number of valid points: {}".format(len(valid_points)))
    prop_map, multipoint = load_map_points(valid_points, config["station_id_field"])
    if "samples" not in config or config["samples"] is None:
        config["samples"] = len(valid_points)
    station_ids = (
        get_data_from_nearest_point(p, prop_map, multipoint)
        for p in valid_points[: config["samples"]]
    )
    print("Server data initialization complete.")
    return station_ids


def startup_successful(result):
    print("Server started.")
    factory.transitionState()
    station_ids = threads.deferToThread(initialize_data)
    station_ids.addCallback(factory.initCompleted)


def startup_failed(reason):
    log.err
    reactor.stop()


if __name__ == "__main__":
    server = sys.argv[1]
    port = sys.argv[2]
    endpoint = serverFromString(
        reactor, "tcp:" + port + ":interface=" + server
    )  # TCP4ServerEndpoint(reactor, 7781)
    factory = GWGServerFactory()
    e = endpoint.listen(factory)
    e.addCallback(startup_successful)
    e.addErrback(startup_failed)

    reactor.run()
