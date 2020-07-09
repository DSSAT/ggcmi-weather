#!/usr/bin/env python

from enum import Enum, unique
import socketserver
import sys
import threading
import time

from config import config
from gwg import find_valid_points, load_map_points, get_data_from_nearest_point


@unique
class GWGServerState(Enum):
    INITIALIZING = (1,)
    DISTRIBUTING = (2,)
    FINALIZING = 3


# try:
#     val = next(self.factory.pool)
#     point, id = val
#     lat, lon = point
#     export = "DATA:{}:{}:{}".format(lat, lon, id)
#     self.sendLine(export.encode("utf-8"))
# except StopIteration:
#     self.factory.transitionState()


class GWGServerHandler(socketserver.StreamRequestHandler):
    def handle(self):
        response = "TAW"
        if self.server.state is None:
            response = "ALEPH"
        elif self.server.state == GWGServerState.INITIALIZING:
            response = "INIT"
        elif self.server.state == GWGServerState.DISTRIBUTING:
            try:
                val = next(self.server.pool)
                point, id = val
                lat, lon = point
                response = "DATA:{}:{}:{}".format(lat, lon, id)
            except StopIteration:
                self.server.transitionState()
        elif self.server.state == GWGServerState.FINALIZING:
            response = "DONE"
        else:
            response = "TAW"
        self.wfile.write(bytes(response, "utf-8"))


class GWGServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    def __init__(self, server_addr, handler):
        socketserver.TCPServer.__init__(self, server_addr, handler)
        self.state = None
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

    def initializeData(self):
        self.pool = initialize_data()
        self.transitionState()


def initialize_data():
    print("Server data initializing.")
    all_valid_points = [find_valid_points(entry) for entry in config["mapping"]]
    valid_points = list(all_valid_points[0].intersection(*all_valid_points[1:]))
    print("> Number of valid points: {}".format(len(valid_points)))
    prop_map, multipoint = load_map_points(valid_points, config["station_id_field"])
    if "samples" not in config or config["samples"] is None:
        config["samples"] = len(valid_points)
    station_ids = [
        get_data_from_nearest_point(p, prop_map, multipoint)
        for p in valid_points[: config["samples"]]
    ]
    print("Server data initialization complete.")
    return iter(station_ids)


if __name__ == "__main__":
    host = sys.argv[1]
    port = int(sys.argv[2])
    server = GWGServer((host, port), GWGServerHandler)
    with server:
        start_time = time.monotonic()
        main_thread = threading.Thread(target=server.serve_forever)
        main_thread.daemon = True
        main_thread.start()
        server.transitionState()
        server.initializeData()
        start_time = time.monotonic()
        print("Starting to serve data.")
        # We will actually use the generator here
        while server.state != GWGServerState.FINALIZING:
            pass
        end_time = time.monotonic()
        print("Finished serving data. It took", end_time - start_time, "seconds.")
        print("Resting for 35 seconds for any last connections.")
        time.sleep(35)
        end_time = time.monotonic()
        server.shutdown()
