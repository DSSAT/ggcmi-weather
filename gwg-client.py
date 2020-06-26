#!/usr/bin/env python
from pathlib import Path
import socket
import sys
import time

from netCDF4 import Dataset

from gwg import config, generateDSSATWeather


def process_data(raw_data):
    lat, lon, station_id = tuple(raw_data.split(":")[1:])
    lat_i = int(lat)
    lon_i = int(lon)
    generateDSSATWeather(lat_i, lon_i, station_id)


def connect(s_addr, s_port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.connect((s_addr, s_port))
            response = str(s.recv(4096), "utf-8")
            if response.startswith("DATA"):
                process_data(response)
                return True
            elif response == "DONE":
                return False
            else:
                time.sleep(5)
                return True
        except Exception:
            return False


if __name__ == "__main__":
    server = sys.argv[1]
    port = int(sys.argv[2])
    config["output_path"] = Path(config["output_dir"])
    config["output_path"].mkdir(parents=True, exist_ok=True)
    for entry in config["mapping"]:
        entry["dataset"] = Dataset(entry["file"])
    con_status = connect(server, port)
    while con_status:
        con_status = connect(server, port)
    for entry in config["mapping"]:
        entry["dataset"].close()
        entry["dataset"] = None
