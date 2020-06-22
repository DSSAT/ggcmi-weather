#!/usr/bin/env python
import itertools
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import fiona
import numpy as np
import ray
from ray.util import ActorPool
from netCDF4 import Dataset
from shapely.geometry import MultiPoint, Point
from shapely.ops import nearest_points

from actor import GGCMIActor

# Start user configuration
config = {
    "start_date": datetime(2011, 1, 1),
    "station_vector": "../data/Grid/05d_pt_land2.shp",
    "station_id_field": "ID",
    "output_dir": "../output",
    "samples": 100,
    "mapping": [
        {
            "file": "../data/ISIMIP/gfdl-esm4_r1i1p1f1_w5e5_historical_rsds_global_daily_2011_2014.nc",
            "netcdfVar": "rsds",
            "dssatVar": "SRAD",
            "conversion": lambda x: x * 86400 / 1000000,
        },
        {
            "file": "../data/ISIMIP/gfdl-esm4_r1i1p1f1_w5e5_historical_tasmin_global_daily_2011_2014.nc",
            "netcdfVar": "tasmin",
            "dssatVar": "TMIN",
            "conversion": lambda x: x - 273.15,
        },
        {
            "file": "../data/ISIMIP/gfdl-esm4_r1i1p1f1_w5e5_historical_tasmax_global_daily_2011_2014.nc",
            "netcdfVar": "tasmax",
            "dssatVar": "TMAX",
            "conversion": lambda x: x - 273.15,
        },
        {
            "file": "../data/ISIMIP/gfdl-esm4_r1i1p1f1_w5e5_historical_pr_global_daily_2011_2014.nc",
            "netcdfVar": "pr",
            "dssatVar": "RAIN",
            "conversion": lambda x: x * 86400,
        },
    ],
}
# end configuration


# DO NOT edit past this point
# Lasciate ogne speranza, voi ch'intrate - Dante
def check_config():
    is_valid = True
    logger.debug("Checking the configuration")
    logger.debug("==========================")
    # Validate all the values exist
    if config["mapping"] is None:
        logger.error("Missing mapping")
        is_valid = False
    # Validate the shape of mapping is correct
    # Validate all the files references exist
    return is_valid


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


# Really entering into the darkside of the moon here
if __name__ == "__main__":
    print("Handling preflight")
    dts = datetime.now().strftime("%Y%m%d")
    logging.basicConfig(
        level=logging.DEBUG,
        filename="gw-{0}-{1}.log".format(dts, os.getpid()),
        filemode="w",
    )
    logger = logging.getLogger("ggcmi-weather")

    if not check_config():
        print("Invalid configuration, please check the log for more information")
        sys.exit(1)

    output_path = Path(config["output_dir"])
    config["output_path"] = output_path
    output_path.mkdir(parents=True, exist_ok=True)

    start_time = time.monotonic()
    print("Finding all valid points in the netCDF files")
    all_valid_points = [find_valid_points(entry) for entry in config["mapping"]]
    valid_points = list(all_valid_points[0].intersection(*all_valid_points[1:]))
    print("> Number of valid points: {}".format(len(valid_points)))
    prop_map, multipoint = load_map_points(valid_points, config["station_id_field"])
    checkpoint_time = time.monotonic()
    if "samples" not in config or config["samples"] is None:
        config["samples"] = len(valid_points)

    print("Spawning worker threads")
    ray.init()
    actors = [GGCMIActor.remote(i, config, logger) for i in range(8)]
    print("Starting station processing")
    pool = ActorPool(actors)
    station_ids = (
        get_data_from_nearest_point(p, prop_map, multipoint) for p in valid_points
    )

    for station_data in itertools.islice(station_ids, config["samples"]):
        pool.submit(lambda a, s: a.generateDSSATWeather.remote(s), station_data)

    while pool.has_next():
        print(pool.get_next_unordered())

    end_time = time.monotonic()
    print(
        "> Total runtime for {} points: {} seconds".format(
            config["samples"], end_time - start_time
        )
    )


# The design SHOULD be as follows:
# DONE The controller should generate the point and distance map
# DONE Controller accesses nearest point value and submits it to the actorpool for processing
# PENDING GGCMIActors are initialized with the config
# TODO Each actor spins up and opens the required file(s) for localized reading
# TODO The controller joins all the actors to an ActorPool
