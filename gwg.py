from datetime import timedelta
import socket

import fiona
import numpy as np
from netCDF4 import Dataset
import pandas as pd
from shapely.geometry import MultiPoint, Point
from shapely.ops import nearest_points

from config import config


def point_to_lat_lon(point):
    lat_i, lon_i = point
    lat = 89.75 - (lat_i / 2.0)
    lon = -179.75 + (lon_i / 2.0)
    return (lat, lon)


def find_valid_location_indexes(data, npfilter):
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


def writeWeatherFile(point, station_id, station_data):
    host = socket.gethostname()
    print("Writing file for {} from host {}".format(str(point), host))
    daily_data, tavg, tamp = station_data
    file_path = config["output_path"] / "{}.WTH".format(station_id)
    raw_lat, raw_lon = point
    lat, lon = point_to_lat_lon(point)
    headers = ["{:>6}".format(e["dssatVar"]) for e in config["mapping"]]

    with file_path.open(mode="w") as fp:
        fp.write("*WEATHER DATA: ISIMIP Lat: {} Lon: {}\n\n".format(raw_lat, raw_lon))
        fp.write("@ INSI      LAT     LONG  ELEV   TAV   AMP REFHT WNDHT\n")
        fp.write(
            "  ISIM  {:> 7.2f}  {:> 7.2f}   -99 {:> 5.1f} {:> 5.1f}  1.00  1.00\n".format(
                lat, lon, tavg, tamp
            )
        )
        fp.write("@DATE{}\n".format("".join(headers)))
        for i, daily_values in enumerate(daily_data):
            td = timedelta(days=i)
            current_date = (config["start_date"] + td).strftime("%y%j")
            converted_dv = list(map("{:> 6.1f}".format, daily_values))
            fp.write("{0}{1}\n".format(current_date, "".join(converted_dv)))


def generateDSSATWeather(lat_i, lon_i, station_id):
    print("Starting generation")
    point = (lat_i, lon_i)
    data = np.array(
        [
            config["mapping"][i]["dataset"].variables[
                config["mapping"][i]["netcdfVar"]
            ][:, lat_i, lon_i]
            for i in range(len(config["mapping"]))
        ]
    )

    conversions = [e["conversion"] for e in config["mapping"]]
    converted = extract_data_from_point(data, conversions)

    prepared = converted.transpose(1, 0)
    df = pd.DataFrame(data=prepared, columns=["SRAD", "TMIN", "TMAX", "RAIN"])
    df["dateoffset"] = df.index
    df["date"] = config["start_date"] + pd.to_timedelta(df["dateoffset"], "d")
    df["tavg"] = (df["TMIN"] + df["TMAX"]) / 2
    del df["dateoffset"]
    df = df.set_index("date")

    monthly = df.resample("M").mean()

    tavg = monthly["tavg"].mean()
    tamp = monthly["tavg"].max() - monthly["tavg"].min()
    df = None
    station_data = (prepared, tavg, tamp)
    writeWeatherFile(point, station_id, station_data)
