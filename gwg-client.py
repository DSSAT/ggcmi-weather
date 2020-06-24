#!/usr/bin/env python
from datetime import timedelta
from pathlib import Path
import socket
import sys

from netCDF4 import Dataset
import numpy as np
import pandas as pd
from twisted.internet import reactor, threads
from twisted.internet.protocol import ClientFactory
from twisted.protocols.basic import LineReceiver

from config import config


class GWGClientProtocol(LineReceiver):
    def lineReceived(self, string):
        res = string.decode("utf-8")
        if res == "INIT":
            reconnect(5)
        elif res == "COMPLETE":
            reactor.stop()
        else:
            process_data(res)


class GWGClientFactory(ClientFactory):
    def __init__(self):
        pass

    def buildProtocol(self, addr):
        return GWGClientProtocol()

    def clientConnectionLost(self, connector, reason):
        pass

    def clientConnectionFailed(self, connector, reason):
        print("Connection failed. Reason:", reason)
        reactor.stop()


def point_to_lat_lon(point):
    lat_i, lon_i = point
    lat = 89.75 - (lat_i / 2.0)
    lon = -179.75 + (lon_i / 2.0)
    return (lat, lon)


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


def connect(server, port, factory):
    reactor.connectTCP(server, port, factory)


def reconnect(retry_time=None):
    if retry_time is None:
        connect(server, port, factory)
    else:
        print("Attempting to reconnect in", retry_time, "seconds")
        reactor.callLater(retry_time, connect, server, port, factory)


def process_data(raw_data):
    print(raw_data)
    lat, lon, station_id = tuple(raw_data.split(":")[1:])
    lat_i = int(lat)
    lon_i = int(lon)
    t = threads.deferToThread(generateDSSATWeather, lat_i, lon_i, station_id)
    t.addCallback(reconnect)


if __name__ == "__main__":
    server = sys.argv[1]
    port = int(sys.argv[2])
    factory = GWGClientFactory()
    print("Starting")
    config["output_path"] = Path(config["output_dir"])
    config["output_path"].mkdir(parents=True, exist_ok=True)
    for entry in config["mapping"]:
        entry["dataset"] = Dataset(entry["file"])
    connect(server, port, factory)
    reactor.run()
    for entry in config["mapping"]:
        entry["dataset"].close()
        entry["dataset"] = None
