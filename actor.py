from datetime import timedelta
import socket

from netCDF4 import Dataset
import numpy as np
import pandas as pd
import ray


@ray.remote
class GGCMIActor(object):
    def __init__(self, i, config, logger):
        self.config = config
        self.id = i
        self.logger = logger
        for entry in self.config["mapping"]:
            entry["dataset"] = Dataset(entry["file"])

    def __del__(self):
        pass

    def react(self, station_id):
        return "{} - Station ID: {}".format(self.id, station_id)

    def point_to_lat_lon(self, point):
        lat_i, lon_i = point
        lat = 89.75 - (lat_i / 2.0)
        lon = -179.75 + (lon_i / 2.0)
        return (lat, lon)

    def extract_data_from_point(self, data, conversions):
        # First check the data
        it = iter(data)
        target_len = len(next(it))
        if not all(len(point) == target_len for point in it):
            return False
        converted = np.empty_like(data)
        for i, f in enumerate(conversions):
            converted[i] = f(data[i])
        return converted

    def writeWeatherFile(self, point, station_id, station_data):
        host = socket.gethostname()
        print("Writing file for {} from host {}".format(str(point), host))
        daily_data, tavg, tamp = station_data
        file_path = self.config["output_path"] / "{}.WTH".format(station_id)
        raw_lat, raw_lon = point
        lat, lon = self.point_to_lat_lon(point)
        headers = ["{:>6}".format(e["dssatVar"]) for e in self.config["mapping"]]

        with file_path.open(mode="w") as fp:
            fp.write(
                "*WEATHER DATA: ISIMIP Lat: {} Lon: {}\n\n".format(raw_lat, raw_lon)
            )
            fp.write("@ INSI      LAT     LONG  ELEV   TAV   AMP REFHT WNDHT\n")
            fp.write(
                "  ISIM  {:> 7.2f}  {:> 7.2f}   -99 {:> 5.1f} {:> 5.1f}  1.00  1.00\n".format(
                    lat, lon, tavg, tamp
                )
            )
            fp.write("@DATE{}\n".format("".join(headers)))
            for i, daily_values in enumerate(daily_data):
                td = timedelta(days=i)
                current_date = (self.config["start_date"] + td).strftime("%y%j")
                converted_dv = list(map("{:> 6.1f}".format, daily_values))
                fp.write("{0}{1}\n".format(current_date, "".join(converted_dv)))

    def generateDSSATWeather(self, station_data):
        point, station_id = station_data
        lat_i, lon_i = point
        data = np.array(
            [
                self.config["mapping"][i]["dataset"].variables[
                    self.config["mapping"][i]["netcdfVar"]
                ][:, lat_i, lon_i]
                for i in range(len(self.config["mapping"]))
            ]
        )

        conversions = [e["conversion"] for e in self.config["mapping"]]
        converted = self.extract_data_from_point(data, conversions)

        prepared = converted.transpose(1, 0)
        df = pd.DataFrame(data=prepared, columns=["SRAD", "TMIN", "TMAX", "RAIN"])
        df["dateoffset"] = df.index
        df["date"] = self.config["start_date"] + pd.to_timedelta(df["dateoffset"], "d")
        df["tavg"] = (df["TMIN"] + df["TMAX"]) / 2
        del df["dateoffset"]
        df = df.set_index("date")

        monthly = df.resample("M").mean()

        tavg = monthly["tavg"].mean()
        tamp = monthly["tavg"].max() - monthly["tavg"].min()
        df = None
        station_data = (prepared, tavg, tamp)
        self.writeWeatherFile(point, station_id, station_data)
