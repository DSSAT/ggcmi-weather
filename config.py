from datetime import datetime

config = {
    "start_date": datetime(2011, 1, 1),
    "station_vector": "/ufrc/hoogenboom/share/ggcmi/shapes/05d_pt_land2.shp",
    "station_id_field": "ID",
    "output_dir": "/ufrc/hoogenboom/share/runs/cvillalobos/gwg/out",
    "mapping": [
        {
            "file": "/ufrc/hoogenboom/share/data/isimip/climate_land_only/climate3b/historical/GFDL-ESM4/gfdl-esm4_r1i1p1f1_w5e5_historical_rsds_global_daily_2011_2014.nc",
            "netcdfVar": "rsds",
            "dssatVar": "SRAD",
            "conversion": lambda x: x * 86400 / 1000000,
        },
        {
            "file": "/ufrc/hoogenboom/share/data/isimip/climate_land_only/climate3b/historical/GFDL-ESM4/gfdl-esm4_r1i1p1f1_w5e5_historical_tasmin_global_daily_2011_2014.nc",
            "netcdfVar": "tasmin",
            "dssatVar": "TMIN",
            "conversion": lambda x: x - 273.15,
        },
        {
            "file": "/ufrc/hoogenboom/share/data/isimip/climate_land_only/climate3b/historical/GFDL-ESM4/gfdl-esm4_r1i1p1f1_w5e5_historical_tasmax_global_daily_2011_2014.nc",
            "netcdfVar": "tasmax",
            "dssatVar": "TMAX",
            "conversion": lambda x: x - 273.15,
        },
        {
            "file": "/ufrc/hoogenboom/share/data/isimip/climate_land_only/climate3b/historical/GFDL-ESM4/gfdl-esm4_r1i1p1f1_w5e5_historical_pr_global_daily_2011_2014.nc",
            "netcdfVar": "pr",
            "dssatVar": "RAIN",
            "conversion": lambda x: x * 86400,
        },
    ],
}
