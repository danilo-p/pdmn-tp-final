import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import geopandas
import googlemaps
import os

gmaps = googlemaps.Client(key=os.environ.get('GOOGLE_MAPS_API_KEY'))

countries_codes_and_coordinates_df = pd.read_csv(
    "./output/countries_codes_and_coordinates.csv", skipinitialspace=True)


def find_country_data(geocoded_data):
    result = None
    for data in geocoded_data:
        if "country" in data["types"]:
            result = data
            break
    return result


def add_iso_a2(entry):
    country_name, count = entry

    geocoded_data = gmaps.geocode(country_name)
    if len(geocoded_data) == 0:
        return (None, country_name, count)

    country_geocoded = find_country_data(geocoded_data)
    if country_geocoded == None:
        return (None, country_name, count)

    address_geocoded = find_country_data(
        country_geocoded['address_components'])
    if address_geocoded == None:
        return (None, country_name, count)

    iso_a2 = address_geocoded['short_name']
    return (iso_a2, country_name, count)


def add_iso_a3(entry):
    iso_a2, country_name, count = entry

    filtered_df = countries_codes_and_coordinates_df[
        countries_codes_and_coordinates_df["Alpha-2 code"] == iso_a2]

    if len(filtered_df) == 0:
        return (None, iso_a2, country_name, count)

    iso_a3 = filtered_df["Alpha-3 code"].values[0]

    return (iso_a3, iso_a2, country_name, count)


def plot_choropleth_map(data):
    data_with_iso_a2 = [add_iso_a2(entry) for entry in data]

    data_with_iso_a3 = [add_iso_a3(entry) for entry in data_with_iso_a2]

    data_with_iso_a3_np = np.array(data_with_iso_a3)

    data_with_iso_a3_df = pd.DataFrame(data_with_iso_a3_np, columns=[
                                       'iso_a3', 'iso_a2', 'country_name', 'count'])

    naturalearth_lowres_df = geopandas.read_file(
        geopandas.datasets.get_path('naturalearth_lowres'))

    naturalearth_lowres_with_count = naturalearth_lowres_df.set_index(
        'iso_a3').join(data_with_iso_a3_df.set_index('iso_a3'))

    naturalearth_lowres_with_count["count"] = pd.to_numeric(
        naturalearth_lowres_with_count["count"])

    naturalearth_lowres_with_count["count"] = naturalearth_lowres_with_count["count"]\
        .fillna(0)

    naturalearth_lowres_with_count.plot("count", legend=True)
