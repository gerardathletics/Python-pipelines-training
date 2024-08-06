# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
# pylint: disable=line-too-long

from branca.colormap import LinearColormap
import pandas as pd
import folium
from folium.plugins import MarkerCluster
import luigi


# read data from inputs/earthquakes.csv and output the X largest earthquakes
num_earthquakes = 50


class ReadEarthquakes(luigi.Task):
    def output(self):
        return luigi.LocalTarget(f'./outputs/{num_earthquakes}_earthquakes.csv')

    def run(self):
        # read data from input file
        df = pd.read_csv('./inputs/earthquakes.csv')
        # sort data by magnitude and get top earthquakes
        df = df.sort_values(
            by='Magnitude', ascending=False).head(num_earthquakes)
        # write top 10 earthquakes to output file
        df.to_csv(self.output().path, index=False)


class CreateMap(luigi.Task):
    def requires(self):
        return ReadEarthquakes()

    def output(self):
        return luigi.LocalTarget(f'./outputs/{num_earthquakes}_earthquake_map.html')

    def run(self):
        # read data from input file
        df = pd.read_csv(self.input().path)

        # create a map centered on the mean latitude and longitude
        mean_lat = df['Latitude'].mean()
        mean_lon = df['Longitude'].mean()
        m = folium.Map(location=[mean_lat, mean_lon], zoom_start=4)

        # calculate min and max magnitude for scaling
        min_mag = df['Magnitude'].min()
        max_mag = df['Magnitude'].max()

        # create a color map
        color_map = LinearColormap(colors=['yellow', 'orange', 'red'],
                                   vmin=min_mag, vmax=max_mag)

        color_map.add_to(m)
        color_map.caption = 'Earthquake Magnitude'

        # add markers for each earthquake
        for index, row in df.iterrows():
            # scale magnitude
            scaled_radius = 8 + \
                ((row['Magnitude'] - min_mag) / (max_mag - min_mag)) * 25

            color = color_map(row['Magnitude'])

            folium.CircleMarker(
                location=[row['Latitude'], row['Longitude']],
                radius=scaled_radius,
                popup=f"Magnitude: {row['Magnitude']:.2f}",
                tooltip=f"Magnitude: {row['Magnitude']:.2f}",
                color=color,
                fill=True,
                fill_color=color,
                fill_opacity=0.7
            ).add_to(m)

        # save map to output file
        m.save(self.output().path)


if __name__ == '__main__':
    luigi.build([CreateMap()])
