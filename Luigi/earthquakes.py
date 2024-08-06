# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
# pylint: disable=line-too-long

import pandas as pd
import folium
from folium.plugins import MarkerCluster
import luigi


# read data from inputs/earthquakes.csv and output the 10 largest earthquakes


class ReadEarthquakes(luigi.Task):
    def output(self):
        return luigi.LocalTarget('./Luigi/outputs/earthquakes.csv')

    def run(self):
        # read data from input file
        df = pd.read_csv('./Luigi/inputs/earthquakes.csv')
        # sort data by magnitude and get top 10 earthquakes
        df = df.sort_values(by='Magnitude', ascending=False).head(10)
        # write top 10 earthquakes to output file
        df.to_csv(self.output().path, index=False)


class CreateMap(luigi.Task):
    def requires(self):
        return ReadEarthquakes()

    def output(self):
        return luigi.LocalTarget('./Luigi/outputs/earthquake_map.html')

    def run(self):
        # read data from input file
        df = pd.read_csv(self.input().path)
        # create a map centered on the equator
        m = folium.Map(location=[0, 0], zoom_start=2)
        # create a marker cluster
        mc = MarkerCluster()
        # add markers for each earthquake to the cluster
        for index, row in df.iterrows():
            mc.add_child(folium.Marker(location=[row['Latitude'], row['Longitude']],
                                       popup='Magnitude: {magnitude}'.format(magnitude=row['Magnitude'])))
        # add marker cluster to map
        m.add_child(mc)
        # save map to output file
        m.save(self.output().path)


if __name__ == '__main__':
    luigi.build([CreateMap()])
