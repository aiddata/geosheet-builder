
"""
Author: Miranda Lv
Date: 06/12/2018
Description:

"""



import pandas as pd
import urllib2
from bs4 import BeautifulSoup
import shortuuid
import numpy as np
import sys
import json
import os
import copy

class BuilderClass(object):

    def __init__(self, geosheet=None, outgeojson=None):

        self.geosheet = geosheet
        self.outgeojson = outgeojson


    def get_full_url(self):

        """
        This function is used to 1). create an unique location id; 2). retrieve the full geojson url
        :return: geoshee.csv with location id and geojson link
        """

        print "Start retrieving geojson url."

        df = pd.read_csv(self.geosheet, encoding='utf-8', sep='\t')
        df.dropna(how="all", inplace=True)

        print "Creating unique location id......"

        # Create an unique location id
        # ------------------------------
        sLength = len(df["project_id"])
        df['location_id'] = pd.Series(np.random.randn(sLength), index=df.index)
        df["location_id"] = df["location_id"].apply(lambda x: self.create_id())
        # ------------------------------

        # create a project_location id filed, which will be used for geojson file names
        df['project_location_id'] = df[['project_id', 'location_id']].apply(lambda x: '_'.join(str(v) for v in x), axis=1)

        # get the full geojson link
        df["full_url"] = df["GeoJSON Link or Feature ID"].apply(lambda x: self.get_geojson(x))


        grouped_df = df.groupby(["full_url", "project_location_id"])

        for name, group in grouped_df:

            self.get_feature_geojson(name[0], name[1])


        #df.to_csv(self.geosheet, sep='\t', encoding='utf-8', index=False)
        df.to_csv("test.tsv", sep='\t', encoding='utf-8', index=False)
        print "Finish creating unique location id...."
        print "Finish retrieving geojson url."


    def create_id(self):

        newid = shortuuid.uuid()

        return newid

    # get geojson url from gist
    def get_geojson(self, url):

        """
        :param url: The gist url from column "GeoJSON Link or Feature ID" in GeoSheet
        :return: The url for geocoded geojson file
        """

        baseurl = "https://gist.github.com"

        try:
            html = urllib2.urlopen(url).read()
            soup = BeautifulSoup(html, 'html.parser')
            element = soup.find("a", class_="btn btn-sm ")
            jsonurl = baseurl + element.attrs["href"]

        except:
            jsonurl = url

        return jsonurl

    # get geojson file for GeoBoundary features
    def get_feature_geojson(self, url, filename):

        try:
            req = urllib2.Request(url)
            response = urllib2.urlopen(req)
            content = response.read()
            jsv = json.loads(content)

            sys.path.append("../")
            filename = "location_data/source/" + filename + ".geojson"

            with open(filename, "w") as jsonfile:
                json.dump(jsv, jsonfile)

        except:

            print "Feature from GeoBoundaries."



    def build_dataset(self):

        self.get_full_url()


    def merge_geojson(self, inpath):

        """
        This function is used to merge multiple geojson files into one. Source: https://gist.github.com/migurski/3759608
        :param infiles: input geojson folder directory
        :return: no return, just an output file
        """

        print "Start merging geojson files..."

        infiles = [os.path.join(inpath,f) for f in os.listdir(inpath) if os.path.isfile(os.path.join(inpath, f)) and f.endswith("geojson")]

        outjson = dict(type='FeatureCollection', features=[])

        for infile in infiles:

            project_id = int(float(os.path.splitext(os.path.basename(infile))[0].split("_")[0]))
            location_id = str(os.path.splitext(os.path.basename(infile))[0].split("_")[1])

            # add project location id to the output geojson
            property_dict = dict()
            property_dict["project_id"] = project_id
            property_dict["location_id"] = location_id
            property_dict["project_location_id"] = "_".join([str(project_id),location_id])


            injson = json.load(open(infile))
            newjson = injson
            """
            if len(injson["features"])!=1:#[0]["properties"] = property_dict

                newjson = self.connect_lines(infile)

            else:
                print injson["features"][0]
                newjson = injson

            """

            if newjson.get('type', None) != 'FeatureCollection':
                raise Exception('Sorry, "%s" does not look like GeoJSON' % infile)

            if type(newjson.get('features', None)) != list:
                raise Exception('Sorry, "%s" does not look like GeoJSON' % infile)

            newjson["features"][0]["properties"] = property_dict

            outjson['features'] += newjson['features']

        print "-------------------------"
        print outjson["features"][0]

        encoder = json.JSONEncoder(separators=(',', ':'))
        encoded = encoder.iterencode(outjson)

        output = open(self.outgeojson, 'w')

        for token in encoded:

            output.write(token)


    def geom_check(self):
        """
        :return:
        """
        # check geometry of individual geojson
        # line: not multiple lines in a geojson
        # polygon: not multiple polygons
        # points: not multiple points, no point features!!!!!!!!!!!!!

        return






    def connect_lines(self, infile):
        """
        This function is used to connect polylines that are supposed to be one polyline.
        :return:
        """

        newjson = dict(type='FeatureCollection', features=[])

        injson = json.load(open(infile))

        newjson["features"].append(copy.deepcopy(injson["features"][0]))

        coords = []

        for i in range(0, len(injson["features"])):

            coords = coords + injson["features"][i]["geometry"]["coordinates"]


        newjson["features"][0]["geometry"]["coordinates"] = coords

        return newjson






