
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

    def __init__(self, geosheet=None, outgeojson=None, outtxt=None):

        self.geosheet = geosheet
        self.outgeojson = outgeojson
        self.outtxt = open(outtxt, "w")


    def get_full_url(self):

        """
        This function is used to 1). create an unique location id; 2). retrieve the full geojson url
        :return: geoshee.csv with location id and geojson link
        """
        self.outtxt.write("Start retrieving geojson url.\n")
        print "Start retrieving geojson url."

        df = pd.read_csv(self.geosheet, encoding='utf-8', sep='\t')
        df.dropna(how="all", inplace=True)

        self.outtxt.write("Creating unique location id......\n")
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

        self.outtxt.write("Finish creating unique location id....\n")
        self.outtxt.write("Finish retrieving geojson url.\n")


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
        self.outtxt.write("Start merging geojson files...\n")

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


            injsonfile = json.load(open(infile))
            newjson = self.geom_check(injsonfile, project_id, location_id)

            #self.geom_check(injsonfile, project_id, location_id)

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


    def geom_check(self, injson, proj_id, loc_id):
        """
        :return:
        """
        # check geometry of individual geojson
        # line: not multiple lines in a geojson
        # polygon: not multiple polygons
        # points: not multiple points, no point features!!!!!!!!!!!!!
        # not combination of geometry types

        # input is the geojson file

        geom_types = ["LineString", "Polygon"]

        geoms = injson["features"]

        if len(geoms) != 1:  # [0]["properties"] = property_dict

            dest_geom_type = geoms[0]["geometry"]["type"]

            if dest_geom_type not in geom_types:

                print "Geometry Error: geometry types of project %s location %s is not correct." % (proj_id, loc_id)
                self.outtxt.write("Geometry Error: geometry types of project %s location %s is not correct. \n" % (proj_id, loc_id))
                return injson

            else:

                for i in range(1,len(geoms)):

                    new_geom_type = geoms[0]["geometry"]["type"]

                    if dest_geom_type != new_geom_type:

                        print "Geometry Error: there are multiple geometry types in project %s location %s"%(proj_id, loc_id)
                        self.outtxt.write("Geometry Error: there are multiple geometry types in project %s location %s \n"%(proj_id, loc_id))
                        return injson

                    else:

                        print "Geometry Error: there are multiple geometry features in project %s location %s" % (proj_id, loc_id)
                        self.outtxt.write("Geometry Error: there are multiple geometry features in project %s location %s \n" % (proj_id, loc_id))

                        if new_geom_type == "LineString":

                            self.outtxt.write("Correct multiline geometry for project %s location %s \n" % (proj_id, loc_id))
                            return self.connect_lines(injson)

                        else:
                            return injson

        else:
            return injson





    def connect_lines(self, injson):
        """
        This function is used to connect polylines that are supposed to be one polyline.
        :return:
        """

        newjson = dict(type='FeatureCollection', features=[])

        newjson["features"].append(copy.deepcopy(injson["features"][0]))

        coords = []

        for i in range(0, len(injson["features"])):
            coords = coords + injson["features"][i]["geometry"]["coordinates"]


        newjson["features"][0]["geometry"]["coordinates"] = coords

        return newjson






