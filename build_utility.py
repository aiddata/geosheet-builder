
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
import geopandas as gpd
from shapely.geometry import LineString, shape, asShape
import geojson
from pandas.io.json import json_normalize
import shapely
import warnings


class BuilderClass(object):

    def __init__(self, geosheet=None, outgeojson=None, outtxt=None, geoboundaries=None, newgeosheet=None,
                 country=None, countryadm0=None, transactions=None, deflation_file=None, projects=None):

        self.geosheet = pd.read_csv(geosheet, encoding='utf-8', sep='\t')
        self.outgeojson = outgeojson
        self.geoboundaries = geoboundaries
        self.outtxt = open(outtxt, "w")
        self.newgeosheet = newgeosheet
        self.country = country
        self.country_geom = gpd.read_file(countryadm0)['geometry'][0]
        self.transactions = pd.read_csv(transactions, encoding='utf-8', sep=',')
        self.deflation_file = deflation_file
        self.projects = pd.read_csv(projects, encoding='utf-8', sep=',')



    def build_dataset(self):

        currentpath = os.getcwd()
        alljsonpath = os.path.join(currentpath, 'processing', 'geographic')

        self.merge_geojson(alljsonpath)
        self.merge_ancillary()
        self.geojson2shp()

        infiles = [os.path.join(alljsonpath, f) for f in os.listdir(alljsonpath) if
                   os.path.isfile(os.path.join(alljsonpath, f)) and f.endswith("geojson")]

        for file in infiles:

            file_geom = gpd.read_file(file)['geometry'][0]

            if (not shape(self.country_geom).contains(shape(file_geom))) or shape(self.country_geom).overlaps(shape(file_geom)):

                warnings.warn("Polygon %s is out of the country.."%(os.path.basename(file)))





    def get_full_url(self):

        """
        This function is used to 1). create an unique location id; 2). retrieve the full geojson url
        :return: geosheet.csv with location id and geojson link
        """
        self.outtxt.write("Start retrieving geojson url.\n")
        print "Start retrieving geojson url."

        #df = pd.read_csv(self.geosheet, encoding='utf-8', sep='\t')
        self.geosheet.dropna(how="all", inplace=True)

        self.outtxt.write("Creating unique location id......\n")
        print "Creating unique location id......"

        # convert project id to integer
        self.geosheet["project_id"] = self.geosheet["project_id"].astype(int)

        # Create an unique location id
        # ------------------------------
        sLength = len(self.geosheet["project_id"])
        self.geosheet['location_id'] = pd.Series(np.random.randn(sLength), index=self.geosheet.index)
        self.geosheet["location_id"] = self.geosheet["location_id"].apply(lambda x: self.create_id())
        # ------------------------------

        # create a project_location id filed, which will be used for geojson file names
        self.geosheet['project_location_id'] = self.geosheet[['project_id', 'location_id']].apply(lambda x: '_'.join(str(v) for v in x), axis=1)

        # get the full geojson link
        self.geosheet["full_url"] = self.geosheet["GeoJSON Link or Feature ID"].apply(lambda x: self.get_geojson(x))

        # save the geosheet under processing/ancillary, this geosheet has unique location id, and directory geojson urls
        self.geosheet.to_csv(self.newgeosheet, encoding='utf-8', sep='\t', index=False)

        grouped_df = self.geosheet.groupby(["full_url", "project_location_id"])

        for name, group in grouped_df:

                self.get_feature_geojson(name[0], name[1])


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

            filename = "processing/geographic/" + filename + ".geojson"

            with open(filename, "w") as jsonfile:
                json.dump(jsv, jsonfile)

        except:

            print "Feature from GeoBoundaries."
            self.get_geoboundary_feature(url, filename)



    def merge_geojson(self, inpath):

        """
        This function is used to merge multiple geojson files into one. Source: https://gist.github.com/migurski/3759608
        :param infiles: input geojson folder directory
        :return: a geojson file includes all geocoded location geojsons.
        """

        print "Start merging geojson files..."
        self.outtxt.write("Start merging geojson files...\n")

        infiles = [os.path.join(inpath,f) for f in os.listdir(inpath) if os.path.isfile(os.path.join(inpath, f)) and f.endswith("geojson")]

        outjson = dict(type='FeatureCollection', features=[])

        count = 0
        for infile in infiles:

            count += 1

            project_id = int(float(os.path.splitext(os.path.basename(infile))[0].split("_")[0]))
            location_id = str(os.path.splitext(os.path.basename(infile))[0].split("_")[1])

            # add project location id to the output geojson
            property_dict = dict()
            property_dict["project_id"] = project_id
            property_dict["location_id"] = location_id
            property_dict["project_location_id"] = "_".join([str(project_id),location_id])


            injsonfile = json.load(open(infile))
            newjson = self.geom_check(injsonfile, project_id, location_id)


            if newjson["features"][0]["geometry"]["type"] == "LineString":
                newjson = self.buffer_line(newjson)

            if newjson.get('type', None) != 'FeatureCollection':
                raise Exception('Sorry, "%s" does not look like GeoJSON' % infile)

            if type(newjson.get('features', None)) != list:
                raise Exception('Sorry, "%s" does not look like GeoJSON' % infile)


            newjson["features"][0]["properties"] = property_dict
            outjson['features'] += newjson['features']

        print "-------------------------"

        print "There are %s geocoded locations"%(count)

        json.dumps(outjson)
        output = open(self.outgeojson, "w")
        json.dump(outjson, output)
        output.close()


    def merge_ancillary(self):

        """
        :return: This function is used to merge geojson file with geosheet, transaction info, and calculate even split commitment to each location
        """

        # add location count information to geojson file
        merged_geojson = gpd.read_file(self.outgeojson)

        count_series = merged_geojson.project_id.value_counts()
        count_df = pd.DataFrame(count_series)
        count_df['merge_id'] = count_df.index
        count_df['counts'] = count_df.project_id
        del count_df['project_id'] # this project_id field is similar to location count, should be deleted
        merge_df = merged_geojson.merge(count_df, how='left', left_on='project_id', right_on='merge_id')

        # add geosheet to geojson file
        newgeosheet = pd.read_csv(self.newgeosheet, encoding='utf-8', sep='\t')
        outdf = merge_df.merge(newgeosheet, how='left', on='project_location_id')

        # add transaction info to geojson file
        full_df = outdf.merge(self.transactions, how='left', left_on='project_id_x', right_on='project_id')
        full_df['even_split_commitment'] = full_df.transaction_value / full_df.counts
        full_df['location_id'] = full_df['location_id_x']


        keep_columns = ['project_location_id', 'project_id', 'location_id', 'Location Name',
                         'Identified Location Type',
                         'Geocoded Location Type', 'Source URL', 'GeoJSON Link or Feature ID',
                         'Geoparsing Notes', 'Geocoding and Review Note', 'full_url', 'geometry',
                         'transaction_value', 'even_split_commitment']

        full_df = pd.DataFrame(full_df[keep_columns], columns=keep_columns)

        # merge project level information
        full_proj_df = full_df.merge(self.projects, how='left', on='project_id').set_geometry('geometry')

        with open(self.outgeojson, "wb") as output:
            json.dump(json.loads(full_proj_df.to_json()), output)


    def geom_check(self, injson, proj_id, loc_id):
        """
        This function is used to check the geometry of each geocoded locations.
            - geojson cannot be point feature
            - geometry cannot be a combination of multiple types
            - geometry cannot have multi-features
        :param injson: individual geojson
        :param proj_id: project id
        :param loc_id: location id
        :return: geojson file with fixed geometry (multi-line features) or geometry checked
        """

        geom_types = ["LineString", "Polygon"]

        geoms = injson["features"]

        if len(geoms) != 1: # multi-features

            dest_geom_type = geoms[0]["geometry"]["type"]

            if dest_geom_type not in geom_types: # cannot be point feature

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
        :param injson: the individual geojson that has multiple line features
        :return: one line feature
        """

        newjson = dict(type='FeatureCollection', features=[])

        newjson["features"].append(copy.deepcopy(injson["features"][0]))

        coords = []

        for i in range(0, len(injson["features"])):
            coords = coords + injson["features"][i]["geometry"]["coordinates"]


        newjson["features"][0]["geometry"]["coordinates"] = coords

        return newjson


    def get_geoboundary_feature(self, feat_id, proj_loc_id):

        """
        :param countryjsons:
            - the folder that has the country administrative boundaries
            - the administrative geoboundary geojson file
        :param feat_id: the identical feature id that is used to track the location feature
        :param proj_loc_id: the proj_loc_id field
        :return: a geojson file of feature
        """

        try:
            country = feat_id.split("_")[0]
            adms = feat_id.split("_")[1]

            jsonpath = os.path.join(self.geoboundaries, "%s/%s_%s/%s_%s.geojson"%(country, country, adms, country, adms))

            geo_data = gpd.read_file(jsonpath)
            feat_geo = geo_data[geo_data["feature_id"] == feat_id]

            filename = "processing/geographic/" + proj_loc_id + ".geojson"

            with open(filename, "wb") as output:

                json.dump(json.loads(feat_geo.to_json()), output)

        except:


            pass



    def buffer_line(self, injson):

        """
        :param injson: the input geojson file is a LineString feature
        :return: a buffered line feature, default buffer distance is 0.0001 degree, around 10 meters
        """

        line = LineString(injson["features"][0]["geometry"]["coordinates"])
        buffered_line = shape(line).buffer(0.0001).__geo_interface__

        # the geo_interface returns geometries in tuple pairs
        # however the geojson has list pairs geometries

        poly_tuples = buffered_line["coordinates"][0]
        poly_lists = [[list(i) for i in poly_tuples]]

        injson["features"][0]["geometry"]["coordinates"]= poly_lists
        injson["features"][0]["geometry"]["type"] = "Polygon"

        return injson


    # validate geojson
    def parse(self, text):
        try:
            return json.loads(text)
        except ValueError as e:
            print('invalid json: %s' % e)
            return None  # or: raise


    def add_count(self):

        # count the number of line features that have been buffered
        # count the number of polygon features
        # count the total number of final product

        return


    def location_type_check(self):

        """
        This function is used to check the identified location type with geocoded location type
        :return: write into report.txt
        """
        newdf = pd.read_csv(self.newgeosheet, encoding='utf-8', sep='\t')

        newdf['discrepancy_check']= newdf.apply(lambda x: 'False' if x['Identified Location Type']!=x['Geocoded Location Type'] else 'True', axis=1) #

        loc_type_fail= newdf[newdf['discrepancy_check']=='False']

        grouped_df = loc_type_fail.groupby(["project_id", "location_id"])

        self.outtxt.write("Start checking the discrepancy of identified location type with geocoded location type.\n")
        for name, group in grouped_df:

            self.outtxt.write("Location type discrepancy check failed for project %s and location %s...\n"%(name[0], name[1]))

        return

    def spatial_scrub(self, geocoded_geom):

        """
        :param geom: geometry of geocoded feature
        :param country_geom: geometry of destination country
        :return:
        """

        if not shape(self.country_geom).contains(shape(geocoded_geom)):

            raise ('not passed spatial scrub')


    def deflation(self, year, currency, val):

        deflation_sheet = pd.read_csv(self.deflation_file, encoding='utf-8', sep='\t')
        df_iso = deflation_sheet[deflation_sheet['currency_val'] == currency]

        if df_iso.empty:
            raise ('donor_iso3 \'%s\' not found in deflator table' % (currency))
        else:

            try:
                def_val = df_iso.loc[df_iso['transaction_year'] == year, 'deflator'][0]
                deflated_val = def_val * val
            except KeyError:
                raise ("Year not found for: %s in the year of %s"%(currency, year))

        return deflated_val


    def geojson2shp(self):

        """
        This script is to convert the merged geojson file to shapefile
        :return: shapefile under the same directory of geojson file
        """

        df = gpd.read_file(self.outgeojson)
        gdf = gpd.GeoDataFrame(df, geometry=df.geometry)
        gdf.crs = {'init': 'epsg:4326'}
        filename = os.path.join(os.path.dirname(self.outgeojson), 'merged_locations.shp')

        gdf.to_file(driver='ESRI Shapefile', filename=filename)




