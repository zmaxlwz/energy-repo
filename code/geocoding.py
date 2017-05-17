import sys
import googlemaps as gmaps
import psycopg2
import csv
import json

class GeoCoding:
    def __init__(self, key):
        """ initialize variables

        """
        self.key = key
        self.client = gmaps.Client(key = self.key)
        #self.table_name = 'streets_reverse_geocoded'

        self.pg_dbname = "los_angeles"
        self.pg_username = "awsmaster"
        self.pg_password = "philips2017"
        self.pg_host = "citytouch-buenos-aires-log.cuxwb2nbset5.us-west-2.rds.amazonaws.com"
        self.pg_port = "5432"

    def connect_db(self):
        """ build connection to the database

        """    
        #connect to the database
        try:
            print(self.pg_dbname)
            self.conn = psycopg2.connect("dbname=%s user=%s password=%s host=%s port=%s" % (self.pg_dbname, self.pg_username, self.pg_password, self.pg_host, self.pg_port))
            print("connected!")
        except psycopg2.Error as e:
            print("I am unable to connect to the database")
            print(e)

        #define cursor
        self.cur = self.conn.cursor() 

    def disconnect_db(self):
        """ close db connection

        """    
        self.cur.close()
        self.conn.close()

    def getAssetsList(self):
        """ get assets list from assets table, 
            for LA, we are interested in the assets that are not deleted and commissioning_date are not null

        """                
        try:
            self.cur.execute("select id, latitude, longitude \
                              from assets \
                              where is_deleted = 'f' \
                              and commissioning_date is not null")
        except:
            print("I am unable to get data")

        rows = self.cur.fetchall()  

        assets_list = []
        for row in rows:
            asset_id = row[0]
            asset_latitude = row[1]
            asset_longitude = row[2]
            assets_list.append((asset_id, asset_latitude, asset_longitude))

        return assets_list

    def reverseGeocoding(self, assets_list):
        """ perform reverse geocoding for each asset, each item is a tuple including asset_id, asset_latitude and asset_longitude

        """
        
        count = 0
        for row in assets_list:
            count += 1            
            if count > 4:
                break
            print(count)    
            asset_id = row[0]
            asset_latitude = row[1]
            asset_longitude = row[2]
            reverse_addr = self.client.reverse_geocode((float(asset_latitude), float(asset_longitude)))
            try:
                #the returned address is not empty                             
                address_component = reverse_addr[0]['address_components']
                #append street name
                street_name = [obj['long_name'] for obj in address_component if obj['types'][0] == 'route']        
                #field_list.append(street_name[0])
                street_name = street_name[0]

                #append city name
                city_name = [obj['long_name'] for obj in address_component if obj['types'][0] == 'administrative_area_level_2']
                #city_name = [obj['long_name'] for obj in address_component if obj['types'][0] == 'locality']
                #field_list.append(city_name[0])
                city_name = city_name[0]

                #append country name
                country_name = [obj['long_name'] for obj in address_component if obj['types'][0] == 'country']
                #field_list.append(country_name[0])
                country_name = country_name[0]

            except IndexError as indErr:
                print('IndexError occurred in geocoding, list may be empty: ', indErr)
                #field_list = field_list[:3]
                #field_list.append("")       
                #field_list.append("")       
                #field_list.append("")  
                street_name = None
                city_name = None
                country_name = None

            except KeyError as keyErr:
                print('KeyError occurred in geocoding, object does not have the key: ', keyErr)
                #field_list = field_list[:3]
                #field_list.append("")       
                #field_list.append("")       
                #field_list.append("") 
                street_name = None
                city_name = None
                country_name = None

            try:
                self.cur.execute("insert into streets_reverse_geocoded \
                                  (id, asset_id, route, administrative_area_level_2, country) \
                                  values (%s, %s, %s, %s, %s)", (count, asset_id, street_name, city_name, country_name))
            except:
                print("I am unable to get data")

            self.conn.commit()        


    def run(self):
        """ 

        """
        self.connect_db()
        assets_list = self.getAssetsList()
        self.reverseGeocoding(assets_list)
        self.disconnect_db()

if __name__ == "__main__":

    #geocoding paid key
    key = 'AIzaSyAuXEEdlYPKW-KYrDupd04_stQQeDlIrIo'

    geocodingObj = GeoCoding(key)
    geocodingObj.run()


