import psycopg2
import json
import sys
from xml_parser import XML_Parser

class Calendar_XML_Analyzer:

    def __init__(self, configJSONFilename):
        """ initialize variables

        """
        #configuration file name
        self.configFilename = configJSONFilename

        #step 1:  read from json config file, get db connect parameter, time period to check, output file name
        self.getConfig(self.configFilename)
        #step 2:  connect to db
        self.connectDB()
        
    def connectDB(self):
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
        
    def getConfig(self, configFilename):
        """ get configuration parameters

        """    
        with open(configFilename) as config_file:    
            config_data = json.load(config_file)
            #db connection parameters        
            self.pg_dbname = config_data['pg_dbname']
            self.pg_username = config_data['pg_username']
            self.pg_password = config_data['pg_password']
            self.pg_host = config_data['pg_host']
            self.pg_port = config_data['pg_port']

    def get_xml_with_asset_id(self, asset_id):
        """ get the last calendar revision XML for the input asset

        """
        
        try:
                self.cur.execute("select id \
                                  from components \
                                  where asset_id = %s and is_deleted = 'f' and component_kind = 100", (asset_id,))
        except:
                print("I am unable to get data") 

        rows = self.cur.fetchall()
        component_id = rows[0][0]
        print(component_id)

        try:
                self.cur.execute("select dimming_calendar_id \
                                  from communications_nodes \
                                  where id = %s", (component_id,))
        except:
                print("I am unable to get data") 

        rows = self.cur.fetchall()
        dimming_calendar_id = rows[0][0]        
        print(dimming_calendar_id)

        return self.get_xml_with_calendar_id(dimming_calendar_id)            

    def get_xml_with_calendar_id(self, calendar_id):
        """ get the xml of a calendar according to the calendar id

        """
        try:
                self.cur.execute("select serialized_schedules \
                                  from driver_calendar_revisions \
                                  where core_calendar_id = %s \
                                  order by created_on_utc desc", (calendar_id,))
        except:
                print("I am unable to get data")

        rows = self.cur.fetchall()
        calendar_xml = rows[0][0] 
        #calendar_xml is of type str
        #print(type(calendar_xml))
        #print(calendar_xml) 
        return calendar_xml   

    def parse_calendar_xml(self, calendar_xml_str):
        """ use the imported parser to parse the calendar XML

        """
        #step 4:  parse the calendar xml 
        xml_parser = XML_Parser()
        xml_parser.parse_from_string(calendar_xml_str)        


if __name__ == "__main__":

    configJSONFilename = sys.argv[1]
    analyzer = Calendar_XML_Analyzer(configJSONFilename)

    # get the calendar for the input asset id
    #asset_id = sys.argv[2]        
    #calendar_xml_str = analyzer.get_xml_with_asset_id(asset_id)    
    
    # get the calendar for the input calendar id
    calendar_id = sys.argv[2]
    calendar_xml_str = analyzer.get_xml_with_calendar_id(calendar_id)

    analyzer.parse_calendar_xml(calendar_xml_str)  

