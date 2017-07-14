import psycopg2
import json
import sys
import datetime
import csv
from sunrise import sun
from xml_parser import XML_Parser


class SP_Calendar_Mismatch_Detector:
    """ this class is used to detect lighting not as programmed fault by utilizing switching point data and calendar data and suntime information

    """

    def __init__(self, configJSONFilename):
        """ initialize variables

        """
        #configuration file name
        self.configFilename = configJSONFilename
        #1 day delta
        self.oneDayDelta = datetime.timedelta(days=1)
        #1 hour delta
        self.oneHourDelta = datetime.timedelta(hours=1)
        #2 hours delta
        self.twoHoursDelta = datetime.timedelta(hours=2)        
        
        #sunrise dict
        self.sunriseTimeDict = {}
        #sunset dict
        self.sunsetTimeDict = {}

        #step 1:  read from json config file, get db connect parameter, time period to check, output file name
        self.getConfig(self.configFilename)
        #step 2:  connect to db
        self.connectDB()
        #step 3:  compute sunrise and sunset time 
        self.computeSunTime(self.suntime_latitude, self.suntime_longitude, self.startDate, self.endDate)
        
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
                    
            self.pg_dbname = config_data['pg_dbname']
            self.pg_username = config_data['pg_username']
            self.pg_password = config_data['pg_password']
            self.pg_host = config_data['pg_host']
            self.pg_port = config_data['pg_port']

            #output filename
            self.outputFilename = config_data['output_csvfile_name']
            #define energyThreshold: 2 watt
            self.energyThreshold = int(config_data['energy_threshold'])  
            #define onTime threshold for a day: in seconds
            self.onTimeThreshold = int(config_data['onTime_threshold'])  
            #period start date
            self.startDate = datetime.datetime.strptime(config_data['period_start_date'], '%m/%d/%Y').date()
            #period end date
            self.endDate = datetime.datetime.strptime(config_data['period_end_date'], '%m/%d/%Y').date()
            #suntime location latitude
            self.suntime_latitude = float(config_data['suntime_location_latitude'])
            #suntime location longitude
            self.suntime_longitude = float(config_data['suntime_location_longitude'])

            # sunrise time offset
            sunrise_time_delta_hours = int(config_data['sunrise_time_delta_hours'])
            sunrise_time_delta_minutes = int(config_data['sunrise_time_delta_minutes'])
            sunrise_time_delta_seconds = int(config_data['sunrise_time_delta_seconds'])
            # sunrise adjusted time delta - sunrise time should add this delta
            self.sunriseTimeDelta = datetime.timedelta(hours=sunrise_time_delta_hours, minutes=sunrise_time_delta_minutes, seconds=sunrise_time_delta_seconds)
            # sunset time offset
            sunset_time_delta_hours = int(config_data['sunset_time_delta_hours'])
            sunset_time_delta_minutes = int(config_data['sunset_time_delta_minutes'])
            sunset_time_delta_seconds = int(config_data['sunset_time_delta_seconds'])
            # sunset adjusted time delta - sunset time should minus this delta
            self.sunsetTimeDelta = datetime.timedelta(hours=sunset_time_delta_hours, minutes=sunset_time_delta_minutes, seconds=sunset_time_delta_seconds)

            # hours from UTC time for the local time
            hours_from_utc = int(config_data['hours_from_utc'])
            self.local_time_hours_from_utc = datetime.timedelta(hours=hours_from_utc)

    def getComponentsList(self):
        """ get assets list from assets table, which are not deleted and installation_date and commissioning_date are not null

        """                
        try:
            self.cur.execute("select id, latitude, longitude, installation_date, commissioning_date \
                              from assets \
                              where is_deleted = 'f' \
                              and installation_date is not null and commissioning_date is not null")
        except:
            print("I am unable to get data")

        rows = self.cur.fetchall()   
        results = []
        for row in rows:
            asset_id = row[0]
            latitude = row[1]
            longitude = row[2]
            installation_date = row[3]
            commissioning_date = row[4]

            try:
                self.cur.execute("select id \
                                  from components \
                                  where asset_id = %s \
                                  and is_deleted = 'f' and component_kind = 0", (asset_id, ))
            except:
                print("I am unable to get data")

            component_id_data = self.cur.fetchall() 
            component_id = component_id_data[0][0] 

            try:
                self.cur.execute("select s.name as street_name \
                                  from assets as a, streets as s \
                                  where a.id = %s \
                                  and a.street_id = s.id", (asset_id, ))
            except:
                print("I am unable to get data")

            street_name_data = self.cur.fetchall() 
            street_name = street_name_data[0][0]

            try:
                self.cur.execute("select parent_id \
                                  from components A,communications_nodes B \
                                  where A.id=B.id and asset_id = %s", (asset_id, ))
            except:
                print("I am unable to get data")

            cabinet_id_data = self.cur.fetchall()     
            cabinet_id = cabinet_id_data[0][0]

            try:
                self.cur.execute("select c.asset_id, lt.type_designation, lt.actual_wattage \
                                  from luminaire_types lt, luminaires l, components c \
                                  where c.asset_id = %s and c.id = l.id and l.luminaire_type_id = lt.id", (asset_id, ))
            except:
                print("I am unable to get data")

            nominal_wattage_data = self.cur.fetchall()     
            nominal_wattage = nominal_wattage_data[0][2]

            results.append((asset_id, component_id, latitude, longitude, installation_date, commissioning_date, street_name, cabinet_id, nominal_wattage))

        return results  

    def get_component_info_for_one_asset(self, asset_id):
        """ get the information related to one asset

        """
        try:
            self.cur.execute("select id, latitude, longitude, installation_date, commissioning_date \
                              from assets \
                              where id = %s", (asset_id, ))
        except:
            print("I am unable to get data")

        row = self.cur.fetchone() 
        asset_id = row[0]
        latitude = row[1]
        longitude = row[2]
        installation_date = row[3]
        commissioning_date = row[4]

        try:
            self.cur.execute("select id \
                              from components \
                              where asset_id = %s \
                              and is_deleted = 'f' and component_kind = 0", (asset_id, ))
        except:
            print("I am unable to get data")

        component_id_row = self.cur.fetchone() 
        component_id = component_id_row[0] 

        try:
            self.cur.execute("select s.name as street_name \
                              from assets as a, streets as s \
                              where a.id = %s \
                              and a.street_id = s.id", (asset_id, ))
        except:
            print("I am unable to get data")

        street_name_row = self.cur.fetchone() 
        street_name = street_name_row[0]

        try:
            self.cur.execute("select parent_id \
                              from components A,communications_nodes B \
                              where A.id=B.id and asset_id = %s", (asset_id, ))
        except:
            print("I am unable to get data")

        cabinet_id_row = self.cur.fetchone()     
        cabinet_id = cabinet_id_row[0]

        try:
            self.cur.execute("select c.asset_id, lt.type_designation, lt.actual_wattage \
                              from luminaire_types lt, luminaires l, components c \
                              where c.asset_id = %s and c.id = l.id and l.luminaire_type_id = lt.id", (asset_id, ))
        except:
            print("I am unable to get data")

        nominal_wattage_row = self.cur.fetchone()     
        nominal_wattage = nominal_wattage_row[2]

        results = []
        results.append((asset_id, component_id, latitude, longitude, installation_date, commissioning_date, street_name, cabinet_id, nominal_wattage))

        return results

    def computeDaytimeStartEnd(self, date):
        """

        return a tuple (daytimeStart, daytimeEnd)
        both of the two elements are datetime objects
        the sun time is in UTC

        """
        dayStartTime = datetime.datetime.combine(date.date(), datetime.time())
        #compute sunrise time for that date
        (h, m, s) = self.sun.sunrise(when=date)
        time_delta = datetime.timedelta(hours=h, minutes=m, seconds=s)
        sunrise_datetime = dayStartTime + time_delta
        #print(sunrise_datetime)       
        #compute sunset time for that date 
        (h, m, s) = self.sun.sunset(when=date)
        time_delta = datetime.timedelta(hours=h, minutes=m, seconds=s)
        sunset_datetime = dayStartTime + time_delta
        
        return (sunrise_datetime, sunset_datetime)

    def computeSunTime(self, latitude, longitude, startDate, endDate):
        """ this method will compute the sunrise and sunset time for each date between [startDate, endDate]
            based on the given latitude and longitude, and create a dictionary
            fact: the suntime for each light position in Jakarta will not differ by more than 1.5 minutes

            Args:
                latitude:  double variable, the latitude of a given light location
                longitude: double variable, the longitude of a given light location
                startDate: datetime.date() variable, the start of a time period
                endDate:   datetime.date() variable, the end of a time period 
        """    
        self.sun = sun(lat=latitude, long=longitude)
        dateTime = datetime.datetime.combine(startDate, datetime.time(hour=8))
        while dateTime.date() <= endDate:        
            daytimeStart, daytimeEnd = self.computeDaytimeStartEnd(dateTime)
            self.sunriseTimeDict[dateTime.date()] = daytimeStart
            self.sunsetTimeDict[dateTime.date()] = daytimeEnd
            dateTime += self.oneDayDelta  

    def get_xml_with_asset_id(self, asset_id):
        """ get the last calendar revision XML for the input asset id

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
        # the calendars is a list of dict, which contains shapes for 7 days in a week starting from Sunday
        # the useful fields in calendars include 'shape_sunrise_offset', 'shape_sunset_offset', 'item_minutes', 'item_percent'
        # the time in 'item_minutes' is local time
        calendars = xml_parser.parse_from_string(calendar_xml_str) 
        return calendars
        
    def check_weekday_of_date(self, date):
        """ check which weekday the input date belongs to, the input is a date object
            Sunday is 0, Monday is 1, etc
            the returned value is an integer from 0 to 6 corresponding to Sunday to Saturday

        """
        return date.isoweekday() % 7

    def get_calendar_item(self, calendar_item_list, minutes_from_calendar_date_start):
        """ get the calendar item that the input time belongs

            input args:
              calendar_item_list: a list of shape items, each of which is a dictionary
              minutes_from_calendar_date_start: the input time in the format of minutes from the start of the calendar date 
                                                (for example: 720 represents 12pm noon time )

        """
        last_calendar_item = None 
        for item in calendar_item_list:
            if last_calendar_item is None:
                last_calendar_item = item
            else:
                current_calendar_item = item
                last_time_minutes = int(last_calendar_item['item_minutes'])
                current_time_minutes = int(current_calendar_item['item_minutes'])
                if minutes_from_calendar_date_start >= last_time_minutes and minutes_from_calendar_date_start < current_time_minutes:
                    return last_calendar_item
                last_calendar_item = current_calendar_item

        return last_calendar_item                        

    def find_SP_calendar_mismatch(self, asset_tuple, start_time, end_time):
        """ get the switching data for the input asset and check the mismatch compared with calendar and suntime
            need to consider:
            1) calendars for the 7 days
            2) sunrise and sunset time 
            3) sunrise and sunset offset in the shapes

        """
        asset_id = asset_tuple[0]
        component_id = asset_tuple[1]
        latitude = asset_tuple[2]
        longitude = asset_tuple[3]
        installation_date = asset_tuple[4]
        commissioning_date = asset_tuple[5]
        street_name = asset_tuple[6]
        cabinet_id = asset_tuple[7]
        nominal_wattage = asset_tuple[8]

        # get calendars for this asset from XML
        calendar_xml_str = self.get_xml_with_asset_id(asset_id)
        # calendars is a list of 7 dictionaries (shapes), each of which correponds to one day in a week, starting from Sunday
        calendars = self.parse_calendar_xml(calendar_xml_str)

        try:
            self.cur.execute("select timestamp_utc, log_value, is_log_value_off \
                              from switching_points \
                              where component_id = %s \
                              and timestamp_utc >= %s and timestamp_utc < %s \
                              order by timestamp_utc", (component_id, start_time, end_time))
        except:
            print("I am unable to get data")        

        rows = self.cur.fetchall()

        results = []
        for row in rows:
            # convert the time from UTC to local time
            currentTime = row[0] + self.local_time_hours_from_utc
            # log value may be -2, 0, or positive values (positive values represent dimming percent of the light)
            currentLogValue = int(row[1])
            # it is true or false
            currentIsLogValueOff = row[2]

            currentDate = currentTime.date()
            timePart = currentTime.time()

            noontime = datetime.time(12, 0, 0)
            if timePart < noontime:
                calendar_date = currentDate - self.oneDayDelta
            else: 
                calendar_date = currentDate 

            # get this day's sunrise and sunset time in local time
            sunrise_time = self.sunriseTimeDict[currentDate] + self.local_time_hours_from_utc
            sunset_time = self.sunsetTimeDict[currentDate] + self.local_time_hours_from_utc  

            calendar_index = self.check_weekday_of_date(calendar_date)
            calendar_shape_dict = calendars[calendar_index] 

            shape_sunrise_offset = int(calendar_shape_dict['shape_sunrise_offset'])
            shape_sunrise_offset_time = datetime.timedelta(minutes=shape_sunrise_offset)
            shape_sunset_offset = int(calendar_shape_dict['shape_sunset_offset'])
            shape_sunset_offset_time = datetime.timedelta(minutes=shape_sunset_offset)
            # shape_items is a list of dictionary (each dict is an item ordered by time)
            shape_items = calendar_shape_dict['items']

            calendar_date_start_time = datetime.datetime.combine(calendar_date, datetime.time())
            minutes_from_calendar_date_start = (currentTime - calendar_date_start_time).total_seconds() / 60

            calendar_item = self.get_calendar_item(shape_items, minutes_from_calendar_date_start)

            # check if the log value is valid, according to calendars and sun time
            recordInvalid = False
            sunrise_time_with_buffer = sunrise_time + shape_sunrise_offset_time + self.sunriseTimeDelta
            sunrise_time_lower_boundary = sunrise_time + shape_sunrise_offset_time - self.sunriseTimeDelta
            # always add the offset, for Jakarta_utara, the sunset_offset for calendar 2 is -6, so we add this value to sunset time
            sunset_time_with_buffer = sunset_time + shape_sunset_offset_time - self.sunsetTimeDelta
            calendar_percentage = int(calendar_item['item_percent'])
            if currentLogValue > 0:
                # it is light on record
                # need to match both sun time and calendar percent                 
                if currentTime >= sunrise_time_with_buffer and currentTime <= sunset_time_with_buffer:
                    recordInvalid = True
                elif currentLogValue != calendar_percentage:
                    #print(currentLogValue, calendar_percentage)
                    recordInvalid = True
                else:
                    recordInvalid = False    
            elif currentLogValue == 0:
                # it is light off record   
                # need to match either within sunrise buffer, or calendar_percentage value                
                if currentTime >= sunrise_time_lower_boundary and currentTime <= sunrise_time_with_buffer:
                    recordInvalid = False
                elif currentLogValue == calendar_percentage:
                    recordInvalid = False           
                else:
                    recordInvalid = True
            else:
                # the currentLogValue is -2
                # need to be within sunrise buffer
                if currentTime >= sunrise_time_lower_boundary and currentTime <= sunrise_time_with_buffer:
                    recordInvalid = False
                else:
                    recordInvalid = True
                    #print("-2 cases")

            if recordInvalid:
                # it is invalid record
                #print(asset_id, component_id, latitude, longitude, currentTime, currentLogValue, currentIsLogValueOff)  
                results.append((asset_id, component_id, latitude, longitude, installation_date, commissioning_date, street_name, cabinet_id, nominal_wattage, currentTime, currentLogValue, currentIsLogValueOff)) 

        return results        

    def compute_results(self, component_id_list):
        """ compute results

        """
        count = 0
        start_time = datetime.datetime.combine(self.startDate, datetime.time())
        end_time = datetime.datetime.combine(self.endDate, datetime.time())
        results = []
        for component_id_tuple in component_id_list:
            count += 1
            #only compute for first 500 components
            #if count > 500:
            #    break
            print(count)  
            results += self.find_SP_calendar_mismatch(component_id_tuple, start_time, end_time)   

        return results    

    def write_to_file(self, results):
        """ write results to output file

        """
        with open(self.outputFilename, "w") as csvFile:
            csvWriter = csv.writer(csvFile, delimiter=',')   
            title_row = ('asset_id', 'component_id', 'latitude', 'longitude', 'installation_date', 'commissioning_date', 'street_name', 'cabinet_id', 'nominal_wattage', 'current_time', 'current_LogValue', 'current_IsLogValueOff')         
            csvWriter.writerow(title_row)
            for record in results:
                csvWriter.writerow(record)             

    def run(self):
        """ call this method to run the program

        """
        # get components list
        #component_id_list = self.getComponentsList()
        asset_id = 13532
        component_id_list = self.get_component_info_for_one_asset(asset_id)
        # call computeResults method
        results = self.compute_results(component_id_list)
        # write to the output file
        self.write_to_file(results)


if __name__ == "__main__":

    configJSONFilename = sys.argv[1]
    detector = SP_Calendar_Mismatch_Detector(configJSONFilename)    
    detector.run()            
