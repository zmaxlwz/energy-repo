import psycopg2
import json
import sys
import datetime
import csv
from sunrise import sun
from xml_parser import XML_Parser


class LightsNotAsProgrammed:
    """ this class is used to detect both dayburners and night time outage
        at daytime, if the actual wattage is [30%, 500%] * nominal wattage, then it is a dayburning interval
        at nighttime, if the actual wattage is not within [calendar_percentage - 5%,  calendar_percentage + 5%] * nominal wattage, 
           then it is night time outage

    """

    def __init__(self, configJSONFilename):
        """ initialize variables

        """
        #configuration file name
        self.configFilename = configJSONFilename
        # 1 day delta
        self.oneDayDelta = datetime.timedelta(days=1)
        #1 hour delta
        self.oneHourDelta = datetime.timedelta(hours=1)
        #2 hours delta
        self.twoHoursDelta = datetime.timedelta(hours=2)  
        # 7 hours delta, LA in July is -7 from UTC
        self.sevenHoursDelta = datetime.timedelta(hours=7)
        # 8 hours delta, LA in Jan is -8 from UTC
        self.eightHoursDelta = datetime.timedelta(hours=8)
           
        #sunrise dict
        self.sunriseTimeDict = {}
        #sunset dict
        self.sunsetTimeDict = {}
        
        #assets latitude dict
        self.assets_latitude_dict = {}
        #assets longitude dict
        self.assets_longitude_dict = {}
        #assets installation date dict
        self.assets_installation_date_dict = {}
        #assets commissioning date dict
        self.assets_commissioning_date_dict = {}
        #assets nominal wattage dict
        self.assets_nominal_wattage_dict = {}
        #assets luminaire type
        self.assets_luminaire_type_dict = {}
        #assets street name dict
        self.assets_street_name_dict = {}
        #assets component id dict
        self.assets_component_id_dict = {}

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

    def get_config(self, configFilename):
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
            #define onTime threshold for a day: in minutes
            self.onTimeThreshold = int(config_data['onTime_threshold'])  
            #define nominal wattage ratio
            self.nominal_wattage_ratio = float(config_data['nominal_wattage_ratio'])
            #period start date
            self.startDate = datetime.datetime.strptime(config_data['period_start_date'], '%m/%d/%Y').date()
            #period end date
            self.endDate = datetime.datetime.strptime(config_data['period_end_date'], '%m/%d/%Y').date()
            #daytime start time
            self.daytime_start_time_local = datetime.datetime.strptime(config_data['daytime_start_time_local'], '%H:%M:%S').time()
            #daytime end time
            self.daytime_end_time_local = datetime.datetime.strptime(config_data['daytime_end_time_local'], '%H:%M:%S').time()    
            #sunrise avg local time
            self.sunrise_time_avg_local = datetime.datetime.strptime(config_data['sunrise_time_avg_local'], '%H:%M:%S').time()   
            #sunset avg local time
            self.sunset_time_avg_local = datetime.datetime.strptime(config_data['sunset_time_avg_local'], '%H:%M:%S').time()       
            #commissioning date plus the number of days
            self.commissioningDatePlusDays = int(config_data['commissioning_date_plus_days'])
            #sunrise adjusted time delta - sunrise time should add this delta
            self.sunriseTimeDelta = datetime.timedelta(hours=int(config_data['sunrise_time_delta_hours']), minutes=int(config_data['sunrise_time_delta_minutes']), seconds=int(config_data['sunrise_time_delta_seconds']))
            #sunset adjusted time delta - sunset time should minus this delta
            self.sunsetTimeDelta = datetime.timedelta(hours=int(config_data['sunset_time_delta_hours']), minutes=int(config_data['sunset_time_delta_minutes']), seconds=int(config_data['sunset_time_delta_seconds']))
            #suntime location latitude
            self.suntime_latitude = float(config_data['suntime_location_latitude'])
            #suntime location longitude
            self.suntime_longitude = float(config_data['suntime_location_longitude'])

            # hours from UTC time for the local time
            hours_from_utc = int(config_data['hours_from_utc'])
            # UTC time plus this time to get local time
            self.local_time_hours_from_utc = datetime.timedelta(hours=hours_from_utc)

    def getAssetsList(self):
        """ get assets list from assets table, which are not deleted and installation_date and commissioning_date are not null

        """                
        try:
            
            self.cur.execute("select id \
                              from assets \
                              where is_deleted = 'f' \
                              and installation_date is not null and commissioning_date is not null")
            '''
            #for LA
            self.cur.execute("select id \
                              from assets \
                              where is_deleted = 'f' \
                              and commissioning_date is not null")
            '''         
        except:
            print("I am unable to get data")

        rows = self.cur.fetchall()  

        assets_id_list = []
        for row in rows:
            asset_id = row[0]
            assets_id_list.append(asset_id)

        return assets_id_list   
        
    def get_assets_info(self):
        """ get assets information from database, like latitude, longitude, installation_date, commissioning_date 

        """
        try:
            
            self.cur.execute("select a.id, a.latitude, a.longitude, a.installation_date, a.commissioning_date, s.name as street_name \
                              from assets as a, streets as s \
                              where a.is_deleted = 'f' \
                              and a.installation_date is not null and a.commissioning_date is not null \
                              and a.street_id = s.id")
            '''
            #for LA
            self.cur.execute("select a.id, a.latitude, a.longitude, a.installation_date, a.commissioning_date, s.route as street_name \
                              from assets as a, streets_reverse_geocoded as s \
                              where a.is_deleted = 'f' \
                              and a.commissioning_date is not null \
                              and a.id = s.asset_id")   
            '''                                 
        except:
            print("I am unable to get data")

        rows = self.cur.fetchall() 

        for row in rows:
            asset_id = row[0]
            latitude = row[1]
            longitude = row[2]
            installation_date = row[3]
            commissioning_date = row[4]
            street_name = row[5]
            self.assets_latitude_dict[asset_id] = latitude
            self.assets_longitude_dict[asset_id] = longitude
            self.assets_installation_date_dict[asset_id] = installation_date
            self.assets_commissioning_date_dict[asset_id] = commissioning_date
            self.assets_street_name_dict[asset_id] = street_name

    def get_nominal_wattage(self):
        """ get assets nominal wattage from database

        """        
        try:
            
            self.cur.execute("select a.id, lt.type_designation, lt.nominal_wattage, lt.actual_wattage \
                              from luminaire_types lt, luminaires l, components c, assets a \
                              where a.is_deleted = 'f' and a.installation_date is not null and a.commissioning_date is not null \
                              and a.id = c.asset_id and c.id = l.id and l.luminaire_type_id = lt.id \
                              order by a.id")
            '''
            #for LA, nominal_wattage is null, use actual wattage, 
            self.cur.execute("select a.id, lt.type_designation, lt.actual_wattage \
                              from luminaire_types lt, luminaires l, components c, assets a \
                              where a.is_deleted = 'f' and a.commissioning_date is not null \
                              and a.id = c.asset_id and c.id = l.id and l.luminaire_type_id = lt.id \
                              order by a.id")                  
            '''
        except:
            print("I am unable to get data")

        rows = self.cur.fetchall() 

        for row in rows:
            asset_id = row[0]
            luminaire_type = row[1]
            nominal_wattage = row[2]            
            self.assets_luminaire_type_dict[asset_id] = luminaire_type
            self.assets_nominal_wattage_dict[asset_id] = nominal_wattage
            
    def get_components_id(self):
        """ get the component id for assets

        """        
        try:
            self.cur.execute("select a.id, c.id \
                              from assets as a, components as c \
                              where a.is_deleted = 'f' \
                              and a.installation_date is not null and a.commissioning_date is not null \
                              and a.id = c.asset_id \
                              and c.is_deleted = 'f' and c.component_kind = 0 and c.is_registered = 't'")
        except:
            print("I am unable to get data")

        rows = self.cur.fetchall() 

        for row in rows:
            asset_id = row[0]
            component_id = row[1]
            self.assets_component_id_dict[asset_id] = component_id

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
        component_id = self.assets_component_id_dict[asset_id]
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

    def find_energy_consumption_not_as_programmed(self, asset_id, start_time, end_time):
        """

        """
        component_id = self.assets_component_id_dict[asset_id]
        latitude = self.assets_latitude_dict[asset_id]
        longitude = self.assets_longitude_dict[asset_id]
        installation_date = self.assets_installation_date_dict[asset_id]
        commissioning_date = self.assets_commissioning_date_dict[asset_id]
        valid_start_date = commissioning_date + datetime.timedelta(days=self.commissioningDatePlusDays) 
        street_name = self.assets_street_name_dict[asset_id]
        nominal_wattage = self.assets_nominal_wattage_dict[asset_id]

        print("asset id: ", asset_id)
        # get calendars for this asset from XML
        calendar_xml_str = self.get_xml_with_asset_id(asset_id)
        # calendars is a list of 7 dictionaries (shapes), each of which correponds to one day in a week, starting from Sunday
        calendars = self.parse_calendar_xml(calendar_xml_str)

        try:
            #there might be multiple lights in an assets, so need to order by asset_id and component_id
            self.cur.execute("select b.asset_id , b.meter_component_id, a.kwh, a.timestamp_utc \
                              from energy_metering_points b, energy_meter_readings a \
                              where b.asset_id = %s and b.id = a.metering_point_id \
                              and a.timestamp_utc >= %s and a.timestamp_utc < %s \
                              order by b.asset_id, b.meter_component_id, a.timestamp_utc", (asset_id, start_time, end_time))
        except:
            print("I am unable to get data")

        rows = self.cur.fetchall()

        results = []
        
        lastTime = None
        lastEnergy = None
        for row in rows:
            energy_reading = row[2]
            localtime = row[3] + self.local_time_hours_from_utc
            if localtime.date() < valid_start_date:
                continue
            if lastTime is None:
                lastTime = localtime
                lastEnergy = energy_reading
            else:
                currentTime = localtime
                currentEnergy = energy_reading
                
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
                
                sunrise_time_with_buffer = sunrise_time + shape_sunrise_offset_time + self.sunriseTimeDelta
                sunrise_time_lower_boundary = sunrise_time + shape_sunrise_offset_time - self.sunriseTimeDelta
                # always add the offset, for Jakarta_utara, the sunset_offset for calendar 2 is -6, so we add this value to sunset time
                sunset_time_with_buffer = sunset_time + shape_sunset_offset_time - self.sunsetTimeDelta
                sunset_time_higher_boundary = sunset_time + shape_sunset_offset_time + self.sunsetTimeDelta
                calendar_percentage = int(calendar_item['item_percent']) 
                
                # the status is an indicator ('normal', 'dayburner', 'night_outage', 'night_not_dimming')
                status = 'normal'
                secondsInterval = (currentTime - lastTime).total_seconds()
                energyConsumed = currentEnergy - lastEnergy
                
                if secondsInterval == 0:
                    lastTime = currentTime
                    lastEnergy = currentEnergy
                    continue
                # compute the wattage within the interval    
                consumptionRate = (energyConsumed * 1000) / (secondsInterval / 3600.0)
                    
                if currentTime >= sunrise_time_with_buffer and currentTime <= sunset_time_with_buffer:
                    # this is the day time, check if it is a dayburning interval
                    if consumptionRate > nominal_wattage * self.nominal_wattage_ratio + self.energyThreshold and consumptionRate < 5 * nominal_wattage:
                        # this is a dayburning interval
                        status = 'dayburner'
                        results.append((self.pg_dbname, asset_id, latitude, longitude, installation_date, commissioning_date, street_name, nominal_wattage, lastTime, currentTime, 0, consumptionRate, status))

                elif currentTime < sunrise_time_lower_boundary or currentTime > sunset_time_higher_boundary:
                    # this is the night time, check if the energy consumption following calendars
                    if consumptionRate < nominal_wattage * (calendar_percentage * 0.01 - 0.1):
                        # the actal wattage is below the (calendar percentage - 10%) * nominal_wattage, it is night time outage 
                        status = 'night_outage'
                        results.append((self.pg_dbname, asset_id, latitude, longitude, installation_date, commissioning_date, street_name, nominal_wattage, lastTime, currentTime, calendar_percentage, consumptionRate, status))

                    elif consumptionRate > nominal_wattage * (calendar_percentage * 0.01 + 0.1) and consumptionRate <= nominal_wattage * (1 + 0.1):
                        # the actual wattage is above the (calendar percentage + 10%) * nominal_wattage and below 110% * nominal_wattage, it is night not dimming
                        status = 'night_not_dimming'
                        results.append((self.pg_dbname, asset_id, latitude, longitude, installation_date, commissioning_date, street_name, nominal_wattage, lastTime, currentTime, calendar_percentage, consumptionRate, status))

                    elif consumptionRate > nominal_wattage * (1 + 0.1):
                        # the actual wattage is above 110% * nominal_wattage, it is actual wattage above nominal wattage
                        status = 'actual_above_nominal_wattage'   
                        results.append((self.pg_dbname, asset_id, latitude, longitude, installation_date, commissioning_date, street_name, nominal_wattage, lastTime, currentTime, calendar_percentage, consumptionRate, status)) 

                lastTime = currentTime
                lastEnergy = currentEnergy         

        return results                
        
    def compute_results(self, assets_id_list):
        """ do the main computing and detect lights energy consumption not as programmed

        """    

        '''
        # process 1000 assets as a batch, 1000 is a configurable parameter
        start_index = 0
        step = 1000
        end_index = start_index + step
        assets_id_sublist = assets_id_list[start_index:end_index]
        results = []
        count = 0
        while assets_id_sublist:
            count += 1
            print(count)
            results += self.compute_energy_consumption(assets_id_sublist)
            start_index += step
            end_index += step
            assets_id_sublist = assets_id_list[start_index:end_index]
        '''

        start_time = datetime.datetime.combine(self.startDate, datetime.time())
        end_time = datetime.datetime.combine(self.endDate, datetime.time())
 
        # process assets one at a time
        results = []
        count = 0
        for asset_id in assets_id_list:
            count += 1
            print(count)
            results += self.find_energy_consumption_not_as_programmed(asset_id, start_time, end_time)

        return results                              

    def write_to_file(self, results):
        """ write results to output file

        """
        with open(self.outputFilename, "w") as csvFile:
            csvWriter = csv.writer(csvFile, delimiter=',')   
            title_row = ('region', 'asset_id', 'latitude', 'longitude', 'installation_date', 'commissioning_date', 'street_name', 'nominal_wattage', 'timestamp_start', 'timestamp_end', 'calendar_percentage', 'actual_wattage', 'error_type')         
            csvWriter.writerow(title_row)
            for record in results:
                csvWriter.writerow(record)        

    def run(self):
        """  call this method to run the program

        """
        #step 1:  read from json config file, get db connect parameter, time period to check, output file name
        self.get_config(self.configFilename)
        #step 2:  connect to db
        self.connect_db()
        #step 3:  compute sunrise and sunset time 
        self.computeSunTime(self.suntime_latitude, self.suntime_longitude, self.startDate, self.endDate)
        #step 4:  get assets list
        #assets = self.getAssetsList()
        #assets = [(3776, -6.118187, 106.894265, datetime.date(2016, 5, 27), datetime.date(2016, 6, 1))]
        #assets = [(3776, -6.118187, 106.894265), (13532, -6.102635, 106.932242)]
        assets_id_list = self.getAssetsList()
        #step 5:  call computeResults method
        #self.computeResults(assets)
        self.get_assets_info()
        print("got assets info")
        self.get_nominal_wattage()
        print("got nominal wattage info")
        self.get_components_id()
        print("got components id info")

        print("start computing")
        results = self.compute_results(assets_id_list)
        print("finished computing") 

        self.write_to_file(results)
        print("finished output to files")        


if __name__ == "__main__":

    configJSONFilename = sys.argv[1]
    lightsObj = LightsNotAsProgrammed(configJSONFilename)    
    lightsObj.run()
