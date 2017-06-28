import datetime
from sunrise import sun
import psycopg2
import csv
import sys
import json
import statistics
from collections import Counter

class DayburnerEnergyNominalWattage:

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
                self.cur.execute("select dimming_calendar_id \
                                  from components A, communications_nodes B \
                                  where A.id = B.id and asset_id = %s \
                                  and dimming_calendar_id is not null", (asset_id, ))
            except:
                print("I am unable to get data")

            calendar_id_row = self.cur.fetchone()
            # print(asset_id, calendar_id_row)
            calendar_id = int(calendar_id_row[0])
            if calendar_id != 1:
                # this asset doesn't follow the 100% calendar, ignore it in this analysis
                continue

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

            results.append((asset_id, component_id, latitude, longitude, installation_date, commissioning_date, street_name, cabinet_id, nominal_wattage))

        return results                     

    def detect_dayburners_with_nominal_wattage(self, asset_tuple, start_time, end_time):
        """ use the nominal wattage and suntime to compute the normal energy consumption for each day
            get the actual daily energy consumption from the energy_aggregation_daily table, which is measured_kwh + interpolated_kwh  
            report if the energy consumption differs by 0.1 kwh or 0.2 kwh between the normal and actual one

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

        try:
            self.cur.execute("select b.asset_id, a.interpolated_kwh, a.measured_kwh, a.aggregation_time \
                              from energy_metering_points b, energy_aggregation_daily a \
                              where b.asset_id = %s and b.id = a.metering_point_id \
                              and a.aggregation_time >= %s and a.aggregation_time < %s \
                              order by b.asset_id, a.aggregation_time", (asset_id, start_time, end_time))
        except:
            print("I am unable to get data")        

        rows = self.cur.fetchall()

        results = []

        for row in rows:
            # get the current date
            currentTime = row[3]
            currentDate = currentTime.date()
            # compute the actual energy consumption for this day
            interpolated_kwh = row[1]
            measured_kwh = row[2]
            dailyEnergyConsumption = interpolated_kwh + measured_kwh

            # compute the night time using sunset and sunrise time and normal energy consumption
            sunrise_time = self.sunriseTimeDict[currentDate]
            sunset_time = self.sunsetTimeDict[currentDate]
            daytime_in_min = (sunset_time - sunrise_time).total_seconds() / 60.0
            nighttime_in_min = 24 * 60 - daytime_in_min

            # compute the normal energy consumption in kwh using night time and nominal wattage
            normal_energy_consumption = (nominal_wattage * nighttime_in_min / 60.0) / 1000.0 

            if dailyEnergyConsumption - normal_energy_consumption > 0.1:
                # actual energy consumption is more than 0.2 kwh higher than normal_energy_consumption, report that
                results.append((asset_id, component_id, latitude, longitude, installation_date, commissioning_date, street_name, cabinet_id, nominal_wattage, currentDate, dailyEnergyConsumption, normal_energy_consumption))

        return results

    def computeResults(self, component_id_list):
        """ compute results

        """    
        count = 0
        start_time = datetime.datetime.combine(self.startDate, datetime.time())
        end_time = datetime.datetime.combine(self.endDate, datetime.time())
        with open(self.outputFilename, "w") as csvFile:
            csvWriter = csv.writer(csvFile, delimiter=',')  
            title_row = ('asset_id', 'component_id', 'latitude', 'longitude', 'installation_date', 'commissioning_date', 'street_name', 'cabinet_id', 'nominal_wattage', 'current_date', 'actual_energy_consumption', 'normal_energy_consumption')
            csvWriter.writerow(title_row)     
            for component_id_tuple in component_id_list:
                count += 1
                #only compute for first 500 components
                #if count > 500:
                #    break
                print(count)
                #print(component_id_tuple)
                
                results = self.detect_dayburners_with_nominal_wattage(component_id_tuple, start_time, end_time)              
                #print('len of results: ', len(results))
                #the results returned is a list of tuples
                #if len(results) > 0:
                for record in results:
                    csvWriter.writerow(record)     

    def run(self):
        """  call this method to run the program

        """
        # get components list
        component_id_list = self.getComponentsList()
        #component_id_list = [(2003, 3957, 41.3826797751266, 2.17683879808138, '2016-02-19', '2016-02-19', 'Placa de Sant Jaume', 3935, 72)]
        #call computeResults method
        self.computeResults(component_id_list)

if __name__ == "__main__":

    configJSONFilename = sys.argv[1]
    testObj = DayburnerEnergyNominalWattage(configJSONFilename)    
    testObj.run()            

    
