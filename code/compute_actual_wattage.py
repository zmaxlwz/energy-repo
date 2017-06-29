import datetime
from sunrise import sun
import psycopg2
import csv
import sys
import json
import statistics
from collections import Counter

class ActualWattage:

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
        #sunrise adjusted time delta - sunrise time should add this delta
        self.sunriseTimeDelta = datetime.timedelta(hours=0, minutes=30, seconds=0)
        #sunset adjusted time delta - sunset time should minus this delta
        self.sunsetTimeDelta = datetime.timedelta(hours=0, minutes=30, seconds=0)
        #sunrise dict
        self.sunriseTimeDict = {}
        #sunset dict
        self.sunsetTimeDict = {}

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

    def computeDaytimeStartEnd(self, date):
        """

        return a tuple (daytimeStart, daytimeEnd)
        both of the two elements are datetime objects

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

    def compute_actual_wattage_from_aggregation_energy(self, asset_tuple, start_time, end_time):
        """ use aggregation energy table 
            For Jakarta, CABA, daily energy and night time to compute wattage in each day 
            and then compute the average as the actual wattage

        """
        asset_id = asset_tuple[0]

        try:
            self.cur.execute("select b.asset_id, a.interpolated_kwh, a.measured_kwh, a.aggregation_time \
                              from energy_metering_points b, energy_aggregation_daily a \
                              where b.asset_id = %s and b.id = a.metering_point_id \
                              and a.aggregation_time >= %s and a.aggregation_time < %s \
                              order by b.asset_id, a.aggregation_time", (asset_id, start_time, end_time))
        except:
            print("I am unable to get data")        

        rows = self.cur.fetchall()

        wattage_list = []
        for row in rows:
            currentTime = row[3]
            currentDate = currentTime.date()
            interpolated_kwh = row[1]
            measured_kwh = row[2]
            dailyEnergyConsumption = interpolated_kwh + measured_kwh

            #compute night time
            sunrise_time = self.sunriseTimeDict[currentDate]
	        sunset_time = self.sunsetTimeDict[currentDate]
	        daytime_in_hours = (sunset_time - sunrise_time).total_seconds() / 60.0 / 60.0
	        nighttime_in_hours = 24 - daytime_in_hours
	        
	        # compute the actual wattage for the asset in watts
	        actual_wattage = (dailyEnergyConsumption / nighttime_in_hours) * 1000
            wattage_list.append(actual_wattage)

        wattage_list = [x for x in wattage_list if x != 0]
        if len(energy_rolling_window) == 0:
            return None      

        actual_wattage = statistics.mean(wattage_list)
        
        return actual_wattage  

    def compute_results(self, component_id_list):
        """ report the assets with actual wattage and nominal wattage

        """
        count = 0
        start_time = datetime.datetime.combine(self.startDate, datetime.time())
        end_time = datetime.datetime.combine(self.endDate, datetime.time())
        with open(self.outputFilename, "w") as csvFile:
            csvWriter = csv.writer(csvFile, delimiter=',')  
            title_row = ('asset_id', 'component_id', 'latitude', 'longitude', 'installation_date', 'commissioning_date', 'street_name', 'cabinet_id', 'nominal_wattage', 'actual_wattage')
            csvWriter.writerow(title_row)     
            for component_id_tuple in component_id_list:
                count += 1
                #only compute for first 500 components
                #if count > 500:
                #    break
                print(count)
                #print(component_id_tuple)
                actual_wattage = self.compute_actual_wattage_from_aggregation_energy(component_id_tuple, start_time, end_time)
                if actual_wattage is None:
                    continue

                asset_id = component_id_tuple[0]
                component_id = component_id_tuple[1]
                latitude = component_id_tuple[2]
                longitude = component_id_tuple[3]
                installation_date = component_id_tuple[4]
                commissioning_date = component_id_tuple[5]
                street_name = component_id_tuple[6]
                cabinet_id = component_id_tuple[7]
                nominal_wattage = component_id_tuple[8]         
                
                #if actual_wattage > nominal_wattage:
                #    csvWriter.writerow((asset_id, component_id, latitude, longitude, installation_date, commissioning_date, street_name, cabinet_id, nominal_wattage, actual_wattage)) 
                csvWriter.writerow((asset_id, component_id, latitude, longitude, installation_date, commissioning_date, street_name, cabinet_id, nominal_wattage, actual_wattage))

    def run(self):
        # test run
        self.getConfig(self.configFilename)
        self.connectDB()

        self.computeSunTime(self.suntime_latitude, self.suntime_longitude, self.startDate, self.endDate)
        component_id_list = self.getComponentsList()
        self.compute_results(component_id_list)

if __name__ == "__main__":

    configJSONFilename = sys.argv[1]
    testObj = ActualWattage(configJSONFilename)    
    testObj.run()            


