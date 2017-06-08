import datetime
from sunrise import sun
import psycopg2
import csv
import sys
import json
import statistics
from collections import Counter

class DayburnerEnergyOnly:

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

    def find_dayburners_energy_with_actual_wattage(self, asset_tuple, actual_wattage, start_time, end_time):
        """ find dayburners by use actual wattage, which is computed by reverse engineering
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
            self.cur.execute("select b.asset_id, a.kwh, a.timestamp_utc \
                              from energy_metering_points b, energy_meter_readings a \
                              where b.asset_id = %s and b.id = a.metering_point_id \
                              and a.timestamp_utc >= %s and a.timestamp_utc < %s \
                              order by b.asset_id, a.timestamp_utc", (asset_id, start_time, end_time))
        except:
            print("I am unable to get data")        

        rows = self.cur.fetchall()

        results = []
        lastTime = None
        lastDate = None
        lastEnergy = None
        for row in rows:
            if lastDate is None:
                # it is the first date
                lastTime = row[2]
                lastDate = lastTime.date()
                lastEnergy = row[1]
            elif row[2].date() != lastDate:
                # it is a new date
                currentTime = row[2]
                currentDate = currentTime.date()
                currentEnergy = row[1] 
                energyConsumption = currentEnergy - lastEnergy
                num_days = (currentDate - lastDate).days
                dailyEnergyConsumption = energyConsumption / num_days

                # use actual wattage to compute the on time in minutes
                actual_on_time_in_min = (dailyEnergyConsumption * 1000 / actual_wattage) * 60.0

                # compute the night time using sunset and sunrise time and normal energy consumption
                sunrise_time = self.sunriseTimeDict[lastDate]
                sunset_time = self.sunsetTimeDict[lastDate]
                daytime_in_min = (sunset_time - sunrise_time).total_seconds() / 60.0
                nighttime_in_min = 24 * 60 - daytime_in_min

                normal_energy_consumption = (actual_wattage * nighttime_in_min / 60.0) / 1000.0 

                # compute the difference between computed on time and night time,  
                # also the actual energy consumption and normal energy consumption
                # if the time difference is more than 1 hour and energy difference is more than 0.1 kwh, report
                if actual_on_time_in_min - nighttime_in_min > 60 and dailyEnergyConsumption - normal_energy_consumption > 0.2:
                    # report the record
                    results.append((asset_id, component_id, latitude, longitude, installation_date, commissioning_date, street_name, cabinet_id, nominal_wattage, actual_wattage, lastDate, dailyEnergyConsumption, normal_energy_consumption, actual_on_time_in_min, nighttime_in_min))
   
                #update record
                lastTime = currentTime    
                lastDate = currentDate
                lastEnergy = currentEnergy 
        return results 


    def find_dayburners_aggregation_energy_with_actual_wattage(self, asset_tuple, actual_wattage, start_time, end_time):
        """ use energy_aggregation_daily table to get daily energy consumption, which is measured_kwh + interpolated_kwh

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
            currentTime = row[3]
            currentDate = currentTime.date()
            interpolated_kwh = row[1]
            measured_kwh = row[2]
            dailyEnergyConsumption = interpolated_kwh + measured_kwh
            # use actual wattage to compute the on time in minutes
            actual_on_time_in_min = (dailyEnergyConsumption * 1000 / actual_wattage) * 60.0

            # compute the night time using sunset and sunrise time and normal energy consumption
            sunrise_time = self.sunriseTimeDict[currentDate]
            sunset_time = self.sunsetTimeDict[currentDate]
            daytime_in_min = (sunset_time - sunrise_time).total_seconds() / 60.0
            nighttime_in_min = 24 * 60 - daytime_in_min

            normal_energy_consumption = (actual_wattage * nighttime_in_min / 60.0) / 1000.0 

            # compute the difference between computed on time and night time,  
            # also the actual energy consumption and normal energy consumption
            # if the time difference is more than 1 hour and energy difference is more than 0.1 kwh, report
            if actual_on_time_in_min - nighttime_in_min > 60 and dailyEnergyConsumption - normal_energy_consumption > 0.2:
                # report the record
                results.append((asset_id, component_id, latitude, longitude, installation_date, commissioning_date, street_name, cabinet_id, nominal_wattage, actual_wattage, currentDate, dailyEnergyConsumption, normal_energy_consumption, actual_on_time_in_min, nighttime_in_min))

        return results

    def compute_actual_wattage(self, asset_tuple, start_time, end_time):
        """ compute asset's actual wattage according to energy consumption and sunset to sunrise night time
            if the days between start_time and end_time are more than 30 days, use the first 30 days' energy consumption
            if the days are fewer than 30 days, use all days' energy consumption
            get the most frequent energy consumption as the normal energy consumption and use the sunset to sunrise time to compute wattage

        """

        asset_id = asset_tuple[0]

        try:
            self.cur.execute("select b.asset_id, a.kwh, a.timestamp_utc \
                              from energy_metering_points b, energy_meter_readings a \
                              where b.asset_id = %s and b.id = a.metering_point_id \
                              and a.timestamp_utc >= %s and a.timestamp_utc < %s \
                              order by b.asset_id, a.timestamp_utc", (asset_id, start_time, end_time))
        except:
            print("I am unable to get data")        

        rows = self.cur.fetchall()

        energy_rolling_window = []
        lastTime = None
        lastDate = None
        lastEnergy = None
        for row in rows:
            if lastDate is None:
                # it is the first date
                lastTime = row[2]
                lastDate = lastTime.date()
                lastEnergy = row[1]
            elif row[2].date() != lastDate:
                # it is a new date
                currentTime = row[2]
                currentDate = currentTime.date()
                currentEnergy = row[1] 
                energyConsumption = currentEnergy - lastEnergy
                num_days = (currentDate - lastDate).days
                dailyEnergyConsumption = round(energyConsumption / num_days, 2)
                #print(lastTime, dailyEnergyConsumption)
                energy_rolling_window.append(dailyEnergyConsumption) 
                if len(energy_rolling_window) >= 30:
                    # the rolling window is full
                    break   
                #update record
                lastTime = currentTime    
                lastDate = currentDate
                lastEnergy = currentEnergy 

        #print(energy_rolling_window)
        #print(self.get_most_frequent_value(energy_rolling_window))
        #print(currentDate)

        energy_rolling_window = [x for x in energy_rolling_window if x != 0]
        if len(energy_rolling_window) == 0:
            return None

        normal_energy_consumption = self.get_most_frequent_value(energy_rolling_window)
        sunrise_time = self.sunriseTimeDict[currentDate]
        sunset_time = self.sunsetTimeDict[currentDate]
        daytime_in_hours = (sunset_time - sunrise_time).total_seconds() / 60.0 / 60.0
        nighttime_in_hours = 24 - daytime_in_hours
        
        # compute the actual wattage for the asset in watts
        actual_wattage = (normal_energy_consumption / nighttime_in_hours) * 1000

        return actual_wattage 

    def get_most_frequent_value(self, energy_rolling_window):
        """ get the most frequent value from the input energy_rolling_window
            and assume it is the normal daily energy consumption value

        """
        # get the count for each value in energy_rolling_window
        #print(energy_rolling_window)
        counter = Counter(energy_rolling_window)   
        result = counter.most_common(1)
        most_frequent_value = result[0][0]
        return most_frequent_value
                
    def computeResults(self, component_id_list):
        """ compute results

        """    
        #id = 3776
        #latitude = -6.118187
        #longitude = 106.894265
        #assets = [(3776, -6.118187, 106.894265), (13532, -6.102635, 106.932242)]
        #assets = [(100280, -6.097081, 106.978368)]
        #assets = self.getAssetsList()
        #print(assets)
        count = 0
        start_time = datetime.datetime.combine(self.startDate, datetime.time())
        end_time = datetime.datetime.combine(self.endDate, datetime.time())
        with open(self.outputFilename, "w") as csvFile:
            csvWriter = csv.writer(csvFile, delimiter=',')  
            title_row = ('asset_id', 'component_id', 'latitude', 'longitude', 'installation_date', 'commissioning_date', 'street_name', 'cabinet_id', 'nominal_wattage', 'actual_wattage', 'current_date', 'dailyEnergyConsumption', 'normal_energy_consumption', 'actual_on_time_in_min', 'nighttime_in_min')
            csvWriter.writerow(title_row)     
            for component_id_tuple in component_id_list:
                count += 1
                #only compute for first 500 components
                #if count > 500:
                #    break
                print(count)
                #print(component_id_tuple)
                actual_wattage = self.compute_actual_wattage(component_id_tuple, start_time, end_time)
                if actual_wattage is None:
                    continue

                #results = self.find_dayburners_energy_with_actual_wattage(component_id_tuple, actual_wattage, start_time, end_time)             
                results = self.find_dayburners_aggregation_energy_with_actual_wattage(component_id_tuple, actual_wattage, start_time, end_time)                
                #print('len of results: ', len(results))
                #the results returned is a list of tuples
                #if len(results) > 0:
                for record in results:
                    csvWriter.writerow(record)    

    def run(self):
        """  call this method to run the program

        """
        #step 1:  read from json config file, get db connect parameter, time period to check, output file name
        self.getConfig(self.configFilename)
        #step 2:  connect to db
        self.connectDB()
        #step 3:  compute sunrise and sunset time 
        self.computeSunTime(self.suntime_latitude, self.suntime_longitude, self.startDate, self.endDate)
        #step 3:  get components list
        #component_id_list = [951]
        component_id_list = self.getComponentsList()
        #step 4:  call computeResults method
        self.computeResults(component_id_list)
        
    def run2(self):
        # test run
        self.getConfig(self.configFilename)
        self.connectDB()
        
        #component_id = 3209
        start_time = datetime.datetime(2016, 9, 1, 0, 0, 0)
        end_time = datetime.datetime(2016, 9, 25, 0, 0, 0)  
        #results = self.compute_light_on_time(component_id, start_time, end_time)
        #print(results)
        
        self.computeSunTime(self.suntime_latitude, self.suntime_longitude, start_time.date(), end_time.date())
        #print(self.sunriseTimeDict)
        #print(self.sunsetTimeDict)

        asset_id = 2140

        actual_wattage = self.compute_actual_wattage(asset_id, start_time, end_time)
        print(actual_wattage)

if __name__ == "__main__":

    configJSONFilename = sys.argv[1]
    testObj = DayburnerEnergyOnly(configJSONFilename)    
    testObj.run()            

