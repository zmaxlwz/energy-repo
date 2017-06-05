import datetime
from sunrise import sun
import psycopg2
import csv
import sys
import json
import statistics

class ComputeSwitchingTime:

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
                                  and component_kind = 0", (asset_id, ))
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

            results.append((asset_id, component_id, latitude, longitude, installation_date, commissioning_date, street_name, cabinet_id))

        return results

    def compute_light_on_time(self, component_id_tuple, start_time, end_time):
        """ use switching point table, compute the light on time for the input component from [0:00 - 23:59:59] in UTC

        """
        #timeStart = datetime.datetime.combine(self.startDate, datetime.time(hour=6))
        #timeEnd = datetime.datetime.combine(self.endDate, datetime.time(hour=23))

        asset_id = component_id_tuple[0]
        component_id = component_id_tuple[1]
        latitude = component_id_tuple[2]
        longitude = component_id_tuple[3]
        installation_date = component_id_tuple[4]
        commissioning_date = component_id_tuple[5]
        street_name = component_id_tuple[6]
        cabinet_id = component_id_tuple[7]

        try:
            self.cur.execute("select component_id, timestamp_utc, log_value, is_log_value_off \
                              from switching_points \
                              where component_id = %s \
                              and timestamp_utc > %s and timestamp_utc < %s  \
                              order by timestamp_utc", (component_id, start_time, end_time))
        except:
            print("I am unable to get data")

        rows = self.cur.fetchall() 

        if len(rows) == 0:
            return {}

        # the results include tuples in the format of (component_id, date, light_on_time_in_minutes)
        results = {}

        current_date = None        
        for row in rows:   
            # currentTime is in UTC  
            currentTime = row[1]
            currentLogValue = row[2]
            currentIsLogValueOff = row[3]
            if current_date is None:
                # it is the first day
                current_date = currentTime.date()
                #reset variables for the new date
                totalOnTime = 0
                numIntervals = 0
                switching_on_time = None
            elif current_date != currentTime.date():
                # it is a new day
                if switching_on_time is not None:
                    # the light is on until the end of day
                    end_of_day_time = datetime.datetime.combine(current_date, datetime.time(23, 59, 59))
                    totalOnTime += (end_of_day_time - switching_on_time).total_seconds() / 60.0
                    numIntervals += 1
                    switching_on_time = datetime.datetime.combine(currentTime.date(), datetime.time())
                #first write result
                if totalOnTime > 0:
                    #write result
                    #results.append((asset_id, component_id, latitude, longitude, installation_date, commissioning_date, street_name, cabinet_id, current_date, numIntervals, totalOnTime)) 
                    results[current_date] = totalOnTime
                    #results.append((component_id, current_date, numIntervals, totalOnTime))
                current_date = currentTime.date()
                #reset variables for the new date
                totalOnTime = 0
                numIntervals = 0 
                #switching_on_time = None      

            if currentIsLogValueOff == False and switching_on_time is None:
                switching_on_time = currentTime
            elif currentIsLogValueOff == True and switching_on_time is not None:
                #1. compute the time length  
                secondsInterval = (currentTime - switching_on_time).total_seconds()
                totalOnTime += secondsInterval / 60.0
                #2. increment the interval count
                numIntervals += 1
                #3. set switching_on_time to None
                switching_on_time = None     

        if switching_on_time is not None:
            # the light is on until the end of day
            end_of_day_time = datetime.datetime.combine(current_date, datetime.time(23, 59, 59))
            totalOnTime += (end_of_day_time - switching_on_time).total_seconds() / 60.0
            numIntervals += 1

        if totalOnTime > 0:
            #write result
            #results.append((asset_id, component_id, latitude, longitude, installation_date, commissioning_date, street_name, cabinet_id, current_date, numIntervals, totalOnTime)) 
            results[current_date] = totalOnTime
            #results.append((component_id, current_date, numIntervals, totalOnTime))

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

    def find_dayburners_energy_deviation_rolling_window_avg(self, asset_tuple, start_time, end_time):
        """ find dayburners by calculating energy deviation from 30 day rolling window average 
            if # of stdev > 1.5 and deviation > 0.2 kwh, report that record
        """

        asset_id = asset_tuple[0]
        component_id = asset_tuple[1]
        latitude = asset_tuple[2]
        longitude = asset_tuple[3]
        installation_date = asset_tuple[4]
        commissioning_date = asset_tuple[5]
        street_name = asset_tuple[6]
        cabinet_id = asset_tuple[7]

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
                #print(lastTime, dailyEnergyConsumption)
                if len(energy_rolling_window) == 30:
                    # the rolling window is full
                    # compute the mean and std of energy consumption within the rolling window       
                    avg_energy_consumption = statistics.mean(energy_rolling_window)
                    std_energy_consumption = statistics.stdev(energy_rolling_window)
                    if std_energy_consumption == 0:
                        if dailyEnergyConsumption - avg_energy_consumption > 0.2:
                            results.append((asset_id, component_id, latitude, longitude, installation_date, commissioning_date, street_name, cabinet_id, lastDate, dailyEnergyConsumption, avg_energy_consumption, std_energy_consumption, None))
                    else:    
                        num_std = (dailyEnergyConsumption - avg_energy_consumption) / std_energy_consumption
                        #if num_std < -1.5 or num_std > 1.5:
                        if num_std > 1.5 and dailyEnergyConsumption - avg_energy_consumption > 0.2:
                            results.append((asset_id, component_id, latitude, longitude, installation_date, commissioning_date, street_name, cabinet_id, lastDate, dailyEnergyConsumption, avg_energy_consumption, std_energy_consumption, num_std))
                    '''
                    energy_deviation = dailyEnergyConsumption - avg_energy_consumption
                    #if energy_deviation < -0.2 or energy_deviation > 0.2:
                    if energy_deviation > 0.2:    
                        #report this abnormal case
                        #print('{0} {1:5.1f} {2: 5.4f} {3} {4:5.1f} {5} {6:5.1f}'.format(asset_id, dailyEnergyConsumption, num_std, lastDate, lastEnergy, currentDate, currentEnergy))
                        
                        results.append((asset_id, lastDate, dailyEnergyConsumption, avg_energy_consumption, energy_deviation))
                    '''
                    #update rolling window    
                    energy_rolling_window.pop(0)
                    energy_rolling_window.append(dailyEnergyConsumption)    
                else:
                    # the rolling window is not full, just append new element to the end of the rolling window
                    energy_rolling_window.append(dailyEnergyConsumption)    
                #update record
                lastTime = currentTime    
                lastDate = currentDate
                lastEnergy = currentEnergy 
        return results 
        
                
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
            title_row = ('asset_id', 'component_id', 'latitude', 'longitude', 'installation_date', 'commissioning_date', 'street_name', 'cabinet_id', 'current_date', 'dailyEnergyConsumption', 'avg_energy_consumption', 'std_energy_consumption', 'num_std')
            csvWriter.writerow(title_row)     
            for component_id_tuple in component_id_list:
                count += 1
                #only compute for first 500 components
                #if count > 500:
                #    break
                print(count)
                results = self.find_dayburners_energy_deviation_rolling_window_avg(component_id_tuple, start_time, end_time)
                on_time_dict = self.compute_light_on_time(component_id_tuple, start_time, end_time)
                #print('len of results: ', len(results))
                #the results returned is a list of tuples
                #if len(results) > 0:
                for record in results:
                    #csvWriter.writerow(results)
                    date = record[8]
                    if date not in on_time_dict:
                        print(record[0], record[1])
                        print(on_time_dict)
                        continue
                    total_light_on_time = on_time_dict[date]
                    sunrise_time = self.sunriseTimeDict[date]
                    sunset_time = self.sunsetTimeDict[date]
                    daytime_in_min = (sunset_time - sunrise_time).total_seconds() / 60.0
                    nighttime_in_min = 24 * 60 - daytime_in_min
                    if total_light_on_time - nighttime_in_min > 60:
                        # the difference between total_light_on_time and nighttime_in_min is more than 60 minutes
                        # this is day-burning record, write to output file
                        csvWriter.writerow(record)
                #self.plot(results)    

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
        end_time = datetime.datetime(2016, 10, 1, 0, 0, 0)  
        #results = self.compute_light_on_time(component_id, start_time, end_time)
        #print(results)
        
        self.computeSunTime(self.suntime_latitude, self.suntime_longitude, start_time.date(), end_time.date())
        print(self.sunriseTimeDict)
        print(self.sunsetTimeDict)

if __name__ == "__main__":

    configJSONFilename = sys.argv[1]
    testObj = ComputeSwitchingTime(configJSONFilename)    
    testObj.run()            
