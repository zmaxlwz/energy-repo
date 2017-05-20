import datetime  
from sunrise import sun 
import psycopg2
import csv
import sys
import json

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
            
    def run(self):
        """  call this method to run the program

        """
        #step 1:  read from json config file, get db connect parameter, time period to check, output file name
        self.getConfig(self.configFilename)
        #step 2:  connect to db
        self.connectDB()
        #step 3:  compute sunrise and sunset time 
        #self.computeSunTime(self.suntime_latitude, self.suntime_longitude, self.startDate, self.endDate)
        #step 3:  get components list
        #component_id_list = [951]
        component_id_list = self.getComponentsList()
        #step 4:  call computeResults method
        self.computeResults(component_id_list)

    def getComponentsList(self):
        """ get assets list from assets table, which are not deleted and installation_date and commissioning_date are not null

        """                
        try:
            self.cur.execute("select c.id, c.asset_id, a.latitude, a.longitude, a.installation_date, a.commissioning_date \
                              from assets as a, components as c \
                              where a.is_deleted = 'f' and a.installation_date is not null and a.commissioning_date is not null \
                              and a.id = c.asset_id and c.component_kind = 0")
        except:
            print("I am unable to get data")

        rows = self.cur.fetchall()   
        results = []
        for row in rows:
            asset_id = row[1]
            component_id = row[0]
            latitude = row[2]
            longitude = row[3]
            installation_date = row[4]
            commissioning_date = row[5]

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
        with open(self.outputFilename, "w") as csvFile:
            csvWriter = csv.writer(csvFile, delimiter=',')  
            title_row = ('asset_id', 'component_id', 'latitude', 'longitude', 'installation_date', 'commissioning_date', 'street_name', 'cabinet_id', 'current_date', 'num_intervals', 'total_on_time')
            csvWriter.writerow(title_row)     
            for component_id_tuple in component_id_list:
                count += 1
                #only compute for first 500 components
                #if count > 500:
                #    break
                print(count)
                results = self.computeOnTime(component_id_tuple)
                #print('len of results: ', len(results))
                #the results returned is a list of tuples
                if len(results) > 0:
                    for record in results:
                        #csvWriter.writerow(results)
                        csvWriter.writerow(record)
                #self.plot(results)
                
    def computeOnTime(self, component_id_tuple):
        """ using switching point table, compute the switching point turn-on time in minutes from day start
            this is for Barcelona dataset, turn-on time range is [14:00 - 22:00]

        """
        timeStart = datetime.datetime.combine(self.startDate, datetime.time(hour=6))
        timeEnd = datetime.datetime.combine(self.endDate, datetime.time(hour=23))

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
                              order by timestamp_utc;", (component_id, timeStart, timeEnd))
        except:
            print("I am unable to get data")

        rows = self.cur.fetchall() 
        if len(rows) == 0:
            return []

        results = []
        #lastLogValue, lastIsLogValueOff, lastTime = None, None, None    
        current_date = None        
        for row in rows:   

            #convert to Barcelona local time   
            # Jan +1 hour        
            #currentTime = row[1] + self.oneHourDelta
            # Sep +2 hours
            currentTime = row[1] + self.twoHoursDelta
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
                #first write result
                if totalOnTime > 0:
                    #write result
                    results.append((asset_id, component_id, latitude, longitude, installation_date, commissioning_date, street_name, cabinet_id, current_date, numIntervals, totalOnTime)) 
                current_date = currentTime.date()
                #reset variables for the new date
                totalOnTime = 0
                numIntervals = 0 
                switching_on_time = None      

            if currentTime.time() > datetime.time(8, 30) and currentTime.time() < datetime.time(19):
                #it is within day time
                if currentIsLogValueOff == False and switching_on_time is None:
                    switching_on_time = currentTime
                elif currentIsLogValueOff == True and switching_on_time is not None:
                    #1. compute the time length  
                    secondsInterval = (currentTime - switching_on_time).total_seconds()
                    totalOnTime += secondsInterval
                    #2. increment the interval count
                    numIntervals += 1
                    #3. set switching_on_time to None
                    switching_on_time = None          
        
        if totalOnTime > 0:
            #write result
            results.append((asset_id, component_id, latitude, longitude, installation_date, commissioning_date, street_name, cabinet_id, current_date, numIntervals, totalOnTime)) 

        return results    

if __name__ == "__main__":

    configJSONFilename = sys.argv[1]
    testObj = ComputeSwitchingTime(configJSONFilename)    
    testObj.run()            
