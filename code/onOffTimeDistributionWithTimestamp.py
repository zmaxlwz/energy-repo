import datetime  
from sunrise import sun 
import psycopg2
import csv
import sys
import json

class ComputeDistribution:

    def __init__(self, configJSONFilename):
        """ initialize variables

        """
        #configuration file name
        self.configFilename = configJSONFilename
        #1 day delta
        self.oneDayDelta = datetime.timedelta(days=1)
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
        #component_id_list = [4980]
        component_id_list = self.getComponentsList()
        #step 4:  call computeResults method
        self.computeResults(component_id_list)

    def getComponentsList(self):
        """ get assets list from assets table, which are not deleted and installation_date and commissioning_date are not null

        """                
        try:
            self.cur.execute("select c.id, c.asset_id, c.installation_date \
                              from assets as a, components as c \
                              where a.is_deleted = 'f' and a.installation_date is not null and a.commissioning_date is not null \
                              and a.id = c.asset_id and c.component_kind = 0")
        except:
            print("I am unable to get data")

        rows = self.cur.fetchall()    
        return [row[0] for row in rows]

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
            title_row = ('component_id', 'timestamp', 'time_in_minutes_from_daystart')
            csvWriter.writerow(title_row)     
            for component_id in component_id_list:
                count += 1
                print(count)
                #results = self.computeTurnOnTime(component_id)
                results = self.computeTurnOffTime(component_id)
                #print('len of results: ', len(results))
                #the results returned is a list of tuples
                if len(results) > 0:
                    for record in results:
                        #csvWriter.writerow(results)
                        csvWriter.writerow(record)
                #self.plot(results)
                
    def computeTurnOnTime(self, component_id):
        """ using switching point table, compute the switching point turn-on time in minutes from day start
            this is for Barcelona dataset, turn-on time range is [14:00 - 22:00]

        """
        timeStart = datetime.datetime.combine(self.startDate, datetime.time(hour=6))
        timeEnd = datetime.datetime.combine(self.endDate, datetime.time(hour=23))

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
        lastLogValue, lastIsLogValueOff, lastTime = None, None, None    
        for row in rows:   
            if lastTime is None:               
                lastTime = row[1]
                lastLogValue = row[2]
                lastIsLogValueOff = row[3]
            else:                
                currentTime = row[1]
                currentLogValue = row[2]
                currentIsLogValueOff = row[3]
                '''               
                if currentLogValue == 100 and lastLogValue == 0:
                    #turn on light
                    if currentTime.time() > datetime.time(6) and currentTime.time() < datetime.time(13):
                        minutesFromDayStart = (currentTime - datetime.datetime.combine(currentTime.date(), datetime.time())).total_seconds() / 60.0
                        results.append(minutesFromDayStart)
                '''
                #print(currentIsLogValueOff, lastIsLogValueOff)
                if currentIsLogValueOff == False and lastIsLogValueOff == True:
                    #turn on light
                    if currentTime.time() > datetime.time(14) and currentTime.time() < datetime.time(22):
                        minutesFromDayStart = (currentTime - datetime.datetime.combine(currentTime.date(), datetime.time())).total_seconds() / 60.0
                        results.append((component_id, currentTime, minutesFromDayStart))    
                lastLogValue = currentLogValue
                lastIsLogValueOff = currentIsLogValueOff
                lastTime = currentTime 
        
        return results    

    def computeTurnOffTime(self, component_id):   
        """ using switching point table, compute the switching point turn-off time in minutes from day start
            this is for Barcelona dataset, off time range is [2 - 10am]

        """
        timeStart = datetime.datetime.combine(self.startDate, datetime.time(hour=6))
        timeEnd = datetime.datetime.combine(self.endDate, datetime.time(hour=23))

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
        lastLogValue, lastIsLogValueOff, lastTime = None, None, None    
        for row in rows:   
            if lastTime is None:                
                lastTime = row[1]
                lastLogValue = row[2]
                lastIsLogValueOff = row[3]
            else:                
                currentTime = row[1]
                currentLogValue = row[2]
                currentIsLogValueOff = row[3]
                '''               
                if currentLogValue == 0 and lastLogValue == 100:
                    #turn off light
                    if (currentTime.time() > datetime.time(19) and currentTime.time() <= datetime.time(23, 59, 59)):
                        minutesFromDayStart = (currentTime - datetime.datetime.combine(currentTime.date() + datetime.timedelta(days=1), datetime.time())).total_seconds() / 60.0
                        results.append(minutesFromDayStart)
                    elif (currentTime.time() > datetime.time(0) and currentTime.time() < datetime.time(3)):
                        minutesFromDayStart = (currentTime - datetime.datetime.combine(currentTime.date(), datetime.time())).total_seconds() / 60.0
                        results.append(minutesFromDayStart)    
                '''
                if currentIsLogValueOff == True and lastIsLogValueOff == False:
                    #turn off light
                    if currentTime.time() > datetime.time(2) and currentTime.time() < datetime.time(10):
                        minutesFromDayStart = (currentTime - datetime.datetime.combine(currentTime.date(), datetime.time())).total_seconds() / 60.0
                        results.append((component_id, currentTime, minutesFromDayStart))     
                lastLogValue = currentLogValue
                lastIsLogValueOff = currentIsLogValueOff
                lastTime = currentTime 
        
        return results    

if __name__ == "__main__":

    configJSONFilename = sys.argv[1]
    testObj = ComputeDistribution(configJSONFilename)    
    testObj.run()            