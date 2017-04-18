import datetime  
from sunrise import sun 
import psycopg2
import csv
import sys

class Test:

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
            self.suntime_latitude = config_data['suntime_location_latitude']
            #suntime location longitude
            self.suntime_longitude = config_data['suntime_location_longitude']
            
    def run(self):
        """  call this method to run the program

        """
        #step 1:  read from json config file, get db connect parameter, time period to check, output file name
        self.getConfig(self.configFilename)
        #step 2:  connect to db
        self.connectDB()
        #step 3:  compute sunrise and sunset time 
        #self.computeSunTime(self.suntime_latitude, self.suntime_longitude, self.startDate, self.endDate)
        #step 3:  get assets list
        #assets = self.getAssetsList()
        assets = [(2490, -6.113218, 106.778701)]
        #step 4:  call computeResults method
        self.computeResults(assets)
        
    def computeResults(self, assets):
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
            for asset in assets:
                count += 1
                print(count)
                #id, latitude, longitude = asset
                #results = self.computeEnergyForOneAsset(id, latitude, longitude)
                results = self.computeEnergyForOneAsset(asset)
                for record in results:
                    csvWriter.writerow(record)
                    
    def computeEnergyForOneAsset(self, asset):    
        """ compute the energy consumption for one asset

        Args:
            asset: a tuple, (id, latitude, longitude, installation_date, commissioning_date)

        """    
        #self.sun = sun(lat=lat, long=long)
        #get installation date
        #installation_date = self.getInstallationDate(id)     #which is datetime.date() object
        #get commission date
        #year = 2016
        #month = 7
        #day = 1
        #date = datetime.datetime(year, month, day, 8, 0, 0)
        #commissioning_date = self.getCommissioningDate(id)    #which is datetime.date() object
        #print(asset)
        id, lat, long = asset
        #get last date
        #last_date = datetime.datetime(2017, 2, 7)
        #last_meter_reading_datetime = self.getLastMeterReadingDate(id)
        
        #date = datetime.datetime.combine(commissioning_date, datetime.time(hour=8))
        #date = date + self.oneDayDelta
        
        timeStart = datetime.datetime.combine(self.startDate, datetime.time(hour=6))
        timeEnd = datetime.datetime.combine(self.endDate, datetime.time(hour=10)) 

        return self.computeEnergyForEachInterval(id, timeStart, timeEnd)  
        

    def computeEnergyForEachInterval(self, asset_id, timeStart, timeEnd):
        """ accumulate the light 'on' time and energy consumption during daytime

        return:  (onTime, EnergyConsumed)

        """   
        #daytimeStart = datetime.datetime(2016, 7, 1, 20, 3, 1)
        #daytimeEnd = datetime.datetime(2016, 7, 2, 10, 49, 18) 

        try:
            self.cur.execute("select b.asset_id , a.kwh, a.timestamp_utc \
                         from energy_metering_points b, energy_meter_readings a \
                         where b.asset_id = %s and b.id = a.metering_point_id \
                            and a.timestamp_utc >= %s and a.timestamp_utc <= %s \
                         order by a.timestamp_utc", (asset_id, timeStart, timeEnd))
        except:
            print("I am unable to get data")

        rows = self.cur.fetchall() 
        if len(rows) == 0:
            return []
        
        #totalOnTime, totalEnergyConsumed, totalWatts = 0, 0, 0
        #num_interval, num_interval_positive = 0, 0
        results = []
        lastEnergy, lastTime = None, None    
        for row in rows:   
            if lastTime is None:
                lastEnergy = row[1]
                lastTime = row[2]
            else:
                currentEnergy = row[1]
                currentTime = row[2]
                secondsInterval = (currentTime - lastTime).total_seconds()
                energyConsumed = currentEnergy - lastEnergy                
                if secondsInterval == 0:
                    continue
                #num_interval += 1    
                consumptionRate = (energyConsumed * 1000) / (secondsInterval / 3600.0)
                #print(row)
                #print(energyConsumed, consumptionRate, secondsInterval)
                results.append((asset_id, currentTime, consumptionRate))
                lastEnergy = currentEnergy
                lastTime = currentTime 
        
        return results

            