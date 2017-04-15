import datetime  
from sunrise import sun 
import psycopg2
import csv
import sys

class EnergyConsumption:

    def __init__(self, outputFilename, dbName, energyThreshold):
        """ initialize variables

        """
        #output filename
        self.outputFilename = outputFilename
        #db name
        self.dbname = dbName
        #define energyThreshold: 2 watt
        self.energyThreshold = int(energyThreshold)      
        #define onTime threshold for a day: in seconds
        self.onTimeThreshold = 0
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
            print(self.dbname)
            self.conn = psycopg2.connect("dbname=%s user='awsmaster' password='philips2017' host='citytouch-buenos-aires-log.cuxwb2nbset5.us-west-2.rds.amazonaws.com' port='5432'" % (self.dbname,))
            print("connected!")
        except psycopg2.Error as e:
            print("I am unable to connect to the database")
            print(e)

        #define cursor
        self.cur = self.conn.cursor()    

    def run(self):
        """  call this method to run the program

        """
        #step 1:  read from json config file, get db connect parameter, time period to check, output file name
        self.startDate = datetime.date(2017, 1, 1)
        self.endDate = datetime.date(2017, 1, 7)
        latitude = -6.218868
        longitude = 106.845189
        #step 2:  connect to db
        self.connectDB()
        #step 3:  compute sunrise and sunset time 
        self.computeSunTime(latitude, longitude, self.startDate, self.endDate)
        #step 3:  get assets list
        assets = self.getAssetsList()
        #assets = [(3776, -6.118187, 106.894265), (13532, -6.102635, 106.932242)]
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

    def getAssetsList(self):
        """ get assets list from assets table, which are not deleted and installation_date and commissioning_date are not null

        """                
        try:
            self.cur.execute("select id, latitude, longitude, installation_date, commissioning_date \
                              from assets \
                              where is_deleted = 'f' \
                              and installation_date is not null and commissioning_date is not null")
        except:
            print("I am unable to get data")

        return self.cur.fetchall()


    #def computeEnergyForOneAsset(self, id, lat, long):
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
        id, lat, long, installation_date, commissioning_date = asset
        #get last date
        #last_date = datetime.datetime(2017, 2, 7)
        last_meter_reading_datetime = self.getLastMeterReadingDate(id)
        
        #date = datetime.datetime.combine(commissioning_date, datetime.time(hour=8))
        #date = date + self.oneDayDelta
        results = []
        if last_meter_reading_datetime is None:
            return results
        date = max(commissioning_date, self.startDate)
        last_date = min(last_meter_reading_datetime.date(), self.endDate)    
        while date < last_date:        
            #daytimeStart, daytimeEnd = self.computeDaytimeStartEnd(date)
            daytimeStart = self.sunriseTimeDict[date]
            daytimeEnd = self.sunsetTimeDict[date]
            (onTime, energyConsumedKwh, energyConsumedWatts, numIntervals, numPositiveIntervals) = self.computeEnergyForOneDay(id, daytimeStart, daytimeEnd)
            if onTime > self.onTimeThreshold:
                #print(date, id, lat, long, installation_date, commissioning_date, onTime, energyConsumedKwh, energyConsumedWatts)
                results.append((date, id, lat, long, installation_date, commissioning_date, onTime, energyConsumedKwh, energyConsumedWatts, numIntervals, numPositiveIntervals))
            date += self.oneDayDelta  

        return results      

    def getInstallationDate(self, id):
        """ get asset installation date

        """        	
        try:
            self.cur.execute("select id, installation_date from assets where id = %s", (id,))
        except:
            print("I am unable to get data")

        rows = self.cur.fetchall()
        if len(rows) != 1:
            raise Exception("retrieve installation date error for asset " + str(id))
        else:
            return rows[0][1]  

    def getCommissioningDate(self, id):
        """ get asset commissioning date 

        """           
        try:
            self.cur.execute("select id, commissioning_date from assets where id = %s", (id,))
        except:
            print("I am unable to get data")

        rows = self.cur.fetchall()
        if len(rows) != 1:
            raise Exception("retrieve commissioning date error for asset " + str(id))
        else:
            return rows[0][1]   

    def getLastMeterReadingDate(self, id):
        """ get last meter reading date for the asset

        """         
        try:
            self.cur.execute("select max(a.timestamp_utc) \
                              from energy_meter_readings a, energy_metering_points b \
                              where a.metering_point_id = b.id and b.asset_id = %s", (id,))
        except:
            print("I am unable to get data")

        rows = self.cur.fetchall()
        if len(rows) != 1:
            raise Exception("retrieve commissioning date error for asset " + str(id))
        else:
            return rows[0][0]  

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
        #print(sunset_datetime)
        #compute adjusted daytime start time and daytime end time
        #halfHourDelta = datetime.timedelta(hours=0, minutes=30, seconds=0)
        adjustedStartTime = sunrise_datetime + self.sunriseTimeDelta
        #print(adjustedStartTime)
        adjustedEndTime = sunset_datetime - self.sunsetTimeDelta
        #print(adjustedEndTime)
        return (adjustedStartTime, adjustedEndTime)

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
        while dateTime.date() < endDate:        
            daytimeStart, daytimeEnd = self.computeDaytimeStartEnd(dateTime)
            self.sunriseTimeDict[dateTime.date()] = daytimeStart
            self.sunsetTimeDict[dateTime.date()] = daytimeEnd
            dateTime += self.oneDayDelta
    
    def computeEnergyForOneDay(self, asset_id, daytimeStart, daytimeEnd):
        """ accumulate the light 'on' time and energy consumption during daytime

        return:  (onTime, EnergyConsumed)

        """   
        #daytimeStart = datetime.datetime(2016, 7, 1, 20, 3, 1)
        #daytimeEnd = datetime.datetime(2016, 7, 2, 10, 49, 18) 

        try:
            self.cur.execute("select b.asset_id , a.kwh, a.timestamp_utc \
                         from energy_meter_readings a, energy_metering_points b \
                         where a.metering_point_id = b.id and b.asset_id = %s \
                            and a.timestamp_utc >= %s and a.timestamp_utc <= %s \
                         order by a.timestamp_utc", (asset_id, daytimeStart, daytimeEnd))
        except:
            print("I am unable to get data")

        rows = self.cur.fetchall() 
        if len(rows) == 0:
            return (0, 0, 0, 0, 0)
        
        totalOnTime, totalEnergyConsumed, totalWatts = 0, 0, 0
        num_interval, num_interval_positive = 0, 0
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
                lastEnergy = currentEnergy
                lastTime = currentTime 
                if secondsInterval == 0:
                    continue
                num_interval += 1    
                consumptionRate = (energyConsumed * 1000) / (secondsInterval / 3600.0)
                #print(row)
                #print(energyConsumed, consumptionRate, secondsInterval)
                if consumptionRate > self.energyThreshold:
                    totalOnTime += secondsInterval
                    totalEnergyConsumed += energyConsumed
                    num_interval_positive += 1
        if totalOnTime == 0:
            totalWatts = 0
        else:
            totalWatts = (totalEnergyConsumed * 1000) / (totalOnTime / 3600.0)               
        #print(totalOnTime, totalEnergyConsumed)
        return totalOnTime, totalEnergyConsumed, totalWatts, num_interval, num_interval_positive


if __name__ == "__main__":

    outputFilename = sys.argv[1]
    dbName = sys.argv[2]
    energyThreshold = sys.argv[3]
    energyConsumption = EnergyConsumption(outputFilename, dbName, energyThreshold)    
    energyConsumption.run()
    
    

