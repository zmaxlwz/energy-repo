import datetime  
from sunrise import sun 
import psycopg2
import csv

class EnergyConsumption:

    def __init__(self):
        
        #connect to the database
        try:
            self.conn = psycopg2.connect("dbname='jakarta_utara' user='awsmaster' password='philips2017' host='citytouch-buenos-aires-log.cuxwb2nbset5.us-west-2.rds.amazonaws.com' port='5432'")
            print("connected!")
        except psycopg2.Error as e:
            print("I am unable to connect to the database")
            print(e)

        #define cursor
        self.cur = self.conn.cursor()

        #define threshold: 2 watt
        self.threshold = 2
        
        #pass

    def computeResults(self):
        """ compute results

        """    
        #id = 3776
        #latitude = -6.118187
        #longitude = 106.894265
        #assets = [(3776, -6.118187, 106.894265), (13532, -6.102635, 106.932242)]
        #assets = [(100280, -6.097081, 106.978368)]
        assets = self.getAssetsList()
        #print(assets)
        with open("out.csv", "w") as csvFile:
            csvWriter = csv.writer(csvFile, delimiter=',')
            for asset in assets:
                id, latitude, longitude = asset
                results = self.computeEnergyForOneAsset(id, latitude, longitude)
                for record in results:
                    csvWriter.writerow(record)

    def getAssetsList(self):
        """ get assets list from assets table, which are not deleted and installation_date and commissioning_date are not null

        """                
        try:
            self.cur.execute("select id, latitude, longitude \
                              from assets \
                              where is_deleted = 'f' \
                              and installation_date is not null and commissioning_date is not null")
        except:
            print("I am unable to get data")

        return self.cur.fetchall()


    def computeEnergyForOneAsset(self, id, lat, long):
        """ compute the energy consumption for one asset

        """    
        self.sun = sun(lat=lat, long=long)
        #get installation date
        installation_date = self.getInstallationDate(id)     #which is datetime.date() object
        #get commission date
        #year = 2016
        #month = 7
        #day = 1
        #date = datetime.datetime(year, month, day, 8, 0, 0)
        commissioning_date = self.getCommissioningDate(id)    #which is datetime.date() object

        #get last date
        #last_date = datetime.datetime(2017, 2, 7)
        last_date = self.getLastMeterReadingDate(id)
        #1 day delta
        oneDayDelta = datetime.timedelta(days=1)
        
        date = datetime.datetime.combine(commissioning_date, datetime.time(hour=8))
        date = date + oneDayDelta
        results = []
        if last_date is None:
            return results
        while date < last_date:        
            daytimeStart, daytimeEnd = self.computeDaytimeStartEnd(date)
            (onTime, energyConsumedKwh, energyConsumedWatts) = self.computeEnergyForOneDay(id, daytimeStart, daytimeEnd)
            if onTime > 1800:
                #print(date.date(), id, lat, long, installation_date, commissioning_date, onTime, energyConsumedKwh, energyConsumedWatts)
                results.append((date.date(), id, lat, long, installation_date, commissioning_date, onTime, energyConsumedKwh, energyConsumedWatts))
            date += oneDayDelta  

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
        halfHourDelta = datetime.timedelta(hours=0, minutes=30, seconds=0)
        adjustedStartTime = sunrise_datetime + halfHourDelta
        #print(adjustedStartTime)
        adjustedEndTime = sunset_datetime - halfHourDelta
        #print(adjustedEndTime)
        return (adjustedStartTime, adjustedEndTime)

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
            return (0, 0, 0)
        
        totalOnTime, totalEnergyConsumed, totalWatts = 0, 0, 0
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
                consumptionRate = (energyConsumed * 1000 / (secondsInterval / 3600.0))
                #print(row)
                #print(energyConsumed, consumptionRate, secondsInterval)
                if consumptionRate > self.threshold:
                    totalOnTime += secondsInterval
                    totalEnergyConsumed += energyConsumed
                    totalWatts += consumptionRate
                   
        #print(totalOnTime, totalEnergyConsumed)
        return totalOnTime, totalEnergyConsumed, totalWatts


if __name__ == "__main__":
	energyConsumption = EnergyConsumption()
	energyConsumption.computeResults()


