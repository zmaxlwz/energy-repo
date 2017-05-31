import sys
import psycopg2
import csv
import json

class BarcelonaEnergyCheck:
    def __init__(self):
        """ initialize some variables

        """
        #self.outputFilename = outputFilename

        self.pg_dbname = "citytouch_barcelona"
        self.pg_username = "awsmaster"
        self.pg_password = "philips2017"
        self.pg_host = "citytouch-buenos-aires-log.cuxwb2nbset5.us-west-2.rds.amazonaws.com"
        self.pg_port = "5432"

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

    def disconnect_db(self):
        """ close db connection

        """    
        self.cur.close()
        self.conn.close()

    def get_asset_id_list(self):
        """ get asset id list

        """     
        try:
            self.cur.execute("select id \
                              from assets \
                              where is_deleted = 'f' \
                              and installation_date is not null and commissioning_date is not null")
        except:
            print("I am unable to get data")

        rows = self.cur.fetchall() 

        asset_id_list = []
        for row in rows:
            asset_id = row[0]
            asset_id_list.append(asset_id) 

        return asset_id_list
        
    def check_energy_for_asset(self, asset_id):
        """ check the energy consumption for each day in Barcelona, 
            report if the difference between two consecutive days is greater than 0.1 kwh

        """
        try:
            self.cur.execute("select b.asset_id, a.kwh, a.timestamp_utc \
                              from energy_metering_points b, energy_meter_readings a \
                              where b.asset_id = %s and b.id = a.metering_point_id \
                              order by b.asset_id, a.timestamp_utc", (asset_id, ))
        except:
            print("I am unable to get data")        

        rows = self.cur.fetchall() 
        
        lastTime = None
        lastDate = None
        lastEnergy = None
        lastDailyEnergyConsumption = None
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
                if lastDailyEnergyConsumption is None:
                    lastDailyEnergyConsumption = currentEnergy - lastEnergy
                else:
                    currentDailyEnergyConsumption = currentEnergy - lastEnergy
                    if currentDailyEnergyConsumption - lastDailyEnergyConsumption > 0.1:
                        print('{0} {1:.4f} {2:.4f} {3:.4f} {4} {5:3d.1f} {6} {7:3d.1f}'.format(asset_id, currentDailyEnergyConsumption - lastDailyEnergyConsumption, lastDailyEnergyConsumption, currentDailyEnergyConsumption, lastTime, lastEnergy, currentTime, currentEnergy))
                    lastDailyEnergyConsumption = currentDailyEnergyConsumption
                lastTime = currentTime    
                lastDate = currentDate
                lastEnergy = currentEnergy                    

    def run(self):
        """ run the program

        """               
        self.connect_db()
        #asset_id_list = self.get_asset_id_list()
        #asset_id_list = [2063, 2, 3, 10, 11]
        asset_id_list = [2063]
        for asset_id in asset_id_list:
            self.check_energy_for_asset(asset_id)
        self.disconnect_db()

if __name__ == "__main__":
    energyCheck = BarcelonaEnergyCheck()        
    energyCheck.run()


