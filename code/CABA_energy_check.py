import sys
import psycopg2
import csv
import json
import datetime
import statistics

class CABAEnergyCheck:

    def __init__(self):
        """ initialize some variables

        """
        #self.outputFilename = "output.csv"

        self.pg_dbname = "citytouch_temp"
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
        
    def check_energy_for_asset(self, asset_id):
        """ check the energy consumption for each day in Barcelona, 
            report if the difference between two consecutive days is greater than 0.1 kwh

        """
        try:
            self.cur.execute("select b.asset_id , b.meter_component_id, a.kwh, a.timestamp_utc \
							from energy_metering_points b, energy_meter_readings a \
							where b.asset_id = %s and b.id = a.metering_point_id \
							and a.timestamp_utc >= '2016-12-31' and a.timestamp_utc < '2017-6-11' \
							order by b.asset_id, b.meter_component_id, a.timestamp_utc", (asset_id, ))
        except:
            print("I am unable to get data")        

        rows = self.cur.fetchall() 
        
        lastTime = None
        lastEnergy = None
        for row in rows:
            if lastTime is None:
                lastTime = row[3]
                lastEnergy = row[2]
            else:
                currentTime = row[3]
                currentEnergy = row[2]
                EnergyConsumption = currentEnergy - lastEnergy
                if EnergyConsumption > 1000 or EnergyConsumption < -1000:
                    print('{0} {1} {2} {3} {4} {5}'.format(asset_id, EnergyConsumption, lastTime, lastEnergy, currentTime, currentEnergy))
                
                lastTime = currentTime    
                lastEnergy = currentEnergy   

    def get_assets_list(self):
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

        asset_id_list = []
        for row in rows:
            asset_id = row[0]
            asset_id_list.append(asset_id)

    def run(self):
        self.connect_db()
        assets_id_list = self.get_assets_list()
        for asset_id in assets_id_list:
        	self.check_energy_for_asset(asset_id)

        self.disconnect_db()        

if __name__ == "__main__":
    energyCheck = CABAEnergyCheck()        
    energyCheck.run()

