
import datetime  
from sunrise import sun 
import psycopg2
import csv
import sys
import json

class TestMeterReading(Object):

    def __init__(self):
    	""" initialization
         
    	"""
    	startDate = "5/1/2016"
    	endDate = "6/1/2016"
    	#period start date
        self.startDate = datetime.datetime.strptime(startDate, '%m/%d/%Y').date()
        #period end date
        self.endDate = datetime.datetime.strptime(endDate, '%m/%d/%Y').date()

        self.pg_dbname = "citytouch_data"
        self.pg_username = "awsmaster"
        self.pg_password = "philips2017"
        self.pg_host = "citytouch-buenos-aires-log.cuxwb2nbset5.us-west-2.rds.amazonaws.com"
        self.pg_port = "5432"
        
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

    def computeResult(self):
        """

        """
        assets_id_list = (60568, 60569, 60570, 73842, 70605, 5377)

        results = []

        first_date_time = datetime.datetime.combine(self.startDate, datetime.time(0, 0, 0))
        last_date_time = datetime.datetime.combine(self.endDate, datetime.time(23, 59, 59))

        print("before query for kwh data")

        query = "select b.asset_id , a.kwh, a.timestamp_utc \
                              from energy_metering_points b, energy_meter_readings a \
                              where b.asset_id in %s and b.id = a.metering_point_id \
                              and a.timestamp_utc >= %s and a.timestamp_utc < %s \
                              order by b.asset_id, a.timestamp_utc"

        try:
        	print(self.cur.mogrify(query, (assets_id_list, first_date_time, last_date_time)))
            self.cur.execute(query, (assets_id_list, first_date_time, last_date_time))
        except:
            print("I am unable to get data")

        print("finished query for kwh data")    

        current_asset_id = None
        count = 0

        rows = self.cur.fetchmany(50000)  
        print("success") 

    def run(self):
    	""" run the program

    	"""
    	self.connectDB()
    	self.computeResult()

if __name__ == "__main__":

    testObj = TestMeterReading()    
    testObj.run()
