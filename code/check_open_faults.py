import datetime  
#from sunrise import sun 
import psycopg2
import csv
import sys
import json


class OpenFaultsChecker:

    def __init__(self, inputFilename, outputFilename):
        """ initialize some variables

        """
        self.inputFilename = inputFilename
        self.outputFilename = outputFilename

        self.pg_dbname = "citytouch_barcelona"
        self.pg_username = "awsmaster"
        self.pg_password = "philips2017"
        self.pg_host = "citytouch-buenos-aires-log.cuxwb2nbset5.us-west-2.rds.amazonaws.com"
        self.pg_port = "5432"

        self.oneDayDelta = datetime.timedelta(days=1)

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
        
    def process_input_file(self):
        """ read each dayburner record from the input file 
            and if there are open faults for the day-burner record, write to the output file

        """
        with open(self.inputFilename, "r") as csvFile:
            csvReader = csv.reader(csvFile, delimiter=',') 
            next(csvReader)
            count = 0
            for record in csvReader:
                count += 1
                #print(count)
                asset_id = record[0]
                component_id = record[1]
                current_date = datetime.datetime.strptime(record[10], '%Y-%m-%d').date()       
                num_faults = self.check_open_fault(asset_id, component_id, current_date)
                if num_faults > 0:
                	print(asset_id, component_id, current_date, num_faults)

            print(count)    	

    def check_open_fault(self, asset_id, component_id, current_date):
        """ check if there are open faults during the day-burning date and time 

        """
        next_day = current_date + self.oneDayDelta

        try:
            self.cur.execute("select id, error_key, is_open, first_reported_on, last_reported_on, component_id, is_deleted, asset_id, last_modified_on, closed_on \
                              from faults \
                              where component_id = %s \
                              and first_reported_on < %s and (closed_on > %s or closed_on is null) \
                              order by first_reported_on", (component_id, next_day, current_date))
        except:
            print("I am unable to get data")   

        fault_rows = self.cur.fetchall()  
        
        return len(fault_rows)   

    def run(self):
        """ call this method to run the program

        """    
        self.connect_db()
        self.process_input_file()
        self.disconnect_db()

if __name__ == "__main__":
    inputFilename = sys.argv[1]
    outputFilename = sys.argv[2]
    open_faults_checker = OpenFaultsChecker(inputFilename, outputFilename)
    open_faults_checker.run()




