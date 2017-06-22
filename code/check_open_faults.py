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
        
        results = []
        for row in fault_rows:
        	fault_id = row[0]
        	error_key = row[1]
        	first_reported_on = row[3]
        	closed_on = row[9]
        	results.append((fault_id, error_key, first_reported_on, closed_on))

        #return len(fault_rows)   
        return results

    def process_input_file(self):
        """ read each dayburner record from the input file 
            and if there are open faults for the day-burner record, write to the output file

        """
        results = []
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
                '''       
                num_faults = self.check_open_fault(asset_id, component_id, current_date)
                if num_faults > 0:
                	print(asset_id, component_id, current_date, num_faults)
                '''
                open_faults = self.check_open_fault(asset_id, component_id, current_date)
                for fault_record in open_faults:
                	fault_id = fault_record[0]
                	error_key = fault_record[1]
                	first_reported_on = fault_record[2]
                	closed_on = fault_record[3]
                	results.append((asset_id, component_id, current_date, fault_id, error_key, first_reported_on, closed_on))

            print(count)  

        return results  

    def write_to_file(self, results):
        """ write results to output file

        """
        with open(self.outputFilename, "w") as csvFile:
            csvWriter = csv.writer(csvFile, delimiter=',')   
            title_row = ('asset_id', 'component_id', 'current_date', 'fault_id', 'error_key', 'first_reported_on', 'closed_on')         
            csvWriter.writerow(title_row)
            for record in results:
                csvWriter.writerow(record)     

    def run(self):
        """ call this method to run the program

        """    
        self.connect_db()
        results = self.process_input_file()
        self.write_to_file(results)
        self.disconnect_db()

if __name__ == "__main__":
    inputFilename = sys.argv[1]
    outputFilename = sys.argv[2]
    open_faults_checker = OpenFaultsChecker(inputFilename, outputFilename)
    open_faults_checker.run()




