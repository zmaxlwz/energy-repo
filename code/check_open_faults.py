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
            count = 0
            for record in csvReader:
            	count += 1
        print(count)    	


    def check_open_fault(self):
        """ check if there are open faults during the day-burning date and time 

        """
        pass	

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




