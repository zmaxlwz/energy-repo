import sys
import psycopg2
import csv
import json

class GetHardwareID:
	def __init__(self, outputFilename):
		""" initialize some variables

		"""
		self.outputFilename = outputFilename

		self.pg_dbname = "los_angeles"
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
        
    def get_hardware_id(self):
        """ get hardware id list from communication 

        """        
        try:
            self.cur.execute("select distinct hardware_id \
                              from communications_nodes")
        except:
            print("I am unable to get data")

        rows = self.cur.fetchall() 

        hardware_id_list = []
        for row in rows:
        	hardware_id = row[0]
        	hardware_id_list.append(hardware_id)

        return hardware_id_list
        
    def write_to_file(self, hardware_id_list):
        """ write results to output file

        """
        with open(self.outputFilename, "w") as csvFile:
            csvWriter = csv.writer(csvFile, delimiter=',')   
            for hardware_id in hardware_id_list:
                csvWriter.writerow(hardware_id) 

    def run(self):
        """ run the program

        """               
        self.connect_db()
        hardware_id_list = self.get_hardware_id()
        self.write_to_file(hardware_id_list)
        self.disconnect_db()

if __name__ == "__main__":
	outputFilename = sys.argv[1]
	getHardwareID = GetHardwareID(outputFilename)
	getHardwareID.run()



