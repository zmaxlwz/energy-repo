import sys
import psycopg2
import csv
import json
import datetime
import statistics

class BarcelonaEnergyCheck:
    def __init__(self):
        """ initialize some variables

        """
        self.outputFilename = "output.csv"

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
                        print('{0} {1:6.1f} {2:6.1f} {3:6.1f} {4} {5:6.1f} {6} {7:6.1f}'.format(asset_id, currentDailyEnergyConsumption - lastDailyEnergyConsumption, lastDailyEnergyConsumption, currentDailyEnergyConsumption, lastTime, lastEnergy, currentTime, currentEnergy))
                    lastDailyEnergyConsumption = currentDailyEnergyConsumption
                lastTime = currentTime    
                lastDate = currentDate
                lastEnergy = currentEnergy    

    def print_energy_consumption_for_asset(self, asset_id, start_time, end_time):
        """ print daily energy consumption for the input asset

        """
        try:
            self.cur.execute("select b.asset_id, a.kwh, a.timestamp_utc \
                              from energy_metering_points b, energy_meter_readings a \
                              where b.asset_id = %s and b.id = a.metering_point_id \
                              and a.timestamp_utc >= %s and a.timestamp_utc < %s \
                              order by b.asset_id, a.timestamp_utc", (asset_id, start_time, end_time))
        except:
            print("I am unable to get data")        

        rows = self.cur.fetchall()
        
        print(asset_id)
        energy_rolling_window = []
        lastTime = None
        lastDate = None
        lastEnergy = None
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
                energyConsumption = currentEnergy - lastEnergy
                num_days = (currentDate - lastDate).days
                print(lastTime, lastEnergy, currentTime, currentEnergy)
                print(energyConsumption, num_days)
                dailyEnergyConsumption = energyConsumption / num_days
                energy_rolling_window.append(dailyEnergyConsumption)
                #print('{0} {1:5.1f} {2} {3:5.1f} {4} {5:5.1f}'.format(asset_id, dailyEnergyConsumption, lastTime, lastEnergy, currentTime, currentEnergy))
                lastTime = currentTime    
                lastDate = currentDate
                lastEnergy = currentEnergy   

        print(energy_rolling_window)                  

    def find_dayburners_by_energy_deviation(self, asset_id, start_time, end_time):
        """ detect dayburners by checking the energy consumption deviation from the mean 
            if deviate more than 1 std from mean, then the energy consumption is either too low or too high

        """
        try:
            self.cur.execute("select b.asset_id, a.kwh, a.timestamp_utc \
                              from energy_metering_points b, energy_meter_readings a \
                              where b.asset_id = %s and b.id = a.metering_point_id \
                              and a.timestamp_utc >= %s and a.timestamp_utc < %s \
                              order by b.asset_id, a.timestamp_utc", (asset_id, start_time, end_time))
        except:
            print("I am unable to get data")        

        rows = self.cur.fetchall()

        energy_list = []
        lastTime = None
        lastDate = None
        lastEnergy = None
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
                energyConsumption = currentEnergy - lastEnergy
                num_days = (currentDate - lastDate).days
                energy_list.append(energyConsumption / num_days)
                #update record
                lastTime = currentTime    
                lastDate = currentDate
                lastEnergy = currentEnergy 

        results = []
        if len(energy_list) < 2:
            return results
                
        #compute the mean and std of energy consumption        
        avg_energy_consumption = statistics.mean(energy_list)
        std_energy_consumption = statistics.stdev(energy_list)
        
        lastTime = None
        lastDate = None
        lastEnergy = None
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
                energyConsumption = currentEnergy - lastEnergy
                num_days = (currentDate - lastDate).days
                dailyEnergyConsumption = energyConsumption / num_days
                #num_std = (dailyEnergyConsumption - avg_energy_consumption) / std_energy_consumption
                energy_deviation = dailyEnergyConsumption - avg_energy_consumption
                #if num_std < -1.5 or num_std > 1.5:
                if energy_deviation < -0.2 or energy_deviation > 0.2:
                    #report this abnormal case
                    #print('{0} {1:5.1f} {2: 5.4f} {3} {4:5.1f} {5} {6:5.1f}'.format(asset_id, dailyEnergyConsumption, num_std, lastDate, lastEnergy, currentDate, currentEnergy))
                    #results.append((asset_id, lastDate, dailyEnergyConsumption, avg_energy_consumption, std_energy_consumption, num_std))
                    results.append((asset_id, lastDate, dailyEnergyConsumption, avg_energy_consumption, energy_deviation))
                #update record
                lastTime = currentTime    
                lastDate = currentDate
                lastEnergy = currentEnergy 
        return results   

    def compute_asset_energy_variance(self, asset_tuple, start_time, end_time):
        """ compute the energy consumption variance, stdev, stdev/avg value for the input asset during the period 

        """
        asset_id = asset_tuple[0]
        component_id = asset_tuple[1]
        latitude = asset_tuple[2]
        longitude = asset_tuple[3]
        installation_date = asset_tuple[4]
        commissioning_date = asset_tuple[5]
        street_name = asset_tuple[6]
        cabinet_id = asset_tuple[7]

        try:
            self.cur.execute("select b.asset_id, a.kwh, a.timestamp_utc \
                              from energy_metering_points b, energy_meter_readings a \
                              where b.asset_id = %s and b.id = a.metering_point_id \
                              and a.timestamp_utc >= %s and a.timestamp_utc < %s \
                              order by b.asset_id, a.timestamp_utc", (asset_id, start_time, end_time))
        except:
            print("I am unable to get data")        

        rows = self.cur.fetchall()

        energy_rolling_window = []
        results = []
        lastTime = None
        lastDate = None
        lastEnergy = None
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
                energyConsumption = currentEnergy - lastEnergy
                num_days = (currentDate - lastDate).days
                dailyEnergyConsumption = energyConsumption / num_days
                energy_rolling_window.append(dailyEnergyConsumption)
                #update record
                lastTime = currentTime    
                lastDate = currentDate
                lastEnergy = currentEnergy 

        if len(energy_rolling_window) < 3:
            return results        

        #compute the mean and std of energy consumption        
        avg_energy_consumption = statistics.mean(energy_rolling_window)
        std_energy_consumption = statistics.stdev(energy_rolling_window) 
        variance_energy_consumption = statistics.variance(energy_rolling_window)
        normalized_stdev_energy_consumption = std_energy_consumption / avg_energy_consumption

        results.append((asset_id, component_id, latitude, longitude, installation_date, commissioning_date, street_name, cabinet_id, avg_energy_consumption, std_energy_consumption, normalized_stdev_energy_consumption, variance_energy_consumption))       
    
        return results

    def find_dayburners_energy_deviation_rolling_window_avg(self, asset_tuple, start_time, end_time):
        """ find dayburners by calculating energy deviation from 30 day rolling window average 
            if # of stdev > 1.5 and deviation > 0.2 kwh, report that record
        """

        asset_id = asset_tuple[0]
        component_id = asset_tuple[1]
        latitude = asset_tuple[2]
        longitude = asset_tuple[3]
        installation_date = asset_tuple[4]
        commissioning_date = asset_tuple[5]
        street_name = asset_tuple[6]
        cabinet_id = asset_tuple[7]

        try:
            self.cur.execute("select b.asset_id, a.kwh, a.timestamp_utc \
                              from energy_metering_points b, energy_meter_readings a \
                              where b.asset_id = %s and b.id = a.metering_point_id \
                              and a.timestamp_utc >= %s and a.timestamp_utc < %s \
                              order by b.asset_id, a.timestamp_utc", (asset_id, start_time, end_time))
        except:
            print("I am unable to get data")        

        rows = self.cur.fetchall()

        energy_rolling_window = []
        results = []
        lastTime = None
        lastDate = None
        lastEnergy = None
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
                energyConsumption = currentEnergy - lastEnergy
                num_days = (currentDate - lastDate).days
                dailyEnergyConsumption = energyConsumption / num_days
                #print(lastTime, dailyEnergyConsumption)
                if len(energy_rolling_window) == 30:
                    # the rolling window is full
                    # compute the mean and std of energy consumption within the rolling window       
                    avg_energy_consumption = statistics.mean(energy_rolling_window)
                    std_energy_consumption = statistics.stdev(energy_rolling_window)
                    if std_energy_consumption == 0:
                        if dailyEnergyConsumption - avg_energy_consumption > 0.2:
                            results.append((asset_id, component_id, latitude, longitude, installation_date, commissioning_date, street_name, cabinet_id, lastDate, dailyEnergyConsumption, avg_energy_consumption, std_energy_consumption, None))
                    else:    
                        num_std = (dailyEnergyConsumption - avg_energy_consumption) / std_energy_consumption
                        #if num_std < -1.5 or num_std > 1.5:
                        if num_std > 1.5 and dailyEnergyConsumption - avg_energy_consumption > 0.2:
                            results.append((asset_id, component_id, latitude, longitude, installation_date, commissioning_date, street_name, cabinet_id, lastDate, dailyEnergyConsumption, avg_energy_consumption, std_energy_consumption, num_std))
                    '''
                    energy_deviation = dailyEnergyConsumption - avg_energy_consumption
                    #if energy_deviation < -0.2 or energy_deviation > 0.2:
                    if energy_deviation > 0.2:    
                        #report this abnormal case
                        #print('{0} {1:5.1f} {2: 5.4f} {3} {4:5.1f} {5} {6:5.1f}'.format(asset_id, dailyEnergyConsumption, num_std, lastDate, lastEnergy, currentDate, currentEnergy))
                        
                        results.append((asset_id, lastDate, dailyEnergyConsumption, avg_energy_consumption, energy_deviation))
                    '''
                    #update rolling window    
                    energy_rolling_window.pop(0)
                    energy_rolling_window.append(dailyEnergyConsumption)    
                else:
                    # the rolling window is not full, just append new element to the end of the rolling window
                    energy_rolling_window.append(dailyEnergyConsumption)    
                #update record
                lastTime = currentTime    
                lastDate = currentDate
                lastEnergy = currentEnergy 
        return results 


    def get_assets_list(self):
        """ get assets list from assets table, which are not deleted and installation_date and commissioning_date are not null

        """                
        try:            
            self.cur.execute("select id, latitude, longitude, installation_date, commissioning_date \
                              from assets \
                              where is_deleted = 'f' \
                              and installation_date is not null and commissioning_date is not null")
            '''
            self.cur.execute("select c.id, c.asset_id, a.latitude, a.longitude, a.installation_date, a.commissioning_date \
                              from assets as a, components as c \
                              where a.is_deleted = 'f' and a.installation_date is not null and a.commissioning_date is not null \
                              and a.id = c.asset_id and c.component_kind = 0")        
            '''                      
        except:
            print("I am unable to get data")

        rows = self.cur.fetchall()  

        results = []
        for row in rows:
            asset_id = row[0]
            latitude = row[1]
            longitude = row[2]
            installation_date = row[3]
            commissioning_date = row[4]

            try:
                self.cur.execute("select id \
                                  from components \
                                  where asset_id = %s \
                                  and is_deleted = 'f' and component_kind = 0", (asset_id, ))
            except:
                print("I am unable to get data")

            component_id_data = self.cur.fetchall() 
            component_id = component_id_data[0][0]    

            try:
                self.cur.execute("select s.name as street_name \
                                  from assets as a, streets as s \
                                  where a.id = %s \
                                  and a.street_id = s.id", (asset_id, ))
            except:
                print("I am unable to get data")

            street_name_data = self.cur.fetchall() 
            street_name = street_name_data[0][0]

            try:
                self.cur.execute("select parent_id \
                                  from components A,communications_nodes B \
                                  where A.id=B.id and asset_id = %s", (asset_id, ))
            except:
                print("I am unable to get data")

            cabinet_id_data = self.cur.fetchall()     
            cabinet_id = cabinet_id_data[0][0]

            results.append((asset_id, component_id, latitude, longitude, installation_date, commissioning_date, street_name, cabinet_id))

        return results              

    def write_to_file(self, results):
        """ write results to output file

        """
        with open(self.outputFilename, "w") as csvFile:
            csvWriter = csv.writer(csvFile, delimiter=',')   
            #title_row = ('asset_id', 'component_id', 'latitude', 'longitude', 'installation_date', 'commissioning_date', 'street_name', 'cabinet_id', 'current_date', 'dailyEnergyConsumption', 'avg_energy_consumption', 'std_energy_consumption', 'num_of_std')  
            #title_row = ('asset_id', 'current_date', 'dailyEnergyConsumption', 'avg_energy_consumption', 'energy_deviation')       
            title_row = ('asset_id', 'component_id', 'latitude', 'longitude', 'installation_date', 'commissioning_date', 'street_name', 'cabinet_id', 'avg_energy_consumption', 'std_energy_consumption', 'normalized_stdev_energy_consumption', 'variance_energy_consumption') 
            csvWriter.writerow(title_row)
            for record in results:
                csvWriter.writerow(record)

    def run(self):
        """ run the program

        """               
        self.connect_db()
        #asset_id_list = [2063, 2, 3, 10, 11]
        #asset_id_list = [2063]
        #asset_id_list = [2100, 2102, 2103, 2110, 2111, 2112]
        asset_id_list = [583]
        #asset_tuple_list = self.get_assets_list()        

        #start_time = datetime.datetime(2016, 7, 1, 0, 0, 0)
        #end_time = datetime.datetime(2017, 4, 30, 0, 0, 0)
        #start_time = datetime.datetime(2016, 8, 1, 0, 0, 0)
        #end_time = datetime.datetime(2016, 10, 1, 0, 0, 0)    
        start_time = datetime.datetime(2017, 4, 19, 0, 0, 0)
        end_time = datetime.datetime(2017, 5, 20, 0, 0, 0)
        
        for asset_id in asset_id_list:
            self.print_energy_consumption_for_asset(asset_id, start_time, end_time)

        '''        
        print("total assets: ", len(asset_tuple_list))
        results = []
        count = 0
        for asset_tuple in asset_tuple_list:
            count += 1
            print(count)
            #self.check_energy_for_asset(asset_id)
            #self.print_energy_consumption_for_asset(asset_id, start_time, end_time)
            #results += self.find_dayburners_by_energy_deviation(asset_id, start_time, end_time)
            #results += self.find_dayburners_energy_deviation_rolling_window_avg(asset_tuple, start_time, end_time)
            results += self.compute_asset_energy_variance(asset_tuple, start_time, end_time)
        self.write_to_file(results)
        '''

        self.disconnect_db()

if __name__ == "__main__":
    energyCheck = BarcelonaEnergyCheck()        
    energyCheck.run()


