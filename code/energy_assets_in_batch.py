import datetime  
#from sunrise import sun 
import psycopg2
import csv
import sys
import json

class EnergyConsumption:

    def __init__(self, configJSONFilename):
        """ initialize variables

        """
        #configuration file name
        self.configFilename = configJSONFilename
        # 1 day delta
        self.oneDayDelta = datetime.timedelta(days=1)
        # 7 hours delta, LA in July is -7 from UTC
        self.sevenHoursDelta = datetime.timedelta(hours=7)
        # 8 hours delta, LA in Jan is -8 from UTC
        self.eightHoursDelta = datetime.timedelta(hours=8)
           
        #sunrise dict
        self.sunriseTimeDict = {}
        #sunset dict
        self.sunsetTimeDict = {}
        
        #average sunrise time in Jakarta in UTC
        #self.sunrise_time_avg_utc = datetime.time(22, 55, 8) 
        #self.sunrise_time_avg_utc = datetime.time(23, 0, 0)
        #average sunrise time in CABA in UTC in 2016-5
        #self.sunrise_time_avg_utc = datetime.time(10, 40, 0)
        #self.sunrise_time_avg_local = datetime.time(7, 40, 0)
        #average sunrise time in LA in UTC in 2017-1
        #self.sunrise_time_avg_utc = datetime.time(15, 0, 0)  
        #average sunrise time in LA in local time in 2017-1
        #self.sunrise_time_avg_local = datetime.time(7, 0, 0)  
              
        #average sunset time in Jakarta in UTC
        #self.sunset_time_avg_utc = datetime.time(11, 4, 16)
        #self.sunset_time_avg_utc = datetime.time(11, 0, 0)
        #average sunset time in CABA in UTC in 2016-5
        #self.sunset_time_avg_utc = datetime.time(21, 0, 0)
        #self.sunset_time_avg_local = datetime.time(18, 0, 0)
        #average sunset time in LA in UTC in 2017-1
        #self.sunset_time_avg_utc = datetime.time(1, 0, 0)
        #average sunset time in LA in local time in 2017-1
        #self.sunset_time_avg_local = datetime.time(17, 0, 0)
        
        #assets latitude dict
        self.assets_latitude_dict = {}
        #assets longitude dict
        self.assets_longitude_dict = {}
        #assets installation date dict
        self.assets_installation_date_dict = {}
        #assets commissioning date dict
        self.assets_commissioning_date_dict = {}
        #assets nominal wattage dict
        self.assets_nominal_wattage_dict = {}
        #assets luminaire type
        self.assets_luminaire_type_dict = {}
        #assets street name dict
        self.assets_street_name_dict = {}

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

    def get_config(self, configFilename):
        """ get configuration parameters

        """    
        with open(configFilename) as config_file:    
            config_data = json.load(config_file)
                    
            self.pg_dbname = config_data['pg_dbname']
            self.pg_username = config_data['pg_username']
            self.pg_password = config_data['pg_password']
            self.pg_host = config_data['pg_host']
            self.pg_port = config_data['pg_port']
            #output filename
            self.outputFilename = config_data['output_csvfile_name']
            #define energyThreshold: 2 watt
            self.energyThreshold = int(config_data['energy_threshold'])  
            #define onTime threshold for a day: in minutes
            self.onTimeThreshold = int(config_data['onTime_threshold'])  
            #define nominal wattage ratio
            self.nominal_wattage_ratio = float(config_data['nominal_wattage_ratio'])
            #period start date
            self.startDate = datetime.datetime.strptime(config_data['period_start_date'], '%m/%d/%Y').date()
            #period end date
            self.endDate = datetime.datetime.strptime(config_data['period_end_date'], '%m/%d/%Y').date()
            #daytime start time
            self.daytime_start_time_local = datetime.datetime.strptime(config_data['daytime_start_time_local'], '%H:%M:%S').time()
            #daytime end time
            self.daytime_end_time_local = datetime.datetime.strptime(config_data['daytime_end_time_local'], '%H:%M:%S').time()    
            #sunrise avg local time
            self.sunrise_time_avg_local = datetime.datetime.strptime(config_data['sunrise_time_avg_local'], '%H:%M:%S').time()   
            #sunset avg local time
            self.sunset_time_avg_local = datetime.datetime.strptime(config_data['sunset_time_avg_local'], '%H:%M:%S').time()       
            #commissioning date plus the number of days
            self.commissioningDatePlusDays = int(config_data['commissioning_date_plus_days'])
            #sunrise adjusted time delta - sunrise time should add this delta
            self.sunriseTimeDelta = datetime.timedelta(hours=int(config_data['sunrise_time_delta_hours']), minutes=int(config_data['sunrise_time_delta_minutes']), seconds=int(config_data['sunrise_time_delta_seconds']))
            #sunset adjusted time delta - sunset time should minus this delta
            self.sunsetTimeDelta = datetime.timedelta(hours=int(config_data['sunset_time_delta_hours']), minutes=int(config_data['sunset_time_delta_minutes']), seconds=int(config_data['sunset_time_delta_seconds']))
            #suntime location latitude
            self.suntime_latitude = float(config_data['suntime_location_latitude'])
            #suntime location longitude
            self.suntime_longitude = float(config_data['suntime_location_longitude'])

            # hours from UTC time for the local time
            hours_from_utc = int(config_data['hours_from_utc'])
            # UTC time plus this time to get local time
            self.local_time_hours_from_utc = datetime.timedelta(hours=hours_from_utc)

    def run(self):
        """  call this method to run the program

        """
        #step 1:  read from json config file, get db connect parameter, time period to check, output file name
        self.get_config(self.configFilename)
        #step 2:  connect to db
        self.connect_db()
        #step 3:  compute sunrise and sunset time 
        #self.computeSunTime(self.suntime_latitude, self.suntime_longitude, self.startDate, self.endDate)
        #step 4:  get assets list
        #assets = self.getAssetsList()
        #assets = [(3776, -6.118187, 106.894265, datetime.date(2016, 5, 27), datetime.date(2016, 6, 1))]
        #assets = [(3776, -6.118187, 106.894265), (13532, -6.102635, 106.932242)]
        assets_id_list = self.getAssetsList()
        #step 5:  call computeResults method
        #self.computeResults(assets)
        self.get_assets_info()
        print("got assets info")
        self.get_nominal_wattage()
        print("got nominal wattage info")

        start_index = 0
        step = 1000
        end_index = start_index + step
        assets_id_sublist = assets_id_list[start_index:end_index]
        results = []
        count = 0
        while assets_id_sublist:
            count += 1
            print(count)
            results += self.compute_energy_consumption(assets_id_sublist)
            start_index += step
            end_index += step
            assets_id_sublist = assets_id_list[start_index:end_index]

        print("finished computing")

        self.write_to_file(results)
        print("finished output to files")

    def getAssetsList(self):
        """ get assets list from assets table, which are not deleted and installation_date and commissioning_date are not null

        """                
        try:
            
            self.cur.execute("select id \
                              from assets \
                              where is_deleted = 'f' \
                              and installation_date is not null and commissioning_date is not null")
            '''
            #for LA
            self.cur.execute("select id \
                              from assets \
                              where is_deleted = 'f' \
                              and commissioning_date is not null")
            '''         
        except:
            print("I am unable to get data")

        rows = self.cur.fetchall()  

        assets_id_list = []
        for row in rows:
            asset_id = row[0]
            assets_id_list.append(asset_id)

        return assets_id_list       

    def get_assets_info(self):
        """ get assets information from database, like latitude, longitude, installation_date, commissioning_date 

        """
        try:
            
            self.cur.execute("select a.id, a.latitude, a.longitude, a.installation_date, a.commissioning_date, s.name as street_name \
                              from assets as a, streets as s \
                              where a.is_deleted = 'f' \
                              and a.installation_date is not null and a.commissioning_date is not null \
                              and a.street_id = s.id")
            '''
            #for LA
            self.cur.execute("select a.id, a.latitude, a.longitude, a.installation_date, a.commissioning_date, s.route as street_name \
                              from assets as a, streets_reverse_geocoded as s \
                              where a.is_deleted = 'f' \
                              and a.commissioning_date is not null \
                              and a.id = s.asset_id")   
            '''                                 
        except:
            print("I am unable to get data")

        rows = self.cur.fetchall() 

        for row in rows:
            asset_id = row[0]
            latitude = row[1]
            longitude = row[2]
            installation_date = row[3]
            commissioning_date = row[4]
            street_name = row[5]
            self.assets_latitude_dict[asset_id] = latitude
            self.assets_longitude_dict[asset_id] = longitude
            self.assets_installation_date_dict[asset_id] = installation_date
            self.assets_commissioning_date_dict[asset_id] = commissioning_date
            self.assets_street_name_dict[asset_id] = street_name

    def get_nominal_wattage(self):
        """ get assets nominal wattage from database

        """        
        try:
            
            self.cur.execute("select a.id, lt.type_designation, lt.nominal_wattage, lt.actual_wattage \
                              from luminaire_types lt, luminaires l, components c, assets a \
                              where a.is_deleted = 'f' and a.installation_date is not null and a.commissioning_date is not null \
                              and a.id = c.asset_id and c.id = l.id and l.luminaire_type_id = lt.id \
                              order by a.id")
            '''
            #for LA, nominal_wattage is null, use actual wattage, 
            self.cur.execute("select a.id, lt.type_designation, lt.actual_wattage \
                              from luminaire_types lt, luminaires l, components c, assets a \
                              where a.is_deleted = 'f' and a.commissioning_date is not null \
                              and a.id = c.asset_id and c.id = l.id and l.luminaire_type_id = lt.id \
                              order by a.id")                  
            '''
        except:
            print("I am unable to get data")

        rows = self.cur.fetchall() 

        for row in rows:
            asset_id = row[0]
            luminaire_type = row[1]
            nominal_wattage = row[2]            
            self.assets_luminaire_type_dict[asset_id] = luminaire_type
            self.assets_nominal_wattage_dict[asset_id] = nominal_wattage

    def compute_energy_consumption(self, assets_id_list):
        """ compute the energy consumption

        """
        results = []
        first_date_time = datetime.datetime.combine(self.startDate, datetime.time(0, 0, 0))
        last_date_time = datetime.datetime.combine(self.endDate, datetime.time(23, 59, 59))

        assets_id_tuple = tuple(assets_id_list)

        #print("before query for kwh data")

        try:
            #there might be multiple lights in an assets, so need to order by asset_id and component_id
            self.cur.execute("select b.asset_id , b.meter_component_id, a.kwh, a.timestamp_utc \
                              from energy_metering_points b, energy_meter_readings a \
                              where b.asset_id in %s and b.id = a.metering_point_id \
                              and a.timestamp_utc >= %s and a.timestamp_utc < %s \
                              order by b.asset_id, b.meter_component_id, a.timestamp_utc", (assets_id_tuple, first_date_time, last_date_time))
        except:
            print("I am unable to get data")

        #print("finished query for kwh data")    

        current_asset_id = None
        current_meter_component_id = None
        count = 0

        rows = self.cur.fetchmany(50000)
        while rows:
            #process each record in order 
            for row in rows:   
                row_asset_id = row[0]
                row_meter_component_id = row[1]
                row_energy = row[2]
                # convert time from UTC to local time
                #row_time = row[3] - self.eightHoursDelta
                row_time = row[3] + self.local_time_hours_from_utc
                if current_asset_id is None or current_asset_id != row_asset_id or current_meter_component_id != row_meter_component_id:
                    #it is a new asset
                    if current_asset_id is not None:
                        if totalOnTime == 0:
                            totalWatts = 0
                        else:
                            totalWatts = (totalEnergyConsumed * 1000) / (totalOnTime / 60.0)  
                        if totalOnTime > self.onTimeThreshold:
                            current_date = next_date_day_end_time.date()
                            results.append((current_asset_region_name, current_date, current_asset_id, current_meter_component_id, current_asset_luminaire_type, current_asset_latitude, current_asset_longitude, current_asset_installation_date, current_asset_commissioning_date, current_asset_nominal_wattage, current_asset_street_name, totalOnTime, first_time_stamp_after_sunrise, last_time_stamp_before_sunset, totalEnergyConsumed, totalWatts, num_interval, num_interval_positive))
                
                    current_asset_id = row_asset_id 
                    current_meter_component_id = row_meter_component_id
                    count += 1
                    #print(count)
                    current_asset_latitude = self.assets_latitude_dict[current_asset_id]
                    current_asset_longitude = self.assets_longitude_dict[current_asset_id]
                    current_asset_installation_date = self.assets_installation_date_dict[current_asset_id]
                    current_asset_commissioning_date = self.assets_commissioning_date_dict[current_asset_id]
                    current_asset_valid_start_date = current_asset_commissioning_date + datetime.timedelta(days=self.commissioningDatePlusDays) 
                    current_asset_nominal_wattage = self.assets_nominal_wattage_dict[current_asset_id]
                    current_asset_street_name = self.assets_street_name_dict[current_asset_id]
                    current_asset_region_name = self.pg_dbname
                    current_asset_luminaire_type = self.assets_luminaire_type_dict[current_asset_id]
                    '''
                    current_date = row_time.date()
                    current_date_day_start_time = datetime.datetime.combine(current_date, self.daytime_start_time)
                    current_date_day_end_time = datetime.datetime.combine(current_date, self.daytime_end_time)  
                    '''
                    next_date_sunrise_time = datetime.datetime.combine(row_time.date() + self.oneDayDelta, self.sunrise_time_avg_local)
                    #next_date_sunrise_time = datetime.datetime.combine(row_time.date(), self.sunrise_time_avg_utc)
                    next_date_day_start_time = datetime.datetime.combine(row_time.date() + self.oneDayDelta, self.daytime_start_time_local)
                    #next_date_day_start_time = datetime.datetime.combine(row_time.date(), self.daytime_start_time)
                    next_date_day_end_time = datetime.datetime.combine(row_time.date() + self.oneDayDelta, self.daytime_end_time_local)
                    next_date_sunset_time = datetime.datetime.combine(row_time.date() + self.oneDayDelta, self.sunset_time_avg_local)
                    #next_date_sunset_time = datetime.datetime.combine(row_time.date() + self.oneDayDelta + self.oneDayDelta, self.sunset_time_avg_utc)
                    totalOnTime, totalEnergyConsumed, totalWatts = 0, 0, 0
                    num_interval, num_interval_positive = 0, 0
                    lastEnergy, lastTime = None, None
                    #first_time_stamp_in_daytime, last_time_stamp_in_daytime = None, None
                    first_time_stamp_after_sunrise, last_time_stamp_before_sunset = None, None

                if row_time.date() < current_asset_valid_start_date:    
                    #this date is before valid start date for this asset (commissioning_date + 3 days for instance)
                    continue
                if row_time < next_date_sunrise_time:
                    continue    
                if first_time_stamp_after_sunrise is None:
                    first_time_stamp_after_sunrise = row_time
                if row_time >= next_date_day_start_time and row_time <= next_date_day_end_time:    
                    if lastTime is None:
                        lastEnergy = row_energy
                        lastTime = row_time
                        #first_time_stamp_in_daytime = row_time
                        #last_time_stamp_in_daytime = row_time
                    else:
                        currentEnergy = row_energy
                        currentTime = row_time
                        #last_time_stamp_in_daytime = row_time
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
                        if consumptionRate > current_asset_nominal_wattage * self.nominal_wattage_ratio + self.energyThreshold and consumptionRate < 5 * current_asset_nominal_wattage:
                            # the wattage in this interval is > 30% of nominal wattage and < 5 times of nominal wattage
                            # if < 30% of nominal wattage, it is not significant, may not be a day-burner
                            # if > 5 times of nominal wattage, this may be due to data error, not day-burner
                            totalOnTime += secondsInterval / 60.0
                            totalEnergyConsumed += energyConsumed
                            num_interval_positive += 1
                                    
                if row_time <= next_date_sunset_time:
                    last_time_stamp_before_sunset = row_time
                    continue

                #after next_date_sunset_time, output this date and set for next date                
                if totalOnTime == 0:
                    totalWatts = 0
                else:
                    totalWatts = (totalEnergyConsumed * 1000) / (totalOnTime / 60.0)  
                if totalOnTime > self.onTimeThreshold:
                    current_date = next_date_day_end_time.date()
                    results.append((current_asset_region_name, current_date, current_asset_id, current_meter_component_id, current_asset_luminaire_type, current_asset_latitude, current_asset_longitude, current_asset_installation_date, current_asset_commissioning_date, current_asset_nominal_wattage, current_asset_street_name, totalOnTime, first_time_stamp_after_sunrise, last_time_stamp_before_sunset, totalEnergyConsumed, totalWatts, num_interval, num_interval_positive))
                
                next_date_sunrise_time = datetime.datetime.combine(row_time.date() + self.oneDayDelta, self.sunrise_time_avg_local)
                #next_date_sunrise_time = datetime.datetime.combine(row_time.date(), self.sunrise_time_avg_utc)
                next_date_day_start_time = datetime.datetime.combine(row_time.date() + self.oneDayDelta, self.daytime_start_time_local)
                #next_date_day_start_time = datetime.datetime.combine(row_time.date(), self.daytime_start_time)
                next_date_day_end_time = datetime.datetime.combine(row_time.date() + self.oneDayDelta, self.daytime_end_time_local)
                next_date_sunset_time = datetime.datetime.combine(row_time.date() + self.oneDayDelta, self.sunset_time_avg_local)
                #next_date_sunset_time = datetime.datetime.combine(row_time.date() + self.oneDayDelta + self.oneDayDelta, self.sunset_time_avg_utc)
                totalOnTime, totalEnergyConsumed, totalWatts = 0, 0, 0
                num_interval, num_interval_positive = 0, 0
                lastEnergy, lastTime = None, None
                #first_time_stamp_in_daytime, last_time_stamp_in_daytime = None, None
                first_time_stamp_after_sunrise, last_time_stamp_before_sunset = None, None

            rows = self.cur.fetchmany(50000)   

        if current_asset_id is None:
            #no data             
            return results

        if totalOnTime == 0:
            totalWatts = 0
        else:
            totalWatts = (totalEnergyConsumed * 1000) / (totalOnTime / 60.0)  
        if totalOnTime > self.onTimeThreshold:
            current_date = next_date_day_end_time.date()
            results.append((current_asset_region_name, current_date, current_asset_id, current_meter_component_id, current_asset_luminaire_type, current_asset_latitude, current_asset_longitude, current_asset_installation_date, current_asset_commissioning_date, current_asset_nominal_wattage, current_asset_street_name, totalOnTime, first_time_stamp_after_sunrise, last_time_stamp_before_sunset, totalEnergyConsumed, totalWatts, num_interval, num_interval_positive))
                
        return results          

    def write_to_file(self, results):
        """ write results to output file

        """
        with open(self.outputFilename, "w") as csvFile:
            csvWriter = csv.writer(csvFile, delimiter=',')   
            title_row = ('region', 'date', 'asset_id', 'meter_component_id', 'luminaire_type', 'latitude', 'longitude', 'installation_date', 'commissioning_date', 'nominal_wattage', 'street_name', 'timespan', 'first_timestamp', 'last_timestamp', 'energyConsumedKwh', 'energyConsumedWatts', 'numIntervals', 'numPositiveIntervals')         
            csvWriter.writerow(title_row)
            for record in results:
                csvWriter.writerow(record)

if __name__ == "__main__":

    configJSONFilename = sys.argv[1]
    energyConsumption = EnergyConsumption(configJSONFilename)    
    energyConsumption.run()
    
    