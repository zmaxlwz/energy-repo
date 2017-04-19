import datetime  
from sunrise import sun 
#from pytz import timezone
import pytz


class TestSun:

    def __init__(self, timezoneStr, latitude, longitude):
        """ initialize timezone info
        
        Args:

         timezoneStr: a string which is the input parameter to pytz.timezone()
                      example:  'US/Eastern'
         latitude:  the position latitude
         longitude: the position longitude             

        """

        #self.timezone = pytz.timezone('US/Eastern')
        self.timezone = pytz.timezone(timezoneStr)
        self.utc = pytz.utc

        self.fmt = "%Y-%m-%d %H:%M:%S %Z%z"
        self.latitude = latitude
        self.longitude = longitude
        self.sun = sun(lat=self.latitude,long=self.longitude)

    def getInputDatetime(self, year, month, day, hour, minute, second):
        """  from input date and time, build datetime object, the input time is in UTC

        """    

        #time = datetime.datetime(2017, 4, 11, 12, 0, 0, tzinfo=utc)
        self.time = datetime.datetime(year, month, day, hour, minute, second)
        self.dayStartTime = datetime.datetime(year, month, day)

    def computeSunriseTime(self):
        """  compute the sunrise time of the position

        """    
        #sunrise_time = self.sun.sunrise(when=self.time)
        #sunrise_datetime = datetime.datetime.combine(self.time.date(), sunrise_time)
        (h, m, s) = self.sun.sunrise(when=self.time)
        time_delta = datetime.timedelta(hours=h, minutes=m, seconds=s)
        sunrise_datetime = self.dayStartTime + time_delta
        #the sunrise_datetime is in UTC
        print(sunrise_datetime)

        #convert UTC time to local time 
        utc_dt = self.utc.localize(sunrise_datetime)
        loc_dt = utc_dt.astimezone(self.timezone)
        print(loc_dt.strftime(self.fmt))


    def computeSunsetTime(self):
        """  compute the sunset time of the position

        """  
        #sunset_time = self.sun.sunset(when=self.time)
        #sunset_datetime = datetime.datetime.combine(self.time.date(), sunset_time)
        (h, m, s) = self.sun.sunset(when=self.time)
        time_delta = datetime.timedelta(hours=h, minutes=m, seconds=s)
        sunset_datetime = self.dayStartTime + time_delta
        #the sunset_datetime is in UTC
        print(sunset_datetime)
        
        #convert UTC time to local time
        utc_dt = self.utc.localize(sunset_datetime)
        loc_dt = utc_dt.astimezone(self.timezone)
        print(loc_dt.strftime(self.fmt))  



#print(time.strftime(fmt))
#print(time)

'''
#print('sunrise at ',s.sunrise(when=time))
#print('sunset at ', s.sunset(when=time))
sunrise_time = s.sunrise(when=time)
sunrise_datetime = datetime.datetime.combine(time.date(), sunrise_time)
print(sunrise_datetime)

utc_dt = utc.localize(sunrise_datetime)
loc_dt = utc_dt.astimezone(eastern)
print(loc_dt.strftime(fmt))


sunset_time = s.sunset(when=time)
sunset_datetime = datetime.datetime.combine(time.date(), sunset_time)
print(sunset_datetime)

utc_dt = utc.localize(sunset_datetime)
loc_dt = utc_dt.astimezone(eastern)
print(loc_dt.strftime(fmt))
'''

if __name__ == "__main__":
    '''
    #349 Pleasant St, Malden, MA    lat: 42.4271259, long: -71.0767166
    #04/11/2017  sunrise: 6:09 am  sunset: 7:22 pm    
    timezone = 'US/Eastern'
    latitude = 42.4271259
    longitude = -71.0767166
    year, month, day, hour, minute, second = 2017, 4, 13, 14, 0, 0
    '''
    '''
    #6249 Marguerite Drive, Neward, CA    'lat': 37.5264282, 'lng': -122.0147992
    timezone = 'US/Pacific'
    latitude = 37.5264282
    longitude = -122.0147992
    year, month, day, hour, minute, second = 2017, 2, 7, 12, 0, 0
    '''
    #Jakarta, Utara
    timezone = 'US/Pacific'
    #latitude = -6.346005
    latitude = -6.218868
    #longitude = 106.700762
    longitude = 106.845189
    year, month, day, hour, minute, second = 2016, 5, 28, 8, 0, 0
    #-6.346005, 106.700762, 2016-06-02 22:58:04, 2016-06-03 10:44:26
    #-6.346005, 106.989617, 2016-06-02 22:56:54, 2016-06-03 10:43:16
    #-6.091731, 106.700762, 2016-06-02 22:57:38, 2016-06-03 10:44:51
    #-6.091731, 106.989617, 2016-06-02 22:56:29, 2016-06-03 10:43:42

    #-6.346005, 106.700762, 2016-07-02 23:04:30, 2016-07-03 10:50:08
    #-6.346005, 106.989617, 2016-07-02 23:03:20, 2016-07-03 10:48:59
    #-6.091731, 106.700762, 2016-07-02 23:04:04, 2016-07-03 10:50:34
    #-6.091731, 106.989617, 2016-07-02 23:02:54, 2016-07-03 10:49:25

    testObj = TestSun(timezone, latitude, longitude)
    testObj.getInputDatetime(year, month, day, hour, minute, second)
    testObj.computeSunriseTime()
    testObj.computeSunsetTime()

