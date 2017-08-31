#!/usr/bin/env python

##===================================================================================
#
# ViirsSatPredict.py
#
# This code is designed to calculate the overpass time of satellite given
# an observer on the ground (Altitude = 0).
# This program is by default designed for NPP VIIRS, but can be used for other
# satellties with minimal modification needed.
# 
# How to run:
#    Users need to create an instance, input parameters by changing the 
#    instance attributes. The run method will trigger the prediction process,
#    returning predicted result in nested dictionary.
# 
# Example:
#    import ViirsSatPredict
#    prd = ViirsSatPredict.ViirsSatPredict()
#    prd.tleL1='1 37849U 11061A   15010.14957891  .00000088  00000-0  62074-4 0  9888'
#    prd.tleL2='2 37849  98.6834 312.3948 0001412 123.3398 352.2834 14.19582225165950'
#    prd.startDate='2014-10-31'
#    prd.obsPos['lat']=-5.04309
#    prd.obsPos['lon']=136.9833
#    prd.obsPos['lat']=0
#    output = prd.run()
#
# Input:
#
#    ++++ TLE assigning ++++
#    .tleL1       TLE line one.
#    .tleL2       TLE line two.
#    .tleDir      TLE search directory. If both .tleL1 and .tleL2 are empty, the program 
#                 will search in .tleDir for the text file with filename having the 
#                 cloest date
#    .tleFile     Alternatively user can directly give tle file with string 'NPP' as 
#                 line zero.
#
#    ++++ Time assigning ++++
#    .startDate   Prediction start date in format 'yyyy-mm'dd'
#    .predictDays Number of days to predict, default=3.
#    .startJulianTime  Prediction start time in julian day. NOTE: Input of .startJulianTime
#                      will prevail .startDate for it is a more precise way of designation.
#    .timeZone    Desired time zone for local time reporting in integers.
#    .useTLEEpoch Use the starttime of TLE instead of user input date/time.
#    
#    ++++ Spatial assigning ++++
#    .obsPos      Observer coordinates in decimat lat(deg)/lon(deg)/alt(m).
#                 NOTE: The input should be in dictionary format as 
#                 {'lat':x.xxx,'lon':x.xxx,'alt':0} Altitude should always be zero.
#    .swath       Satellite sensor swath. Default is 3000 Km for VIIRS.
#
#    ++++ Misc. input ++++
#    .verbose    Print prediction result and messages on screen.
#    .sphericalEarth   Assume Earth as a perfect sphere when calculating distance between 
#                      two locations on the surface. If turned off by set to False, 
#                      Vincenty's formulae will be used to give a more precise result.
#    .applyCorrection  Spherical assumption in distance calculation will induce latitudal
#                      errors in overpass time prediction. Such is corrected by applying
#                      a polynomial derived from 40K points of prediction around globe.
#                      By default is turned on, will be diabled if .spericalEarth is set
#                      to False.
# Output:
#      
#   Output is in nested dictionary foramt.
#   {'1':                                        < Numerical id of nexted dictionary
#        { 'id':x,                               < Numerical id of overpass
#          'satLat':x.xx,                        < Predicted satellite latitude
#          'satLon':x.xx,                        < Predicted satellite longitude
#          'scanAngle':x.xx,                     < Predicted scanangle from satellite
#          'distance':x.xx,                      < Predicted distance between 
#                                                  satellite and observer
#          'bearing':x.xx,                       < Predicted bearing angle (deg) from
#                                                  satellite to observer
#          'timeEndUTC':yyyy-mm-dd hh:mm:ss,     < UTC time of overpass in string format
#          'timeEndLOC':yyyy-mm-dd hh:mm:ss(z),  < Local time of overpass in string format
#          'timeEndUTC_J':x.xxx                  < UTC time of overpass in Julian
#          'timeEndLOC_J':x.xxx                  < Local time of overpass in Julina
#          'timeIniUTC_J':x.xxx                  < UTC initial time of prediction in Julian
#          'dayNight':x                          < 1:Daypass, 0:Nightpass
#   }}
#
# Dependency:
#   sgp4
#   jdcal
#
# Version History:
#   2015/6 v1.0
#   2015/7 v1.1 Enabled online TLE update for NPP.
#
# Author:
#   David, Hsu
#   feng.c.hsu@noaa.gov
#   2015/6
#
##=======================================================================================


import os
import sys
import inspect
import glob
import math
from sgp4 import propagation
from sgp4 import io
from sgp4.earth_gravity import wgs72, wgs84
from datetime import datetime
from sgp4.ext import days2mdhms, jday, invjday
from sgp4.propagation import sgp4
import jdcal
import time
import urllib
import re

#Default settings
tleDir='./tle'
tleFile=''
tleDb='./NPP_tle.txt'
tleDbWeb='https://www.ngdc.noaa.gov/eog/viirs/npp_tle/NPP_tle.txt'
overpass=[]
obsPos={'lat':0,'lon':0,'alt':0} #in decimal degrees
startDate=''  #format yyyy-mm-dd in UTC
predictDays=3
verbose=True
applyCorrection=False #By default will activate if spherical Earth is assumed
sphericalEarth=True#False
tleL1=''
tleL2=''
startJulianTime=''
satrec=''
#Constants
mpd=float(24*60)
spd=float(24*60*60)
pi=math.pi
twopi=2*pi
deg2rad=pi/float(180)
rad2deg=1/deg2rad
earthR=6378.137
#Setting
swath=float(3000) #km
iteLimitDef=1000
tspanDef=float(120) #sec
timeZone=0
useTLEEpoch=0

class ViirsSatPredict(object):

    def __init__(self):
    #take assignments
        self.obsPos=obsPos
        self.tleDir=tleDir
        self.tleDb=tleDb
        self.tleDbWeb=tleDbWeb
        self.startDate=startDate
        self.predictDays=predictDays
        self.sphericalEarth=sphericalEarth
        self.tleL1=tleL1
        self.tleL2=tleL2
        self.verbose=verbose
        self.applyCorrection=applyCorrection
        self.tleFile=tleFile
        self.startJulianTime=startJulianTime
        self.timeZone=timeZone
        self.swath=swath
        self.useTLEEpoch=useTLEEpoch

    def run(self):
        #consolidate and check input variables
        self._checkTimeAndTLE()

        result=self._run_predict(self.satrec, float(self.startJulianTime),
                                 self.obsPos, self.predictDays, self.verbose)
        return result

    def _checkTimeAndTLE(self):
        #first, chekc input time or date
        #user input start julian time always prevail date, 
        #nomatter if date is also user input
        timeSet=1
        if self.startJulianTime != '':
            self.startDate=self.jd2Date(float(self.startJulianTime))
        elif self.startDate != '':
            self.startJulianTime=self.date2JD(self.startDate)
        else:
            print 'Warning: time/date not set.'
            timeSet=0

        #second, check if TLE L1/L2 is assigned
        #search for TLE and parse for L1/L2 if not assigned yet
        if (self.tleL1=='' or self.tleL2==''):
            if timeSet==1:
#                self._findTLE(self.tleDir,self.startDate)
                self._findTLE(self.startDate)
            else:
                print 'Both time/date and TLE is not set!'
                sys.exit(1)
        self.satrec=io.twoline2rv(self.tleL1, self.tleL2, wgs72)

        #third, check if told to use TLE epoch
        #overwrite existing time/date or assign if there is none.
        if self.useTLEEpoch:
            print 'use TLE epoch time.'
            #self.satrec=io.twoline2rc(self.tleL1, self.tleL2, wgs72)
            self.startJulianTime=self.satrec.epoch
            self.startDate=self.jd2Date(self.startJulianTime)

    def _checkStartTime(self):
        #Existence of startJulianTime prevails date.
        if self.startJulianTime != '': 
            #overwrite startdate
            self.startDate=self.jd2Date(self.startJulianTime)
            return()
        #Start date not given
        if self.startDate == '':
            if self.useTLEEpoch:
                if self.tleL1 != '' and self.tleL2 != '':
                    print 'Use TLE epoch time.'
                    self.satrec=io.twoline2rv(self.tleL1, self.tleL2, wgs72)
                    self.startDate=self.satrec.epoch
                else:
                    print 'TLE file not defined, cannot use TLE epoch time.'
                    sys.exit(1)
            else:
                print 'Start time unknown.'
                sys.exit(1)
        #Start date is given
        else:
            #parse date to julian
            self.startJulianTime=self.date2JD(self.startDate)


    def date2JD(self, startDate):
        date=startDate.split('-')
        return jday(int(date[0]),
                    int(date[1]),
                    int(date[2]),0,0,0)

    def jd2Date(self, jd):
        return invjday(jd)

#    def _checkTLEInput_old(self):
#        if self.tleL1 == '' or self.tleL2 == '':
#            if self.tleFile == '' and self.startDate != '':
#                self._findTLE(self.tleDir,self.startDate)
#            elif self.startDate == '':
#                print 'Both TLE and Start Time is not set!'
#                sys.exit(1)
#            else:
#                self._splitTLE(self.tleFile)
#        if self.tleL1 == '' or self.tleL2== '':
#            print 'Error finding correct tle'
#            sys.exit(1)

    def _cehckTLEInput(self):
        if self.tleL1 == '' or self.tleL2 == '':
            if self.startDate != '':
                self._findTLE(self.startDate)
            elif self.startDate == '':
                print 'Both TLE and Start Time is not set!'
                sys.exit(1)
        #check again
        if self.tleL1 == '' or self.tleL2== '':
            print 'Error finding correct tle'
            sys.exit(1)


    def _findTLE(self,date):

        self._checkTLEStatus()

        epoch=self._calcEpoch(date)
        #search TLE L1 for same day
        searchFile=open(self.tleDb,'r')

        dt_buff=99999.
        takeNext=False

        for line in searchFile:

            if takeNext:
                self.tleL2=line
                break

            ts=re.search('^1\ .{6}\ .{8}\ (.{14}).*',line)
            try:
                if ts.group(1):
                    dt=abs(float(ts.group(1))-float(epoch))
                    if dt<dt_buff: 
                        dt_buff=dt
                    else:
                        self.tleL1=line
                        takeNext=True
            except:
                continue

    def _checkTLEStatus(self):
        #if the tle db is more than 3 days old, check online db
        doDownload=False
        try:
            ftime=os.path.getmtime(self.tleDb)
            dt=time.time()-ftime
            if dt/86400. > 3: 
                doDownload=True
                print 'TLE DB is older than 3 days, attempt update...'
        except:
            print 'TLE DB non exist, attempt download...'
            doDownload=True

        if doDownload:
            self._downloadTLEDB()

    def _downloadTLEDB(self):
        try:
            dl=urllib.URLopener()
            dl.retrieve(self.tleDbWeb,self.tleDb)
            print 'Update success.'
        except:
            print 'Update failed.'

    def _calcEpoch(self,date):
        year=(str(date[0]))[2:] #take the last 2 digit
        doy=str(self._calcDOY(date))
        return year+doy

    def _calcDOY(self,date):
        dateStr=str(date[0])+str(date[1])+str(date[2])
        t=time.strptime(dateStr,'%Y%m%d')
        doy=time.strftime('%j',t)
        return doy
        
#    def _findTLE_old(self,Dir,date):
#
#        dateStr=str(date[0])+str(date[1])+str(date[2])
#        
#        if self.verbose:
#            print 'Find TLE list for '+dateStr+' in '+Dir+'.'
#        
#        tleList=glob.glob(Dir+'/*/*'+dateStr+'*txt')
#        for i in tleList:
#            if self.verbose:
#                print 'Processing file:'+i
#            a=self._splitTLE(i)
#            if a==0:
#                break

#    def _splitTLE(self, tleFile):
#        #read tle file and search for NPP
#        with open(tleFile) as f:
#            lines=f.readlines()
#        i=0
#        while i < len(lines):
#            if 'NPP' in lines[i].replace('\r\n',''):
#                try:
#                    self.tleL1=lines[i+1].replace('\r\n','')
#                    self.tleL2=lines[i+2].replace('\r\n','')
#                    if self.verbose:
#                        print 'Found TLE: '
#                        print self.tleL1
#                        print self.tleL2
#                    return 0
#                except:
#                    print 'Error reading NPP tle in file:'+tleFile
#                    return 1
#                break
#            i=i+1

    def _run_predict(self,satrec, startTime, obsPos, predictDays, verbose, tleEpoch=False):
        #call predict method and run for designated time_rnage
        dtime=0
        cnt=0
        
        #startTime=self.date2JD(startDate)
        dataLineBuff=''

        if self.verbose:
            #Correction will only be applied when spherical earth assumption is true.
            if self.applyCorrection and self.sphericalEarth:
                print '+++++'
                print 'NOTICE: PREDICTION TIME CORRECTION IS [[ON]]'
                print '+++++'

        while (dtime < predictDays):
            cnt += 1

            out=self.predict(satrec,startTime,obsPos)
            
            if cnt == 1:
                timeIniUTC=out['timeIniUTC']
 
            startTime=out['timeEndUTC']+900/86400.
            if out['error']:
                cnt -= 1
                continue

            eu=self.jd2Date(out['timeEndUTC'])
            el=self.jd2Date(out['timeEndLOC'])

            timeEndUTC_str=('-'.join([str(eu[0]),str(eu[1]),str(eu[2])])+' '+
                            ':'.join([str(eu[3]),str(eu[4]),str(round(eu[5],2))]))

            timeEndLOC_str=('-'.join([str(el[0]),str(el[1]),str(el[2])])+' '+
                            ':'.join([str(el[3]),str(el[4]),str(round(el[5],2))])+
                            ''.join(['(',str(self.timeZone),')']))

            dataLine=[cnt, out['satLat'], out['satLon'], out['scanAngle'],
                      out['distance'], out['bearing'],
                      timeEndUTC_str,timeEndLOC_str,
                      1 if (out['satVelZ'] >0) else 0]

            if self.verbose:
                print dataLine

            #prepare output tuple of dictionaries
            
            a={'id':cnt,'satLat':out['satLat'],'satLon':out['satLon'],
               'scanAngle':out['scanAngle'],'distance':out['distance'],'bearing':out['bearing'],
               'timeEndUTC':timeEndUTC_str,'timeEndLOC':timeEndLOC_str,
               'timeEndUTC_J':out['timeEndUTC'],'timeEndLOC_J':out['timeEndLOC'],
               'timeIniUTC_J':out['timeIniUTC'],
               'dayNight':dataLine[-1]}
            if cnt == 1:
                result={str(cnt):a}
            else:
                result[str(cnt)]=a

            #Take record of departed time from start, to know when to stop
            dtime = out['timeEndUTC']-timeIniUTC

        #return result in tuple of dictionaries
        return result
                

    #make real propagator method and let it to be called from outside
    def predict(self, satrec, startJTime, obsPos):
        obsGeo={'x':obsPos['lon'],'y':obsPos['lat'],'z':obsPos['alt']}
        obsEci={'x':0,'y':0,'z':0}
        satGeo={'x':0,'y':0,'z':0}
        satEci={'x':0,'y':0,'z':0}

        bearingPrev     = 0 #initialize bearing buffer
        bearingDiffPrev = 100 #initialize bearing difference buffer
        beginnerFlag    = 2 #ignore the first 2 iterations for propagation init.
        propCnt         = 0 #initialize propagation count
        error           = 0 #error flag
        iteLimit        = iteLimitDef #propagation itemration limit
        halfSwath       = swath/2
        startEpoch      = startJTime

        actualEpoch     = startJTime
        surfaceDistance = 0
        bearing         = 0
        tspan           = tspanDef

        tspanBuff       = 0
        passedObsBuff   = 0
        visibleBuff     = 0
        passedObs       = 0
        visible         = 0

        #Start Propogation
        bearingPrecision=0.0001

        while (not(passedObs and visible and (math.fabs(bearingDiffPrev) < bearingPrecision)) 
               and not error):
            
            propCnt=propCnt+1

            if propCnt > iteLimit-1:
                if self.verbose:
                    print 'Exceed iteration limit.'
                error=1

            dtMinutes=(actualEpoch - satrec.jdsatepoch)*mpd
            r,v=sgp4(satrec,dtMinutes)
            satEci={'x':r[0],'y':r[1],'z':r[2]}
            satVel={'x':v[0],'y':v[1],'z':v[2]}

            epoch=actualEpoch

            satGeo=self._eci2geo(satEci, actualEpoch)
            obsEci=self._geo2eci(obsGeo, actualEpoch)
            scn=self._getVec(obsEci,satEci) #scan direction vector from sat to obs
            
            bearingRaw=self._getAng(scn,satVel)*rad2deg

            bearing=bearingRaw
            if satGeo['x'] > obsGeo['x']: 
                bearing=0-bearingRaw

            bearingDiff=math.fabs(bearing)-90

            #check if propogation had passed the observer
            passedObs=0
            if ((bearingDiff*bearingDiffPrev < 0) and not beginnerFlag):
                passedObs=1

            #decrease beginner flag count till zero
            beginnerFlag=beginnerFlag-1
            beginnerFlag=math.fabs(beginnerFlag*(beginnerFlag>0))

            #update bearing buffers
            bearingDiffPrev=bearingDiff
            bearingPrev=bearing

            #Spherical Earth big circle distance
            if self.sphericalEarth:
                ang=self._getAng(obsEci,satEci)
                surfaceDistance=(ang*earthR)
                try:
                    surfaceDistance_v=self.getDis(obsGeo,satGeo)
                except:
                    surfaceDistance_v=0

            else:
            #Vincenty's Formulae
                try:
                    surfaceDistance=self.getDis(obsGeo,satGeo)
                except:
                    if self.verbose:
                        print 'Vincenty\'s formulae failed. Fall back to spherical Earth.'
                    ang=self._getAng(obsEci,satEci)
                    if self.verbose:
                        print 'Angle between coordinated: '+'%.3f'%ang
                    surfaceDistance=(ang*earthR)
                
            visible=0
            if (surfaceDistance < halfSwath):
                visible=1

            actualEpoch=actualEpoch+tspan/spd

            if ((not passedObsBuff and passedObs) and (visibleBuff and not visible)):
                visible=1

            #update buffer
            passedObsBuff = passedObs
            visibleBuff   = visible

            if (passedObs and visible):
                tspan=float(tspan)/(-20.)

        dx=satEci['x']-obsEci['x']
        dy=satEci['y']-obsEci['y']
        dz=satEci['z']-obsEci['z']
        slant=math.sqrt(dx*dx+dy*dy+dz*dz)
        alpha=self._getAng(obsEci,satEci)
        scanAngle=math.asin(earthR*math.sin(alpha)/slant)*rad2deg

        #Correction will only be applied when spherical earth assumption is true.
        if self.applyCorrection and self.sphericalEarth:
            endTimeJulUtc=self._applyCorrection(actualEpoch,obsGeo['y'])
        else:
            endTimeJulUtc=actualEpoch

        endTimeJulLoc=endTimeJulUtc+(self.timeZone)/24.
        iniTimeJulLoc=startEpoch+(self.timeZone)/24.
#        print 'do return'
        return {'satLat':satGeo['y'],'satLon':satGeo['x'],'satAlt':satGeo['z'],
                'obsLat':obsGeo['y'],'obsLon':obsGeo['x'],'obsAlt':obsGeo['z'],
                'satVelX':satVel['x'],'satVelY':satVel['y'],'satVelZ':satVel['z'],
                'satEciX':satEci['x'],'satEciY':satEci['y'],'satEciZ':satEci['z'],
                'obsEciX':obsEci['x'],'obsEciY':obsEci['y'],'obsEciZ':obsEci['z'],
                'scanAngle':scanAngle,
                'distance':surfaceDistance,
                'bearing':bearing,
                'timeEndUTC':endTimeJulUtc, 'timeIniUTC':startEpoch,
                'timeEndLOC':endTimeJulLoc, 'timeIniLOC':iniTimeJulLoc,
                'timeZone':timeZone,
                'propCnt':propCnt, 'timeSpan':tspan,
                'error':error}

#==================
## TOOL CHEST
#==================

    def getGreatCircleAng(self,lla1,lla2):
        lat1 = lla1['y']*deg2rad
        lat2 = lla2['y']*deg2rad
        lon1 = lla1['x']*deg2rad
        lon2 = lla2['x']*deg2rad
        d = math.acos((math.sin(lat1)*math.sin(lat2))+(math.cos(lat1)*math.cos(lat2)*math.cos(abs(lon1-lon2))))
        return d

    def _applyCorrection(self, t, lat):
        #apply correction for sphere assumption in propogation
        # input t in Julian                                
        # Y=7E-8*X^4+2E-5*X^3-3E-4*X^2-9.61E-2*X-0.7046 R2=0.885
        # Y: Departed seconds, X: Latitude of observer
        if lat < 65 and lat > -80:
            cor=7e-8*lat**4+2e-5*lat**3-3e-4*lat**2-9.61*lat-0.7046
        else:
            cor=0
        return t-cor/spd
        
    def _getAng(self, v1,v2):
        #return angle between vevtors in rad
        ang = math.acos((v1['x']*v2['x']+v1['y']*v2['y']+v1['z']*v2['z'])/
                        (math.sqrt(v1['x']**2+v1['y']**2+v1['z']**2)*
                         math.sqrt(v2['x']**2+v2['y']**2+v2['z']**2)))

        return ang

    def _getVec(self, p1,p2):
        v={'x':p1['x']-p2['x'],
           'y':p1['y']-p2['y'],
           'z':p1['z']-p2['z']}
        n=math.sqrt(v['x']*v['x']+v['y']*v['y']+v['z']*v['z'])
        return {'x':v['x']/n,
                'y':v['y']/n,
                'z':v['z']/n}
    '''
    Taken from sgp4.propagation._gstime(jdut1)
    '''
    def _gstime(self, jdut1):
        tut1 = (jdut1 - 2451545.0) / 36525.0
        temp = (-6.2e-6* tut1 * tut1 * tut1 + 0.093104 * tut1 * tut1 + 
                 (876600.0*3600 + 8640184.812866) * tut1 + 67310.54841)  #  sec                                         
        temp = (temp * deg2rad / 240.0) % twopi # 360/86400 = 1/240, to deg, to rad                                    

     #  ------------------------ check quadrants ---------------------                                              
        if temp < 0.0:
            temp += twopi

        return temp

    def _geo2eci(self, geo_pos,jd):
        gmst=self._gstime(jd)
        geoPosT={'longitude':geo_pos['x']*deg2rad,
                 'latitude':geo_pos['y']*deg2rad,
                 'height':geo_pos['z']}
        ecfPos =self._geo2ecf(geoPosT)#,gmst)
        eciPos =self._ecf2eci(ecfPos,gmst)
        return eciPos

    def _geo2ecf(self,geodetic_coords):
        longitude=geodetic_coords['longitude']
        latitude=geodetic_coords['latitude']
        height=geodetic_coords['height']
        a=6378.137
        b=6356.7523142
        f=(a-b)/a
        e2=((2*f)-(f*f))
        normal=a/math.sqrt(1-(e2*(math.sin(latitude)*math.sin(latitude))))
        
        return {'x': (normal+height)*math.cos(latitude)*math.cos(longitude),
                'y': (normal+height)*math.cos(latitude)*math.sin(longitude),
                'z': ((normal*(1-e2))+height)*math.sin(latitude)}

    def _ecf2eci(self,ecf_coords,gmst):
        # ccar.colorado.edu/ASEN5070/handouts/coordsys.doc
        #
        #[X]    [C -S  0][X]
        #[Y]  = [S  C  0][Y]
        #[Z]eci [0  0  1][Z]ecf
        #
        return {'x': (ecf_coords['x']*math.cos(gmst))-(ecf_coords['y']*math.sin(gmst)),
                'y': (ecf_coords['x']*(math.sin(gmst)))+(ecf_coords['y']*math.cos(gmst)),
                'z': ecf_coords['z']}

    def _eci2geo_secondary(self,eci_coords,jd):
        #Taken from IDL code eci_to_lla.pro

        # f = Earth oblateness flatteing factor
        f=1./298.257
        re=earthR

        # Get Greenwich sidereal time
        gmst = self._gstime(jd)

        #Calculate length of position vector
        rs = math.sqrt(eci_coords['x']**2+eci_coords['y']**2+eci_coords['z']**2)
        
        # Calculate normalized position vector
        rnx = eci_coords['x']/rs
        rny = eci_coords['y']/rs
        rnz = eci_coords['z']/rs

        #Calculate declination, geodetic latitude and altitude above oblate sphere
        dec = math.asin(rnz)
        lat = math.atan(math.tan(dec)/(1.-f)**2)
        alt = re * (rs/re-(1-f)/(math.sqrt(1-f*(2-f)*(math.cos(dec))**2)))

        #Calculate right ascension and geocentric longitude of satellite
        ra = math.atan2(rny,rnx)
        lon=math.atan2(math.sin(ra-gmst),math.cos(ra-gmst))
        
        #Convert radians into degrees
        lla=eci_coords
        lla['x']=lon*rad2deg
        lla['y']=lat*rad2deg
        lla['z']=alt

        return lla

    def _eci2geo(self,eci_coords, jd):

        # Credit
        #/*
        #* satellite-js v1.1
        #* (c) 2013 Shashwat Kandadai and UCSC
        #* https://github.com/shashwatak/satellite-js
        #* License: MIT
        #*/

        #use strict';
        #// http://www.celestrak.com/columns/v02n03/

        gmst=self._gstime(jd)
        
        a=6378.137
        b=6356.7523142
        R  = math.sqrt((eci_coords['x']*eci_coords['x'])+(eci_coords['y']*eci_coords['y']))
        f  = (a-b)/a
        e2 = ((2*f)-(f*f))
        longitude = math.atan2(eci_coords['y'],eci_coords['x'])-gmst

        kmax = 20
        k = 0
        latitude = math.atan2(eci_coords['z'],
                              math.sqrt(eci_coords['x']*eci_coords['x']+
                                        eci_coords['y']*eci_coords['y']))
        
        C=0
        while (k<kmax):
            C=1/math.sqrt(1-e2*(math.sin(latitude)*math.sin(latitude)))
            latitude=math.atan2(eci_coords['z']+(a*C*e2*math.sin(latitude)),R)
            k+=1
        height=(R/math.cos(latitude))-(a*C)

        longitude = longitude*rad2deg
        latitude = latitude*rad2deg

        #Warp longitude between -180 and 180 degrees
        if longitude < -180: longitude = longitude+360

        return {'x':longitude,'y':latitude,'z':height}

##=============
# Currently this precise distance calculation is not yet fully tested. And
# might prone to error. Legacy spherical model with prediction time correction
# should be used when running automate large volume task.
##=============


    def getDis(self, coord1, coord2):
        #calculate distance between two lat/lon coordinates
        # coord1, coord2: lat/lon in degree

        if coord1['x']==coord2['x'] and coord1['y']==coord2['y']:
            return 0.

        #convert coords to rad
        phi1=coord1['y']*deg2rad
        lembda1=coord1['x']*deg2rad
        phi2=coord2['y']*deg2rad
        lembda2=coord2['x']*deg2rad

        ##===================
        # Following code block taken from 
        # ftp://pdsimage2.wr.usgs.gov/pub/pigpen/Python/Geodetic_py.py
        # Courtesy to Jim Leven
        #====================
        """ 
        Returns the distance between two geographic points on the ellipsoid
        and the forward and reverse azimuths between these points.
        lats, longs and azimuths are in decimal degrees, distance in metres 

        Returns ( s, alpha12,  alpha21 ) as a tuple
        """

        if (abs( phi2 - phi1 ) < 1e-8) and ( abs( lembda2 - lembda1) < 1e-8 ) :
                return 0.0, 0.0, 0.0


        f=1.0 / 298.257223563 #WGS94
        a=earthR*1000 #meters
        b = a * (1.0 - f)

        TanU1 = (1-f) * math.tan( phi1 )
        TanU2 = (1-f) * math.tan( phi2 )

        U1 = math.atan(TanU1)
        U2 = math.atan(TanU2)

        lembda = lembda2 - lembda1
        last_lembda = -4000000.0 # an impossibe value
        omega = lembda

        # Iterate the following equations, 
        #  until there is no significant change in lembda 

        cnt=0
        while ( last_lembda < -3000000.0 or lembda != 0 and abs( (last_lembda - lembda)/lembda) > 1.0e-9 ) :
                cnt=cnt+1
#                if cnt > 10000: 
#                    print [last_lembda,lembda,abs( (last_lembda - lembda)/lembda),                                                                                        ( last_lembda < -3000000.0 or lembda != 0 and abs( (last_lembda - lembda)/lembda) > 1.0e-9 )] 
                sqr_sin_sigma = pow( math.cos(U2) * math.sin(lembda), 2) + \
                        pow( (math.cos(U1) * math.sin(U2) - \
                        math.sin(U1) *  math.cos(U2) * math.cos(lembda) ), 2 )

                Sin_sigma = math.sqrt( sqr_sin_sigma )

                Cos_sigma = math.sin(U1) * math.sin(U2) + math.cos(U1) * math.cos(U2) * math.cos(lembda)
        
                sigma = math.atan2( Sin_sigma, Cos_sigma )

                Sin_alpha = math.cos(U1) * math.cos(U2) * math.sin(lembda) / math.sin(sigma)
                alpha = math.asin( Sin_alpha )

                Cos2sigma_m = math.cos(sigma) - (2 * math.sin(U1) * math.sin(U2) / pow(math.cos(alpha), 2) )

                C = (f/16) * pow(math.cos(alpha), 2) * (4 + f * (4 - 3 * pow(math.cos(alpha), 2)))

                last_lembda = lembda

                lembda = omega + (1-C) * f * math.sin(alpha) * (sigma + C * math.sin(sigma) * \
                        (Cos2sigma_m + C * math.cos(sigma) * (-1 + 2 * pow(Cos2sigma_m, 2) )))

                if cnt > 100: raise
        u2 = pow(math.cos(alpha),2) * (a*a-b*b) / (b*b)

        A = 1 + (u2/16384) * (4096 + u2 * (-768 + u2 * (320 - 175 * u2)))

        B = (u2/1024) * (256 + u2 * (-128+ u2 * (74 - 47 * u2)))

        delta_sigma = B * Sin_sigma * (Cos2sigma_m + (B/4) * \
                (Cos_sigma * (-1 + 2 * pow(Cos2sigma_m, 2) ) - \
                (B/6) * Cos2sigma_m * (-3 + 4 * sqr_sin_sigma) * \
                (-3 + 4 * pow(Cos2sigma_m,2 ) )))

        s = b * A * (sigma - delta_sigma)

##=========================================================
##Do not need this part for calculating forward/reverse azimuth
#        alpha12 = math.atan2( (math.cos(U2) * math.sin(lembda)), \
#                (math.cos(U1) * math.sin(U2) - math.sin(U1) * math.cos(U2) * math.cos(lembda)))

#        alpha21 = math.atan2( (math.cos(U1) * math.sin(lembda)), \
#                (-math.sin(U1) * math.cos(U2) + math.cos(U1) * math.sin(U2) * math.cos(lembda)))

#        if ( alpha12 < 0.0 ) : 
#                alpha12 =  alpha12 + two_pi
#        if ( alpha12 > two_pi ) : 
#                alpha12 = alpha12 - two_pi

#        alpha21 = alpha21 + two_pi / 2.0
#        if ( alpha21 < 0.0 ) : 
#                alpha21 = alpha21 + two_pi
#        if ( alpha21 > two_pi ) : 
#                alpha21 = alpha21 - two_pi

#        alpha12    = alpha12    * 45.0 / piD4
#        alpha21    = alpha21    * 45.0 / piD4last_lembda = -4000000.0lembda1 = lembda1 * piD4 / 45.0
##========================================================
        return s/1000 #convert to KM
        
    


