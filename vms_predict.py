#!/usr/bin/env python2.7

import os,sys,math,re,random,string
#from VMSPredictionSky import VMSPredictionSky as VMSP
#from VMSPretretmentSky import executeSkyTruth as VMPT
import vms_ask_path
import datetime as dt
from ViirsSatPredict import ViirsSatPredict as vPredict
from operator import indexOf
import vms_tools,vms_constants

'''
This program takes the track read from VMS_db and
perform daily prediction.
'''

class vms_predict(object):

#    def __init__(self,trk,hdr):
    def predict_w_output(self,trk,hdr):

        for i in trk:
            print i
        int(1,2)
        
        pred=self.predict(trk,hdr)
        #the result should go into a database table
        #for now it is output as csv
        output_file=os.path.join(vms_ask_path.base_dir,'test_vms_predict.csv')

        vms_tools.write_to_file(','.join(hdr),output_file)
        for p in pred:
            line=','.join(p)
            vms_tools.write_to_file(line,output_file,append=True)

    def get_timetag(self):
        return dt.now().strftime("%Y%m%d%H%M%S%f")

    def get_randomstr(self):
        return ''.join(random.choice(string.ascii_uppercase+string.digits) for _ in range(5))

    def get_output_csv_name(self):
        work_dir=vms_ask_path.tmp_dir
        out_csv='_'.join(['vms_predict',self.get_timetag,self.get_randomstr])+'.csv'

    def predict(self,trk,hdr):
        '''
        This module predicts the boat location within given track
        at viirs overpass, and returns the predicted lcoation
        '''
        print hdr
        id_key_idx      = hdr.index('ID_KEY')
        trans_no_idx    = hdr.index('TRANSMITTER_NO')
        vessel_name_idx = hdr.index('VESSEL_NAME')
        latitude_idx    = hdr.index('LATITUDE') 
        longitude_idx   = hdr.index('LONGITUDE')
        reportdate_idx  = hdr.index('REPORTDATE')
        speed_idx       = hdr.index('SPEED')
        heading_idx     = hdr.index('HEADING')
        gross_ton_idx   = hdr.index('GROSS_TONNAGE')
        length_idx      = hdr.index('LENGTH')
        width_idx       = hdr.index('WIDTH')
        gear_type_idx   = hdr.index('REGISTERED_GEAR_TYPE')
        begin_date_idx  = hdr.index('BEGIN_DATE')
        end_date_idx    = hdr.index('END_DATE')
        region_idx      = hdr.index('REGISTERED_FISHING_REGION')

        #check input tran_no, vessel_name to see if track is from the same vessel
        tran_no_list = [i[trans_no_idx] for i in trk]
        if len(set(tran_no_list)) > 1:
            print 'Input track from multiple transmitter no.:',set(tran_no_list)
            sys.exit(1)

        print trans_no_idx
        print trk[0]
        trans_no    = trk[0][trans_no_idx]
        vessel_name = trk[0][vessel_name_idx]
        gross_ton   = trk[0][gross_ton_idx]
        width       = trk[0][width_idx]
        length      = trk[0][length_idx]
        gear_type   = trk[0][gear_type_idx]
        begin_date  = vms_constants.null_val_str #'null'
        end_date    = vms_constants.null_val_str #'null'
        region      = vms_constants.null_val_str #'null'
        
        ##convert reportdate to localdate (assume reportdate is UTC)
        trk_day = [str(i[reportdate_idx])[0:8] for i in trk]
#        print trk_uniq_day
        trk_uniq_day=set(trk_day)
#        print trk_uniq_day
#        int(1,2)

        pred=[]
        ##for each daily track segment, find record near midnight local time
        for day in sorted(trk_uniq_day):
            print 'day:',day
            daily_trk=[i for i in trk if str(i[reportdate_idx]).startswith(day)]
            for t in daily_trk:
                print t
            loc_dt=[vms_tools.time2loc(str(i[reportdate_idx]),float(i[longitude_idx])) for i in daily_trk]
            #find record cloest to midnight to start prediction
            avg_lon=sum([float(i[longitude_idx]) for i in daily_trk])/len(daily_trk)
            tz=vms_tools.find_tz_ez(avg_lon)
            midnt_dt=dt.datetime(int(day[0:4]),int(day[4:6]),int(day[6:8]),23,59,59)+dt.timedelta(tz/24.)

            midnt_dif=[i-midnt_dt for i in loc_dt]
            midnt_rec=daily_trk[midnt_dif.index(min(midnt_dif))]
            #run prediction
            start_jul=vms_tools.VMS_Time2Julian(midnt_rec[reportdate_idx])
            vpOut     = self.doPrediction(midnt_rec[latitude_idx],
                                          midnt_rec[longitude_idx],
                                          start_jul)
            vpTakeId  = self.calcAppearance(start_jul, vpOut)
            if len(vpTakeId)==0:
                return pred
                
            predJulList=[(vpOut[str(i)])['timeEndUTC_J'] for i in vpTakeId]

            #interpolate bewteen records for each overpass
            print [i for i,j in enumerate(vpTakeId)]
            for i in [i for i,j in enumerate(vpTakeId)]:
                midTime,midLat,midLon,midSpeed,midHeading=\
                    self.calcMidPos(predJulList[i],
                                    [i[latitude_idx] for i in daily_trk],
                                    [i[longitude_idx] for i in daily_trk],
                                    [i[reportdate_idx] for i in daily_trk],
                                    [i[speed_idx] for i in daily_trk],
                                    [i[heading_idx] for i in daily_trk],
                                    [i[id_key_idx] for i in daily_trk])
                midLatStr=str(midLat).replace('.','p').replace('-','m')
                midLonStr=str(midLon).replace('.','p').replace('-','m')
                pred_id_key='_'.join(['PRD',str(trans_no),str(midTime)
                                      ,midLatStr,midLonStr])

                pred_rec=(unicode(pred_id_key),unicode(trans_no),unicode(vessel_name),
                          unicode(midLat),unicode(midLon),
                          unicode(midTime),unicode(midSpeed),unicode(midHeading),
                          unicode(gross_ton),unicode(length),unicode(width),unicode(gear_type),
                          unicode(begin_date),unicode(end_date),unicode(region))
                print 'predicted:'
                print pred_rec
                pred.append(pred_rec)

        return pred

    def calcMidPos(self,predJul,boatLat,boatLon,rpt_dt_list,spd_list,hdn_list,key_list):
        '''
        Take the predicted julian time and interpolate vessel location.
        '''
        boatJulian=[vms_tools.VMS_Time2Julian(i) for i in rpt_dt_list]
        
        ##TO ADD: find errorneous records.
        #1) is the record moving too fast? 30 knot/hr is current threshold.
        #2) is the coordinates outbound?
        #3) is the records temporally too far away?
        
        diff_abs = [abs(x-predJul) for x in boatJulian]
        diff     = [(x-predJul) for x in boatJulian]
        minIdx   = indexOf(diff_abs, min(diff_abs))

        #Set lower bound
        if diff[minIdx] < 0:
            lowIdx = minIdx
        else:
            lowIdx = minIdx - 1
        upIdx = lowIdx + 1
        if upIdx > len(diff) - 1: #Which means lowIdx is already the last one
            upIdx  = upIdx - 1
            lowIdx = lowIdx - 1

        lowLat = boatLat[lowIdx]
        lowLon = boatLon[lowIdx]
        lowJul = boatJulian[lowIdx]

        upLat  = boatLat[upIdx]
        upLon  = boatLon[upIdx]
        upJul  = boatJulian[upIdx]

        #Calculate extrapolate/interpolate ratio
        try:
#            ratio  = (upJul - predJul)/(upJul - lowJul)
            ratio  = (predJul-lowJul)/(upJul - lowJul)
        except:
#            print 'ratio  = (upJul - predJul)/(upJul - lowJul) FAILED'
            print 'ratio  = (predJul-lowJul)/(upJul - lowJul) FAILED'
            print 'upJul,predJul,lowJul',upJul,predJul,lowJul
            print 'upLat,upLon,lowLat,lowLon',upLat,upLon,lowLat,lowLon
            print 'upKey,lowKey',key_list[upIdx],key_list[lowIdx]
            return -999999,91.0,181.0,-999999,-999999
            sys.exit(1)
        #Extrapolate/interpolate
        midLat = (float(upLat) - float(lowLat))*ratio + float(lowLat)
        midLon = (float(upLon) - float(lowLon))*ratio + float(lowLon)
        midLat = round(midLat,5)
        midLon = round(midLon,5)

        #Wrap result
        if abs(midLat > 90):
            if midLat > 0:
                midLat=(midLat-180)
            else:
                midLat=-(180+midLat)

        if abs(midLon > 180):
            if midLon > 0:
                midLon=midLon-360
            else:
                midLon=-(midLon+360)

        midJul = predJul
        midTime = vms_tools.VMS_Julian2Time(midJul,"%Y%m%d%H%M%S")

        #calculate midSpeed
#        dist_a=vms_tools.getDist({'x':upLon,'y':upLat},{'x':midLon,'y':midLat})/1000.
#        dist_b=vms_tools.getDist({'x':lowLon,'y':lowLat},{'x':midLon,'y':midLat})/1000.
        dist_c=vms_tools.getDist({'x':lowLon,'y':lowLat},{'x':upLon,'y':upLat})/1000.
        print 'upJul,predJul,lowJul',upJul,predJul,lowJul
        if predJul > upJul and predJul > lowJul:
            if (predJul-max([upJul,lowJul]))*24. > 2:
                print 'Extrapolation unstable.'
                return midTime,91.0,181.0,999999,999999
#        dt_a  =abs(midJul-upJul)*24. #hour
#        dt_b  =abs(lowJul-midJul)*24.#hour
        dt_c  =abs(upJul-lowJul)*24.
#        spd_a=dist_a/dt_a
#        spd_b=dist_b/dt_b
        spd_c=dist_c/dt_c
#        midSpeed=(spd_a+spd_b)/2
#        midSpeed=int(midSpeed)
        midSpeed=int(spd_c*5) #<- donno why SkyTruth VMS speed is km/h times 5...
#        print 'dist_a,dt_a,spd_a',dist_a,dt_a,spd_a,spd_a*5
#        print 'dist_b,dt_b,spd_b',dist_b,dt_b,spd_b,spd_b*5
        print 'dist_c,dt_c,spd_c',dist_c,dt_c,spd_c,spd_c*5 #<- this is a more precise measure

        #calculate midHeading
        print {'x':midLon,'y':midLat},{'x':lowLon,'y':lowLat}
        midHeading=vms_tools.getHeading({'x':midLon,'y':midLat},{'x':upLon,'y':upLat})
#        midHeading=vms_tools.getHeading({'x':lowLon,'y':lowLat},{'x':upLon,'y':upLat})
#        print 'meanHeading',meanHeading

        return midTime, midLat, midLon, midSpeed, midHeading


    def calcAppearance(self, start_jul,vpOut):
        '''
        Select the correct prediction from ViirsPredict output
        '''
        sortList = list(vpOut)
        sortList.sort()

        jt       = [x for x in ((vpOut[i])['timeEndUTC_J'] for i in sortList)]
        dt       = [(x - start_jul) for x in ((vpOut[i])['timeEndUTC_J'] for i in sortList)]
        dn       = [x for x in ((vpOut[i])['dayNight'] for i in sortList)]

        #Find consecutive overpass within closest night
        dt_buff = 1
        id_buff = ''
        for i in range(0,len(dt)-1):
            #Find minimal dt

            if (dt[i] < dt_buff) and (dn[i] == 0):
                dt_buff = dt[i]
                id_buff = sortList[i]

        #Find consecutive passes
        #by looking neighboring nights within +/- 12 hours of the first prediction
        if id_buff=='':
            print vpOut
            print 'No predictgion was found.'
            return []
        print 'id_buff',id_buff
            
        pjt       = jt[int(id_buff)-1]
        #Consecutive passes
        conPass   = [jt[i] > pjt-0.5 and jt[i] < pjt+0.5
                     and dn[i] == 0 for i in range(0,len(jt)-1)]

        idList    = [i+1 for i,j in enumerate(conPass) if j]

 #       if self.verbose:
        print 'Select prediction ID: '+str([i for i in idList])

        return idList



    def doPrediction(self, lat, lon, jtime):
        vp = vPredict()                   #initialize predictor instance
        vp.obsPos['lat']   = float(lat)   #Latitude
        vp.obsPos['lon']   = float(lon)   #Longitude
        vp.obsPos['alt']   = 0            #Altitude -> Set to zero
        vp.startJulianTime = round(jtime) #Start prediction from midday
        vp.predictDays     = 3            #Days
        vp.swath           = 3000         #Km
        vp.verbose         = False
#        vp.verbose         = self.verbose #Show prediction result
        vp.sphericalEarth  = False        #Use Vincenty's Formulae
        vp.timeZone        = 7
        vp.tleL1           = ''
        vp.tleL2           = ''
        if vp.verbose:
            print 'Target lat/lon: '+'%.4f'%vp.obsPos['lat']+'/'+'%.4f'%vp.obsPos['lon']
            print 'Start time (Julian): '+'%.9f'%vp.startJulianTime
        vpOut = vp.run()
        return vpOut
        
        

