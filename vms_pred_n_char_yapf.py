#!/usr/bin/env python2.7

import os, sys, sqlite3
import vms_ask_path, vms_tools, vms_constants
from vms_characterize import vms_characterize
from vms_predict import vms_predict
import vms_landing_site
import numpy as np
from itertools import groupby
from dateutil.relativedelta import relativedelta
import psycopg2  #postgresql interface
from psycopg2 import sql
'''
This program reads VMS database, and 
(1) predicts the vessle location at the time when VIIRS overpass. 
(2) characterize record status from ship movement (speed, heading) into
    fishing, landing, transporting, and unknown.
The result is saved in the same database.

Created by: David -20170316
'''


class vms_pred_n_char(object):
    def __init__(self):
        '''
        Update all vessels by default.
        '''
        try:
            config_file = sys.argv[1]
        except:
            print 'Must input config file.'
            sys.exit(1)
        if not os.path.exists(config_file):
            print 'Cannot find config file:', config_file
            sys.exit(1)
        self.update_by_config(config_file)

    def update_by_config(self, config_file):
        '''
        This module will update the DB according to the config file.
        '''

        cfg = vms_tools.parse_config(config_file)

        start_date = cfg['timing']['start_date']
        end_date = cfg['timing']['end_date']
        interval = cfg['timing']['interval']  #by month
        trans_no = cfg['boat_info']['trans_no']
        vessel_name = cfg['boat_info']['vessel_name']
        gear_type = cfg['boat_info']['gear_type']
        tonnage = cfg['boat_info']['tonnage']  #currently unused
        width = cfg['boat_info']['width']  #currently unused
        length = cfg['boat_info']['length']  #currently unused
        mode = cfg['flow_control']['mode']
        #        pick_up    = cfg['flow_control']['pick_up']
        #        run_all    = cfg['flow_control']['run_all']
        #        run_one    = cfg['flow_control']['run_one']
        ignr_pend = cfg['flow_control'][
            'ignr_pend']  #do not process previous pending records
        skip_pred = cfg['flow_control'][
            'skip_pred']  #skip prediction to save time if already predicted
        skip_clas = cfg['flow_control'][
            'skip_clas']  #skip status classicitation

        if start_date is '':
            start_date = '20140101'  #default start date
        if end_date is '':
            end_date = '20161231'  #default end_date
        if interval is '':
            interval = 4  #default interval is 4 months

        if gear_type is '':
            print 'Gear type must be set. Abort'
            sys.exit(1)

        print 'start updating selected:'
        print '>timing<'
        print 'start_date :', start_date
        print 'end_date   :', end_date
        print 'interval   :', interval
        print '>boat_info<'
        print 'trans_no   :', trans_no
        print 'vessel_name:', vessel_name
        print 'gear_type  :', gear_type

        ##        print 'tonnage    :',tonnage
        ##        print 'width      :',width
        ##        print 'length     :',length
        ###Flow control judgement
        '''
        first assume processing mode from boat_info input
        if nothing is given, then terminate. NEED AT LEAST GEAR TYPE
        if only gear_type is give, then RUN_ALL or
        if trans_no is also given, then PICK_UP
        auto-determining mode does not set to RUN_ONE

        if start_date is null, default will be the earliest date of DB
        if end_date is null, default will be the last date of DB
        if interval is null, default will be 4 months
        
        input in flow_contrl can override the auto-decided mode
        PICK_UP/RUN_ALL/RUN_ONE are mutually exclusive modes

        PICK_UP requires gear_type and trans_no 
        RUN_ALL requires gear_type only and will ignore all others
        RUN_ONE requires gear_type and trans_no and respects all others
        '''
        #automatic timing
        #if end_date < start_date, terminate and show error
        '''
        auto timing disabled due to skipping records in pick up mode.
        caused by not knowing which interval the process was in
        '''
        start_date_dt = vms_tools.conv_str_to_dt(start_date)
        end_date_dt = vms_tools.conv_str_to_dt(end_date)
        if end_date_dt < start_date_dt:
            print 'End date is earlier than start date, abort.'
            sys.exit(1)
        #if end_date-start_date > interval, ignore interval
        date_list = []
        interval_dt = relativedelta(months=+int(interval))
        if end_date_dt > (start_date_dt + interval_dt):
            #return start/end time list for iteration
            mid_date_dt_buf = start_date_dt
            mid_date_dt = vms_tools.conv_str_to_dt(
                '19000101')  #an impossibly early date

            while mid_date_dt < end_date_dt:
                mid_date_dt = mid_date_dt_buf + interval_dt
                date_step = (
                    vms_tools.conv_dt_to_vms_date(mid_date_dt_buf)[0:8],
                    vms_tools.conv_dt_to_vms_date(mid_date_dt)[0:8])
                mid_date_dt_buf = mid_date_dt
                date_list.append(date_step)

        else:
            date_step = (start_date, end_date)
            date_list.append(date_step)
#        print 'There is %s date steps' %(len(date_list))
#        print date_list
        date_list = [
            vms_tools.conv_dt_to_vms_date(start_date_dt),
            vms_tools.conv_dt_to_vms_date(end_date_dt)
        ]

        #automatic mode determination
        if gear_type is '':
            print 'Gear type must be set. Abort.'
            sys.exit(1)
        if mode is '':
            if trans_no is not '':
                mode = 'pick_up'
            else:
                mode = 'run_all'

        if skip_pred is not '':
            skip_pred = True
        if skip_clas is not '':
            skip_clas = True

        if ignr_pend is not '':
            ignr_pend = False

        print '>flow_control<'
        print 'mode      :', mode
        print 'skip_pred :', skip_pred
        print 'skip_clas :', skip_clas
        print 'ignr_pend :', ignr_pend

        #        for step in date_list:
        step = date_list
        self.update_all(
            step[0],
            step[1],
            gear_type,
            mode,
            trans_no=trans_no,
            vessel_name=vessel_name,
            skip_pred=skip_pred,
            skip_clas=skip_clas,
            ignr_pend=ignr_pend)

    def update_all(self,
                   start_date,
                   end_date,
                   gear_type,
                   mode,
                   trans_no=None,
                   vessel_name=None,
                   skip_pred=False,
                   skip_clas=False,
                   ignr_pend=False):
        '''
        This module will update the DB for all vessels.
        '''
        print 'Start updating all'
        print 'start_date :', start_date
        print 'end_date   :', end_date
        print 'gear_type  :', gear_type
        print 'trans_no   :', trans_no
        print 'vessel_name:', vessel_name
        print 'mode       :', mode
        print 'ignr_pend  :', ignr_pend
        print 'skip_pred  :', skip_pred
        print 'skip_clas  :', skip_clas
        vessel_list, hdr = self.get_vessel_list(gear_type=gear_type)
        #        print vessel_list
        #        raw_input()
        trans_no_idx = hdr.index('TRANSMITTER_NO')
        vessel_name_idx = hdr.index('VESSEL_NAME')
        vessel_gear_idx = hdr.index('REGISTERED_GEAR_TYPE')
        #        start_date='20140401'
        #        end_date='20140731'
        #        print vessel_list,hdr
        vessel_list = sorted(vessel_list)
        mode = mode.upper()
        #trans_no=int(trans_no)
        if (trans_no is not None and trans_no != '')\
           and (mode=='PICK_UP' or mode=='RUN_ONE'):
            trans_no = int(trans_no)
            ini_idx_1 = [int(i[0]) for i in vessel_list].index(trans_no)
            #            ini_idx_2=[i[1] for i in vessel_list].index(vessel_name)
            #            if ini_idx_1 != ini_idx_2:
            #                print 'trans_no and vessel_name idx inconsistent'
            #                print ini_idx_1,ini_idx_2
            #take idx from trans_no anyway
            ini_idx = ini_idx_1
            end_idx = len(vessel_list)
            if mode == 'RUN_ONE':
                end_idx = ini_idx + 1
        elif mode == 'RUN_ALL':
            ini_idx = 0
            end_idx = len(vessel_list)
        else:
            ini_idx = 0
            end_idx = len(vessel_list)

        for i in range(ini_idx, end_idx):
            vessel = vessel_list[i]
            print '+============================='
            print '+Now processing(%s/%s): %s|%s' % (str(i + 1),
                                                     str(len(vessel_list)),
                                                     vessel[trans_no_idx],
                                                     vessel[vessel_name_idx])
            print '+============================='
            print ignr_pend
            print not ignr_pend
            #            raw_input()
            self.update_selected(
                vessel[trans_no_idx],
                vessel[vessel_name_idx],
                vessel[vessel_gear_idx],
                start_date=start_date,
                end_date=end_date,
                skip_pred=skip_pred,
                skip_clas=skip_clas,
                include_pending=not ignr_pend)

    def update_selected(self,
                        trans_no,
                        vessel_name,
                        gear_type,
                        start_date,
                        end_date,
                        skip_pred=False,
                        skip_clas=False,
                        include_pending=True,
                        skip_mtch=False):
        '''
        This module will only update the select vessel.
        '''
        #update DB with predicted value
        ##skip for test next module
        ##        if not skip_pred:
        ##            self.update_predict(trans_no,vessel_name,gear_type,start_date,end_date)

        #sys.exit(1) ##test end here
        #update DB with status value
        #set proc=True to take track records from processed DB to include predicted records
        if not skip_clas:
            self.update_status(
                trans_no,
                vessel_name,
                gear_type,
                start_date,
                end_date,
                proc=True,
                include_pending=include_pending)
#        sys.exit(1)
#update DB with landing site
#        self.update_landing(trans_no,vessel_name,start_date,end_date)

#update DB with fishing ground
#        self.update_fising(trans_no,vessel_name,start_date,end_date)

#update DB with VBD match
##        if not skip_mtch:
##            self.update_match(trans_no,vessel_name,gear_type,start_date,end_date)

    def get_vessel_list(self, gear_type=None):
        '''
        This module retrieves vessel list from vessel DB.
        return in the form of {'trans_no',trans_no,'vessel_name':vessel_name}
        '''

        #vessel_list,hdr=vms_tools.get_vessel_list()

        if gear_type is None:
            print 'Gear type must be set. Abort.'
            sys.exit(1)

        vessel_db_psql = vms_ask_path.vessel_db_psql
        conn = psycopg2.connect(vessel_db_psql)
        cur = conn.cursor()

        cur.execute('select * from vessel where registered_gear_type=%s',
                    [gear_type])
        data = cur.fetchall()

        conn.close()

        header = [i[0].upper() for i in cur.description]

        return data, header
        #return vessel_list,hdr

    def update_predict(self, trans_no, vessel_name, gear_type, start_date,
                       end_date):
        '''
        This module updates predicted location at VIIRS overpass for selected vessel
        '''
        print 'trans_no', trans_no
        print vessel_name, start_date, end_date

        trk, hdr = vms_tools.get_vms_track(
            trans_no,
            vessel_name=vessel_name,
            gear_type=gear_type,
            start_date=start_date,
            end_date=end_date)

        if len(trk) == 0:
            print 'No track found. Abort.'
            return

        trk = vms_tools.clean_track(trk, hdr)
        vmsp = vms_predict()
        prd = vmsp.predict(trk, hdr)
        if len(prd) == 0:
            print 'No prediction found. Abort.'
            return
#        print 'prd',prd
#put predicted record in the processed vms db
#add two more columes as place holder for STATUS and LOCALE

        conn = psycopg2.connect(vms_ask_path.vms_proc_db_psql)
        cur = conn.cursor()

        for t in trk:
            trk_list = list(t)
            trk_list.extend(
                [vms_constants.null_val_str, vms_constants.null_val_str])
            year_idx = hdr.index('REPORTDATE')
            year = ''.join(['y', str(trk_list[year_idx])[0:4]])
            #            try:
            cur.execute(
                sql.SQL("insert into {} values (" + ','.join([
                    '%s' for i in trk_list
                ]) + ") on conflict (id_key) do nothing").format(
                    sql.Identifier(year)), [str(i) for i in trk_list])
            conn.commit()
#            except:
#                print 'Insert or ignore error:',trk_list

        for p in prd:
            prd_list = list(p)
            prd_list.extend(
                [vms_constants.null_val_str, vms_constants.null_val_str])

            #if PRD already exists, replace the record with this one
            #vms_proc_db = vms_ask_path.vms_proc_db
            #conn = sqlite3.connect(vms_proc_db)
            year_idx = hdr.index('REPORTDATE')

            year = ''.join(['y', str(prd_list[year_idx])[0:4]])
            if not year.startswith('y20'):
                continue
            #            try:
            #            print prd_list
            #            insert_val=[str[i] for i in prd_list]
            insert_val = prd_list[:]
            insert_val.extend(
                [vms_constants.null_val_str, vms_constants.null_val_str])
            #            print sql.SQL('insert into {tbl} values ({val1}) on conflict (id_key) do update set {sta}={val_sta}, {loc}={val_loc}').format(tbl=sql.Identifier(year),sta=sql.Identifier('status'),loc=sql.Identifier('locale'),val1=sql.SQL(',').join(sql.Placeholder() * len(prd_list)),val_sta=sql.Placeholder(),val_loc=sql.Placeholder()).as_string(conn)
            #            raw_input()
            cur.execute(
                sql.SQL(
                    'insert into {tbl} values ({val1}) on conflict (id_key) do update set {sta}={val_sta}, {loc}={val_loc}'
                ).format(
                    tbl=sql.Identifier(year),
                    sta=sql.Identifier('status'),
                    loc=sql.Identifier('locale'),
                    val1=sql.SQL(',').join(sql.Placeholder() * len(prd_list)),
                    val_sta=sql.Placeholder(),
                    val_loc=sql.Placeholder()), insert_val)
            conn.commit()
#            except:
#                print 'Insert or replace error:',prd_list

        conn.close()

    def update_status(self,
                      trans_no,
                      vessel_name,
                      gear_type,
                      start_date,
                      end_date,
                      proc=False,
                      include_pending=True):
        '''
        This module updates movement status for selected vessel
        In operational, status update should happen after prediction,
        so predicted record can participate in status characterization.
        Nevertheless, the record can always be re-characterized retro-
        spectively.
        '''

        begin_date = vms_tools.get_begin_date(
            trans_no, vessel_name=vessel_name, gear_type=gear_type)

        #if the query start_date is later than track record start date
        #try to pickup pending records
        print include_pending
        if begin_date is None:
            include_pending = False
        else:
            print start_date, begin_date
            include_pending = True if start_date > begin_date else include_pending
        print 'include pending?', include_pending
        #        raw_input()
        trk, hdr = vms_tools.get_vms_track(
            trans_no,
            vessel_name=vessel_name,
            gear_type=gear_type,
            start_date=start_date,
            end_date=end_date,
            proc=proc,
            include_pending=include_pending)
        if trk is None:
            print 'Track retrieval failed.'
            sys.exit(1)

        trk = vms_tools.clean_track(trk, hdr)
        #        trkt = [i for i in trk if i[0].startswith('PRD')]
        #        for i in trkt:
        #            print i
        #        sys.exit(1)

        if len(trk) == 0:
            print 'No track found. Abort.'
            return
        vmsc = vms_characterize()
        status, sgid = vmsc.characterize(trk, hdr)
        #if there is Landing in the returned status, identify the landing site
        #        print status
        site = self.identify_landing(status, trk, hdr)
        #        print site

        #put record in processed vms db
        #vms_proc_db = vms_ask_path.vms_proc_db
        #conn = sqlite3.connect(vms_proc_db)
        vms_proc_db = vms_ask_path.vms_proc_db_psql
        conn = psycopg2.connect(vms_proc_db)
        cur = conn.cursor()
        id_idx = hdr.index('ID_KEY')
        lat_idx = hdr.index('LATITUDE')
        lon_idx = hdr.index('LONGITUDE')
        rpd_idx = hdr.index('REPORTDATE')

        tbuf = None
        for i in range(0, len(trk)):
            t = trk[i]
            s = status[i]
            g = sgid[i]
            l = site[i]
            rec = list(t)
            rec.extend([s, l])  #append record of status and locale
            year = ''.join(['y', str(t[rpd_idx])[0:4]])
            v = vms_constants.null_val_str
            h = vms_constants.null_val_str
            d = vms_constants.null_val_str
            dt = vms_constants.null_val_str  #calculate velocity and heading change
            if not tbuf is None and g != vms_constants.null_val_str:
                d1 = vms_tools.VMS_Time2Julian(
                    tbuf[rpd_idx], format='%Y-%m-%d %H:%M:%S')
                d2 = vms_tools.VMS_Time2Julian(
                    t[rpd_idx], format='%Y-%m-%d %H:%M:%S')
                dt = round(abs(d2 - d1) * 24., 2)
                lat1 = tbuf[lat_idx]
                lon1 = tbuf[lon_idx]
                lat2 = t[lat_idx]
                lon2 = t[lon_idx]
                d = vms_tools.getDist({
                    'x': lon1,
                    'y': lat1
                }, {'x': lon2,
                    'y': lat2}) / 1000.
                d = round(d, 2)
                if dt == 0:
                    v = vms_constants.null_val_str
                else:
                    v = round(d / dt, 2)
                    h1 = vms_tools.getHeading({
                        'x': lon1,
                        'y': lat1
                    }, {'x': lon2,
                        'y': lat2})
                    if i + 1 < len(trk):
                        tt = trk[i + 1]
                        lat3 = tt[lat_idx]
                        lon3 = tt[lon_idx]
                        h2 = vms_tools.getHeading({
                            'x': lon2,
                            'y': lat2
                        }, {'x': lon3,
                            'y': lat3})
                        h = round(h2 - h1, 2)
                    else:
                        h = vms_constants.null_val_str
            else:
                tbuf = t
            #
            #            print 'status,segment,locale,velocity,heading_change,distance,time_elapsed'
            #            print s,g,l,v,h,d,dt
            if s == 'Null':
                print 'status,segment,locale,velocity,heading_change,distance,time_elapsed'
                print s, g, l, v, h, d, dt
                print 'Encountered Null'
                raw_input()

            cur.execute(
                sql.SQL(
                    'update {} set status=%s, segment=%s, locale=%s,velocity=%s,heading_change=%s,distance=%s,time_elapsed=%s where id_key=%s'
                ).format(sql.Identifier(year)),
                [s, g, l, v, h, d, dt, t[id_idx]])

            conn.commit()
        conn.close()

    def update_match(self,
                     trans_no,
                     vessel_name,
                     gear_type,
                     start_date,
                     end_date,
                     proc=False,
                     include_pending=True):
        '''
        This module udpates the VBD match for PRD records.
        '''
        vms_proc_db = vms_ask_path.vms_proc_db_psql
        prdrec, vmshdr = vms_tools.vms_get_prd(
            trans_no,
            start_date,
            end_date,
            vms_proc_db,
            vessel_name=vessel_name,
            gear_type=gear_type)
        conn = psycopg2.connect(vms_proc_db)
        cur = conn.cursor()
        for rec in prdrec:
            idk_idx = vmshdr.index('id_key')
            rpd_idx = vmshdr.index('reportdate')
            lat_idx = vmshdr.index('latitude')
            lon_idx = vmshdr.index('longitude')
            idk = rec[idk_idx]
            lat = rec[lat_idx]
            lon = rec[lon_idx]
            rpd = rec[rpd_idx]
            print 'Searching VBD match:', idk
            vbd_rec, vbdhdr, dtime, dist = vms_tools.find_vbd(
                lat, lon, rpd, time_thr=10, dist_thr=1000)
            #            print vbd_rec
            #            raw_input()

            if len(vbd_rec) is 0:
                print 'No match is found.'
                continue
            #pick the cloest one
            #            print vbd_rec,vbdhdr,dtime,dist
            #            raw_input()
            min_dist_idx = dist.index(min(dist))
            vbd_rec = vbd_rec[min_dist_idx]
            dtime = dtime[min_dist_idx]
            dist = dist[min_dist_idx]
            #update database with vbd id_key and dist/dtime
            vbd_idk_idx = vbdhdr.index('id_key')
            vbd_idk = vbd_rec[vbd_idk_idx]
            print 'Found match:', vbd_idk

            tbl = sql.Identifier('y' + str(rpd.year))  #rpd[0:4])
            cur.execute(
                sql.SQL(
                    'update {tbl} set vbd_id_key=%s, vbd_dist=%s, vbd_dtime=%s where id_key=%s'
                ).format(tbl=tbl), [vbd_idk,
                                    round(dist, 1), dtime, idk])
        conn.commit()
        conn.close()

    def identify_landing(self, status_list, trk, hdr):
        '''
        This module identifies landing site used in the track.
        '''
        landing_site_list = np.chararray(len(status_list), itemsize=30)
        landing_site_list[:] = vms_constants.null_val_str  #'NA'
        #find continuous landing records
        landing_ranges = []
        for key, group in groupby(enumerate(status_list), lambda x: x[1]):
            if key != 'Landing':
                continue
            else:
                landing_ranges.append(np.array([i[0] for i in group]))
        #loop thru each landing group, check for its site
        lat_idx = hdr.index('LATITUDE')
        lon_idx = hdr.index('LONGITUDE')
        lat = np.array([float(i[lat_idx]) for i in trk])
        lon = np.array([float(i[lon_idx]) for i in trk])
        for rng in landing_ranges:
            lat_avg = sum(lat[rng]) / len(rng)
            lon_avg = sum(lon[rng]) / len(rng)
            site = vms_landing_site.query_by_latlon(lat_avg, lon_avg)
            #            print lat_avg,lon_avg,site,len(rng)
            landing_site_list[rng] = site['Name']

#        print landing_site_list
#        sys.exit(1)
        return landing_site_list

    def update_landing(self, trans_no, vessel_name, start_date, end_date):
        '''
        This module updates landing site usage for selected vessel
        '''
        #the landing site update is taken care by status characterization
        #it's now back to independent process to utilize postgis --20170801 David
        vms_tools.update_landing(trans_no, vessel_name, start_date, end_date)

    def udpate_fishing(self, trans_no, vessel_name, start_date, end_date):
        '''
        This module updates fishing ground usage for selected vessel
        '''
        #fishing ground update will not be available until fishing gound list is available

    #===============================================================
if __name__ == '__main__':
    vms_pred_n_char()
