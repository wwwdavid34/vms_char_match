#!/usr/bin/env python2.7

import os,sys,sqlite3,math,time
import vms_ask_path,vms_constants
import datetime as dt
from sgp4.ext import jday, invjday
import numpy as np
import ConfigParser
import psycopg2
from psycopg2 import sql

'''
This program collects some useful tools which 
are used across all purposes.

Created by: David -20170317
'''

def parse_config(config_file):
    '''
    Parse ini type config file.
    '''
    config = ConfigParser.ConfigParser()
    config.read(config_file)
    cfg = {}
    #get sections
    for section in config.sections():
        try:
            cfg[section]={}
        except:
            cfg[section]=None
            continue
        for option in config.options(section):
            try:
                cfg[section][option]=config.get(section,option)
            except:
                cfg[section][option]=None
                continue
    return cfg
        
def conv_str_to_dt(date,delta=0):
    '''
    convert input date YYMMDD to datetime object
    '''
    return conv_vms_date_to_dt(conv_str_to_vms_date(date))

def conv_str_to_vms_date(date,delta=0):
    '''
    convert input date YYMMDD string to YYYYMMDDhhmmss
    Expecting YYYYMMDD
    '''
    dtobj=dt.datetime.strptime(date[0:8],'%Y%m%d')
    dtstr=conv_dt_to_vms_date(dtobj,delta=delta)
#    dtstr=(dtobj+dt.timedelta(delta)).strftime('%Y%m%d%H%M%S')
    return dtstr

def conv_dt_to_vms_date(dtobj,delta=0):
    '''
    convert input datetime object to YYYYMMDDhhmmss
    '''
    dtstr=(dtobj+dt.timedelta(delta)).strftime('%Y%m%d%H%M%S')
    return dtstr
    
def conv_vms_date_to_dt(date,delta=0):
    '''
    convert vms date string to python datetime object
    '''
    return dt.datetime.strptime(date,'%Y%m%d%H%M%S')+dt.timedelta(delta)

def conv_psql_date_to_dt(date,delta=0):
    '''
    convert psql date string to python datetime object
    '''
    return dt.datetime.strptime(date,'%Y-%m-%d %H:%M:%S')+dt.timedelta(delta)

def conv_dt_to_psql_date(dtobj,delta=0):
    '''
    convert input datetime object to psql format
    '''
    return (dtobj+dt.timedelta(delta)).strftime('%Y-%m-%d %H:%M:%S')


def diff_dt(t1,t2,absolute=False):
    '''
    assumes t2>t1
    return in seconds
    '''
    if not (isinstance(t1,dt.datetime) and
            isinstance(t2,dt.datetime)):
        print 't1 and t2 must be datetime.datetime instance'
        return None
    pm=1 if t2>t1 else -1
    pm=abs(pm) if absolute else pm
    diff=t2-t1
    return pm*diff.total_seconds()

def update_proc_boat_info_all():
    '''
    This module updates boat_info for all boats in annual tables
    Calls update_prod_boat_info
    '''

    vms_db = vms_ask_path.vms_proc_db
    
    conn = sqlite3.connect(vms_db)
    pntr = conn.execute('SELECT * FROM sqlite_master WHERE type=="table"')
#    a = pntr.fetchall()
    tbl_list = [i[1] for i in pntr.fetchall() if i[1].startswith('20')] #ignore boat_list table
    boat_list = []
    print 'Retrieving all existing boats...'
    for tbl in tbl_list:
        print 'Looking in table:',tbl
        pntr = conn.execute('SELECT TRANSMITTER_NO,VESSEL_NAME, GROSS_TONNAGE, LENGTH, WIDTH, REGISTERED_GEAR_TYPE FROM "'+str(tbl)+'"')
        boat_list.extend(list(set(pntr)))
    boat_list = list(set(boat_list))

    nboat=len(boat_list)
    cnt=0
    for boat in boat_list:
        cnt+=1
        print 'Updating boat_info (%s/%s):%s|%s|%s' %(cnt,nboat,boat[0],boat[1],boat[5])
        update_proc_boat_info(boat[0],boat[1],boat[2],boat[3],boat[4],boat[5])
    


def update_proc_boat_info(trans_no,vessel_name,tonnage,length,width,gear_type):
    '''
    This module updates boat_info for
    (1) Currently having boats in proc db
    (2) The start/end time of record for each boat in proc db
    '''

    vms_db = vms_ask_path.vms_proc_db

    conn = sqlite3.connect(vms_db)
    pntr = conn.execute('SELECT * FROM sqlite_master WHERE type=="table"')
    tbl_list = [i[1] for i in pntr.fetchall() if i[1].startswith('20')] #ignore boat_list table
    maxdate = 11110000000000 #very small date
    mindate = 99990000000000 #very large date

    for tbl in tbl_list:
        pntr = conn.execute('SELECT MAX(REPORTDATE),MIN(REPORTDATE) FROM "'+str(tbl)+'" WHERE TRANSMITTER_NO=="'+str(trans_no)+'"AND VESSEL_NAME=="'+str(vessel_name)+'"')
        maxdt,mindt = pntr.fetchall()[0]
        maxdate = maxdt if maxdt>maxdate else maxdate
        mindate = mindt if (mindt<mindate and mindt is not None) else mindate

    info = [trans_no,vessel_name,tonnage,length,width,gear_type,mindate,maxdate]
    pntr = conn.execute('INSERT OR REPLACE INTO boat_info VALUES ('+','.join(['"'+str(i)+'"' for i in info])+')')
    conn.commit()
    conn.close()

def get_begin_date(trans_no,vessel_name=None,
                   tonnage=None,length=None,
                   width=None,gear_type=None):
    '''
    This module retrieves begin date of vessel record from boat_info table in proc db
    '''
    
    vms_db = vms_ask_path.vms_proc_db_psql

#    conn = sqlite3.connect(vms_db)
    conn = psycopg2.connect(vms_db)
    cur=conn.cursor()
    #    sql_cmd = 'SELECT * FROM boat_info WHERE TRANSMITTER_NO=="'+str(trans_no)+'"'
    sql_cmd = 'select * from boat_info where {tran} = %s'
    add_val=[trans_no]
    tran_iden=sql.Identifier('transmitter_no')
    vesl_iden=None
    tonn_iden=None
    leng_iden=None
    widt_iden=None
    gear_iden=None
    if vessel_name is not None:
        sql_cmd+=' and {vesl} = %s'
        vesl_iden=sql.Identifier('vessel_name')
        add_val.append(vessel_name)
    if tonnage is not None:
        sql_cmd+=' and {tonn} = %s'
        tonn_iden=sql.Identifier('gross_tonnage')
        add_val.append(tonnage)
    if length is not None:
        sql_cmd+=' and {leng} = %s'
        leng_iden=sql.Identifier('length')
        add_val.append(length)
    if width is not None:
        sql_cmd+=' and {widt} = %s'
        widt_iden=sql.Identifier('width')
        add_val.append(width)
    if gear_type is not None:
        sql_cmd+=' and {gear} = %s'
        gear_iden=sql.Identifier('registered_gear_type')
        add_val.append(gear_type)

    cur.execute(sql.SQL(sql_cmd).format(tran=tran_iden,
                                        vesl=vesl_iden,
                                        gear=gear_iden,
                                        tonn=tonn_iden,
                                        widt=widt_iden,
                                        leng=leng_iden).as_string(conn),add_val)

    rec=cur.fetchall()
    hdr  = [i[0].upper() for i in cur.description]
    conn.close()
    if len(rec)>1:
        print 'Found more than one boat, please refine query.'
        for i in rec:
            print rec
        return None
    elif len(rec)==0:
        print 'Found no boat for previous records.'
        return None
    else:
        print rec[0]
        inidt_idx = hdr.index('START_DATE')
        return rec[0][inidt_idx]

def get_ais_vessel_list():
    '''
    This module returns list of boats with specified requirements
    '''
    from google.cloud import bigquery as bq
    client=bq.Client()
    query=client.run_sync_query('''
    SELECT
      mmsi
    FROM (
      SELECT
        mmsi,count(*) count
      FROM
        `skytruth-vms.GFW_AIS.AIS_20170603`
      GROUP BY
        mmsi
      ORDER BY
        count DESC
    ) WHERE 
    count>=400
    ''')
    query.use_legacy_sql = False
    query.run()
    vessel_list=[row[0] for row in query.rows]
    return vessel_list
                                
    
def get_ais_track(trans_no,start_date=None,end_date=None):
    '''
    This module serves as a hack to retrieve AIS track from Google BQ.
    '''
    from google.cloud import bigquery as bq
    client=bq.Client()
    if start_date is not None:
        stdt=start_date[2:8]
        if end_date is None:
            print 'Start date and end date must both be set.'
            sys.exit(1)
    if end_date is not None:
        eddt=end_date[2:8]
        if start_date is None:
            print 'Start date and end date must both be set.'
            sys.exit(1)
    if start_date is None and end_date is None:
        stdt='170101'
        eddt='170110'
    cmd='''
    SELECT DISTINCT
      *
    FROM
      `skytruth-vms.GFW_AIS.20*`
    WHERE 
      _table_suffix BETWEEN \''''+stdt+'''\' AND \''''+eddt+'''\'
      AND mmsi='''+str(trans_no)+'''
      AND lat IS NOT null
    ORDER BY
      timestamp
    '''
    print cmd
    query=client.run_sync_query(cmd)
    query.use_legacy_sql = False
    start_time=dt.datetime.now()
    
    query.run()

    end_time=dt.datetime.now()
    elapsed_time=end_time-start_time
    print 'Used %s seconds' % elapsed_time.seconds
    print 'Returned %s rows' % len(query.rows)
    
    header=['transmitter_no','latitude','longitude','reportdate','seg_id']
    header=[i.upper() for i in header]

    if query.complete:
        track=[row for row in query.rows]
        return track,header
    else:
        print 'query.complete',query.complete
        print query.errors
        print cmd.replace('\n',' ').replace('\t','')
        print 'Query failed. Wait 60 seconds and try again'
        for i in range(0,6):
            print (i+1)*10
            time.sleep(10)
#        time.sleep(60)
#        raw_input()
        query=client.run_sync_query(cmd)
        query.use_legacy_sql=False
        query.run()
        if query.complete:
            track=[row for row in query.rows]
        else:
            print 'query.complete',query.complete
            print query.errors
            print cmd.replace('\n',' ').replace('\t','')
            print 'Query failed.'
            track=[]
        return track,header
    

    
def get_vms_track(trans_no,vessel_name=None,gear_type=None,
                  tonnage=None,width=None,length=None,
                  start_date='20140101',end_date='20161231'
                  ,proc=False,clean=True,strict=True,include_pending=False):
    '''
    This module retrieves VMS track for selected vessel
    within specified date range.
    '''

    #find previous continuous pending records segment if include_pending
    ##override settings if include_pending
    proc = True if include_pending else proc
    strict = True if include_pending else strict
#    start_date_orig = start_date #backup user input start_date

    if proc:
        #processed VMS DB
#        vms_db=vms_ask_path.vms_proc_db
        vms_db=vms_ask_path.vms_proc_db_psql
    else:
        #raw VMS DB to be predicted/characterized/matched...
#        vms_db=vms_ask_path.vms_db
        vms_db=vms_ask_path.vms_db_psql
    print 'vms_db',vms_db

    ##override start_date setting
    if include_pending:
        if vessel_name is None:
            print 'vessel_name is missing to retrieve prccessed record start date.'
            sys.exit(1)
#        conn = sqlite3.connect(vms_db)
#        pntr = conn.execute('SELECT START_DATE FROM boat_info WHERE TRANSMITTER_NO=="'+str(trans_no)+'" AND VESSEL_NAME=="'+str(vessel_name)+'"')
#        a = pntr.fetchall()
        conn = psycopg2.connect(vms_db)
        cur  = conn.cursor()
        cur.execute('select start_date from boat_info where transmitter_no=%s and vessel_name=%s',[str(trans_no),str(vessel_name)])
        a=cur.fetchall()
        conn.close()
        if len(a)>1:
            print 'Retrived more than one record.'
            print a
            sys.exit(1)
        else:
            start_date_pending=a[0][0] #start date for pending retrieval
        #cancel pending retrieval if start_date_pending is unreasonable for current setting
        if int(start_date_pending) > int(conv_str_to_vms_date(start_date)):
            print '!!Cancel pending retrieval.!!'
            include_pending=False
    if strict:
        start_delta=0
        end_delta=1
    else:
        start_delta=-0.5
        end_delta=1.5
    start_delta=0
    end_delta=0

    #eapand +/- 0.5 days to cover leading/tailing track
    print 'start raw',start_date
    print 'end raw',end_date
    start_date=conv_str_to_vms_date(start_date,delta=start_delta)
    end_date=conv_str_to_vms_date(end_date,delta=end_delta)
    print 'start_date',start_date
    print 'end_date',end_date

    track,header=vms_get_track_core_psql(trans_no,
                                         start_date,end_date,vms_db,
                                         vessel_name=vessel_name,
                                         gear_type=gear_type,
                                         tonnage=tonnage,
                                         width=width,length=length)
    
    
    if track is None:
        return [None,None]

    ##find previous pending segments and add them to the track
    if include_pending:
        print 'Retrieving pending records...'
        print 'start_date_pending',start_date_pending
        print 'end_date_pending',start_date
        track_prev,header=vms_get_track_core_psql(trans_no,
                                                  start_date_pending,start_date,vms_db,
                                                  vessel_name=vessel_name,
                                                  gear_type=gear_type,
                                                  tonnage=tonnage,
                                                  width=width,length=length)
        if track_prev is None:
            return [None,None]

        if len(track_prev)==0:
            print 'No previous track was found.'
        else:
            tid = header.index('REPORTDATE')
            track_prev=sorted(track_prev,key=lambda t:t[tid])
            #only include the last continuous pending
            track_prev.reverse()
            pending=False
            #append pending track from the latest record
            #include null predicted records as well
            #if there is a record with valid status, the search will be terminated
            sid = header.index('STATUS')
            print 'track_prev',len(track_prev)

            for t in track_prev:
                print t[0],t[sid]
                if t[sid] == 'Pending' or t[sid] == vms_constants.null_val_str:
                    print 'Pending:',t
                    track.append(t)
        #            break
                else:
                    break
#        for i in track:
#            print i
#    print 'len(track)',len(track)
#    sys.exit(1)
        

    if len(track)==0:
        print 'WARNING: No track record is found.'
        return [],header
    else:
        #sort track by time
        tid = header.index('REPORTDATE')
        track_sort=sorted(track,key=lambda t:t[tid])

        if clean:
            return clean_track(track_sort,header),header
        else:
            return track_sort,header

def vms_get_track_core_psql(trans_no,start_date,end_date,vms_db,
                            vessel_name=None,gear_type=None,
                            tonnage=None,width=None,length=None):
    '''
    This is the corepart of retrieving vms track
    implemented in postgresql
    '''

    if int(start_date) > int(end_date):
        print 'start_date > end_date, abort.'
        return [None,None]

    conn=psycopg2.connect(vms_db)
    cur =conn.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    retn=cur.fetchall()
    table_list_raw=[i[0] for i in retn if i[0].startswith('y20')]

    #year list
    table_list=['y'+str(i) for i in range(int(str(start_date)[0:4]),
                                      int(str(end_date)[0:4])+1) if 'y'+str(i) in table_list_raw]
    table_list=['vms_proc']
    print 'table_list',table_list

    track=[]
    for table in table_list:

        sql_cmd = ('select * from {tbl} where {tran} = %s and {rpdt} >= %s and {rpdt} <= %s')
        tbl_iden=sql.Identifier(table)
        tran_iden=sql.Identifier('transmitter_no')
        rpdt_iden=sql.Identifier('reportdate')
        start_date='-'.join([start_date[0:4],start_date[4:6],start_date[6:8]])
        end_date='-'.join([end_date[0:4],end_date[4:6],end_date[6:8]])
        add_val=[trans_no,start_date,end_date]
        vesl_iden=None
        gear_iden=None
        tonn_iden=None
        leng_iden=None
        widt_iden=None
        if vessel_name is not None:
            sql_cmd+=' and {vesl} = %s'
            vesl_iden=sql.Identifier('vessel_name')
            add_val.append(vessel_name)
        if gear_type is not None:
            sql_cmd+=' and {gear} = %s'
            gear_iden=sql.Identifier('registered_gear_type')
            add_val.append(gear_type)
        if tonnage is not None:
            sql_cmd+=' and {tonn} = %s'
            tonn_iden=sql.Identifier('gross_tonnage')
            add_val.append(tonnage)
        if width is not None:
            sql_cmd+=' and {widt} = %s'
            widt_iden=sql.Identifier('width')
            add_val.append(width)
        if length is not None:
            sql_cmd+=' and {leng} = %s'
            leng_iden=sql.Identifier('length')
            add_val.append(length)

        cur.execute(sql.SQL(sql_cmd).format(tbl=tbl_iden,
                                            tran=tran_iden,
                                            vesl=vesl_iden,
                                            rpdt=rpdt_iden,
                                            gear=gear_iden,
                                            tonn=tonn_iden,
                                            widt=widt_iden,
                                            leng=leng_iden).as_string(conn),add_val)

        retn=cur.fetchall()
        print 'Number of found track',len(retn)
        if len(retn)>0:
            track.extend(retn)

    header=[i[0].upper() for i in cur.description]
    conn.close()
    return track,header

    
def vms_get_track_core(trans_no,start_date,end_date,vms_db,
                       vessel_name=None,gear_type=None,
                       tonnage=None,width=None,length=None):
    '''
    This is the core part of retrieving vms track
    '''

    if int(start_date) > int(end_date):
        print 'start_date > end_date, abort.'
        return [None,None]

    conn=sqlite3.connect(vms_db)
    pntr=conn.execute('SELECT * FROM sqlite_master WHERE type=="table"')
    retn=pntr.fetchall()
    table_list_raw=[i[1] for i in retn if i[1].startswith('20')]

    #year list
    table_list=[str(i) for i in range(int(str(start_date)[0:4]),
                                      int(str(end_date)[0:4])+1) if str(i) in table_list_raw]

    print 'table_list',table_list

    track=[]
    for table in table_list:

        sql_cmd = ('SELECT * FROM "'+str(table)+
                   '" WHERE TRANSMITTER_NO == "'+str(trans_no)+
                   '" AND REPORTDATE >= "'+str(start_date)+
                   '" AND REPORTDATE <= "'+str(end_date)+'"')

        if vessel_name is not None:
            sql_cmd+=' AND VESSEL_NAME == "'+str(vessel_name)+'"'
        if gear_type is not None:
            sql_cmd+=' AND REGISTERED_GEAR_TYPE == "'+str(gear_type)+'"'
        if tonnage is not None:
            sql_cmd+=' AND GROSS_TONNAGE == "'+str(tonnage)+'"'
        if width is not None:
            sql_cmd+=' AND WIDTH == "'+str(width)+'"'
        if length is not None:
            sql_cmd+=' AND LENGTH == "'+str(length)+'"'
            
        pntr = conn.execute(sql_cmd)
        print sql_cmd

        retn=pntr.fetchall()

        print 'Number of found track',len(retn)
        if len(retn) > 0:
            track.extend(retn)

    header=[i[0] for i in pntr.description]

    return track,header

def vms_track_normalize(track,hdr,time_span=1):
    '''
    Normalize track bin to 60min.
    time_span can be set in hours.
    '''
    if len(track) < 2:
        print 'Number of track records has to be larger than 1.'
        return None

    rpd_id=hdr.index('REPORTDATE')
    lat_id=hdr.index('LATITUDE')
    lon_id=hdr.index('LONGITUDE')
    tno_id=hdr.index('TRANSMITTER_NO')

    tno = track[0][tno_id]

    lat_list=[float(i[lat_id]) for i in track]
    lon_list=[float(i[lon_id]) for i in track]

    rpd_list=[i[rpd_id] for i in track]
#    dt_list=[conv_vms_date_to_dt(str(i)) for i in rpd_list]
    dt_list=[conv_psql_date_to_dt(str(i)) for i in rpd_list]
    start_dt = dt_list[0]
    end_dt   = dt_list[-1]
    #how many hours?
    if end_dt < start_dt:
        print 'Start time larger than end time.'
        print 'start_dt',start_dt
        print 'end_dt',end_dt
        return None

    #delta hour-1 to the end bin
    #range must covers +/- one more hour
    pt_start = dt.datetime(start_dt.year,start_dt.month,start_dt.day,
                           start_dt.hour)-dt.timedelta(seconds=3600)
    pt_list=[]
    d=0
    while pt_start+dt.timedelta(seconds=int(d*3600)) <= end_dt+dt.timedelta(seconds=3600):
        pt_list.append(pt_start+dt.timedelta(seconds=int(d*3600)))
        d+=1

    #interpolate/extrapolate location at each hour
    ##find 2 trk closest track records
    norm_trk=[]
    cnt=0
    for p in pt_list:
        cnt+=1
        pj = dt2julian(p)
#        pdt_orig = [dt2julian(d)-pj for d in dt_list]
        pdt_list = [(i,abs(dt2julian(d)-pj)) for (i,d) in enumerate(dt_list)]
        pdt_sort = sorted(pdt_list,key=lambda pdt:pdt[1])
#        print '*===='
#        print dt_list
#        print pdt_sort

        min1_id = pdt_sort[0][0]
        min2_id = pdt_sort[1][0]
#        print 'min1,min2',min1_id,min2_id

        lat1 = lat_list[min1_id]
        lon1 = lon_list[min1_id]
        lat2 = lat_list[min2_id]
        lon2 = lon_list[min2_id]
        rpd1 = dt_list[min1_id]
        rpd2 = dt_list[min2_id]
        rpj1 = dt2julian(rpd1)
        rpj2 = dt2julian(rpd2)

        #if two records are away for > 2hr, skip and wait until 
        #a new pj suffices the term



        tot = abs(rpj1-rpj2)
        dis = abs(pj-rpj2)

        latp = ((lat1-lat2)*dis/tot)+lat2
        lonp = ((lon1-lon2)*dis/tot)+lon2
        dtp = conv_dt_to_vms_date(p)
#        print '==='
#        print 'cnt,dtp',cnt,dtp
#        print 'min1_id,min2_id',min1_id,min2_id
#        print 'pj,rpj1,rpj2',pj,rpj1,rpj2
#        print 'dis,tot',dis,tot
#        print 'lat1,lat2,latp',lat1,lat2,latp
#        print 'lon1,lon2,lonp',lon1,lon2,lonp

        if pdt_sort[0][1] < 2*3600./86400.:
        
            dtp = conv_dt_to_vms_date(p)
        #LOCATION consists of min1_id and min2_id gives clue for blowing hourly bin back to original
            norm_trk.append((tno,latp,lonp,dtp,'|'.join([str(min1_id),str(min2_id)])))
        else:
            continue
    return norm_trk,['TRANSMITTER_NO','LATITUDE','LONGITUDE','REPORTDATE','LOCATION']

def vms_get_prd(trans_no,start_date,end_date,vms_db,
                vessel_name=None,gear_type=None,
                tonnage=None,width=None,length=None):
    '''
    This is the module for retrieving the predicted records.
    '''
    if int(start_date) > int(end_date):
        print 'start_date > end_date, abort.'
        return [None,None]

    conn=psycopg2.connect(vms_db)
    cur=conn.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    retn=cur.fetchall()
    table_list_raw=[i[0] for i in retn if i[0].startswith('y20')]

    #year list
    table_list=['y'+str(i) for i in range(int(str(start_date)[0:4]),
                                          int(str(end_date)[0:4])+1) if 'y'+str(i) in table_list_raw]

    print 'table_list',table_list

    rec=[]
    for table in table_list:

        sql_cmd = ("select * from {tbl} where {idk} like %s and {tran}=%s and {rpdt}>=%s and {rpdt}<=%s")
        idk_iden = sql.Identifier('id_key')
        tbl_iden = sql.Identifier(table)
        tran_iden= sql.Identifier('transmitter_no')
        rpdt_iden= sql.Identifier('reportdate')
        start_date_q='-'.join([start_date[0:4],start_date[4:6],start_date[6:8]])+' '+':'.join([start_date[8:10],start_date[10:12],start_date[12:14]])
        end_date_q='-'.join([end_date[0:4],end_date[4:6],end_date[6:8]])+' '+':'.join([end_date[8:10],end_date[10:12],end_date[12:14]])
        add_val  = ['PRD%',trans_no,start_date_q,end_date_q]
        vesl_iden= None
        gear_iden= None
        tonn_iden= None
        leng_iden= None
        widt_iden= None
        if vessel_name is not None:
            sql_cmd+=' and {vesl} = %s'
            vesl_iden = sql.Identifier('vessel_name')
            add_val.append(vessel_name)
        if gear_type is not None:
            sql_cmd+=' and {gear} = %s'
            gear_iden = sql.Identifier('registered_gear_type')
            add_val.append(gear_type)
        if tonnage is not None:
            sql_cmd+=' and {tonn} = %s'
            tonn_iden = sql.Identifier('gross_tonnage')
            add_val.append(tonnage)
        if width is not None:
            sql_cmd+=' and {widt} = %s'
            widt_iden = sql.Identifier('width')
            add_val.append(width)
        if length is not None:
            sql_cmd+=' and {leng} = %s'
            leng_iden = sql.Identifier('length')
            add_val.append(length)


        cur.execute(sql.SQL(sql_cmd).format(tbl=tbl_iden,
                                            idk=idk_iden,
                                            tran=tran_iden,
                                            vesl=vesl_iden,
                                            rpdt=rpdt_iden,
                                            gear=gear_iden,
                                            tonn=tonn_iden,
                                            widt=widt_iden,
                                            leng=leng_iden),add_val)
        retn=cur.fetchall()
        print 'Number of found predictions',len(retn)
        if len(retn)>0:
            rec.extend(retn)
        header=[i[0] for i in cur.description]
        conn.close()
        return rec,header

def find_vbd(lat,lon,rpd,time_thr=10,dist_thr=1000):
    '''
    find vbd record for given lat/lon/time
    time_thr: time difference threshold (sec)
    dist_thr: distance threshold (m)
    '''
    #rpd format=yyyymmddhhmmss
    vbd_db=vms_ask_path.vms_proc_db_psql
    conn=psycopg2.connect(vbd_db)
    cur=conn.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    retn=cur.fetchall()
    table_list_raw=[i[0] for i in retn if i[0].startswith('y20')]
    table='d'+str(rpd)[0:4] #use annual tables -David 20170728 #+str(rpd)[0:6]
    date=str(rpd)[0:8]
    time=str(rpd)[8:14]
    #rough spatially search with postgis, then precise spatial and temporal search with python
    dist_thr_deg=conv_meter_to_deg(dist_thr,lat)*1.1 #increase thr by 10% for safety
    cur.execute(sql.SQL("select * from {tbl} where ST_DWithin(geom, ST_SetSRID(ST_MakePoint(%s,%s),4326),%s)").format(tbl=sql.Identifier(table)),[lon,lat,dist_thr_deg])
    retn=cur.fetchall()
    vbd_hdr=[i[0] for i in cur.description]
    conn.close()
#    print retn
    if len(retn)==0:
#        print 'Found no spatially approximate VBD'
        return [],vbd_hdr,[],[]
    recin=[]
    dtimein=[]
    distin=[]
    for rec in retn:
        #check if time/distance is within temporal/spatial threshold
        idk_idx=vbd_hdr.index('id_key')
        lat_idx=vbd_hdr.index('lat_dnb')
        lon_idx=vbd_hdr.index('lon_dnb')
        reclat=float(rec[lat_idx])
        reclon=float(rec[lon_idx])
        recdate=rec[idk_idx].split('_')[2][1:9]
        rectime=rec[idk_idx].split('_')[3][1:7]
        vbddt=conv_vms_date_to_dt(recdate+rectime)
        vmsdt=conv_vms_date_to_dt(str(rpd).replace('-','').replace(':','').replace(' ',''))
        dtsec=diff_dt(vmsdt,vbddt)
        dist=getDist({'x':lon,'y':lat},
                     {'x':reclon,'y':reclat})
        if dist<dist_thr and dtsec<time_thr:
#            print round(dist,2),dtsec
#            print rec
            recin.append(rec)
            dtimein.append(dtsec)
            distin.append(dist)
    return recin,vbd_hdr,dtimein,distin
        
def conv_meter_to_deg(dist,lat):
    '''
    rough conversion of meter to degree at given latitude
    '''
    dist=float(dist)
    lat=float(lat)
    earth_r=6371000 #m
    rp = earth_r*math.cos(lat*math.pi/180.)
    deg=dist/2/math.pi/rp*360.
    return deg
        
def vms_track_to_csv(track,hdr,dest):
    '''
    write track to CSV file
    '''
    f=open(dest,'wb')
    f.write(','.join(hdr)+'\n')
    for i in track:
        f.write(','.join([str(t) for t in i])+'\n')

def write_to_file(line,dest,append=False,newline=True):
    mode='ab' if append else 'wb'
    f=open(dest,mode)
    f.write(line+'\n')
    f.close

def find_tz_ez(lon):
    '''
    This module finds crude time zone with 30 degree stripe
    '''
    import math
    lon=float(lon)
    abs_lon=abs(abs(lon)-7.5)
    nz=math.ceil(abs_lon/15) if abs(lon) > 7.5 else 0
    return nz if lon > 0 else -nz
    
def time2loc(reportdate,lon):
    '''
    Convertes Skytruth VMS report date to local time using longitude
    '''
    tz=find_tz_ez(lon)
    reportdate=reportdate[0:19]
    rpt_dt=conv_psql_date_to_dt(reportdate,delta=tz/24.)
    return rpt_dt

def dt2julian(dtobj):
    return jday(dtobj.year, dtobj.month, dtobj.day,
                dtobj.hour, dtobj.minute, dtobj.second)
def julian2dt(julday):
    return invjday(inJul)

def VMS_Time2Julian(reportdate,format='%Y%m%d%H%M%S'):
    tFormat=format#"%d-%b-%Y %H:%M:%S"
    
#    reportdate=str(reportdate)
    try: #see if it is a list
        for i in repodatdate:
            if type(i)==dt.datetime:
                a=i
            else:
#                a=time.strptime(i,tFormat)
                a=dt.datetime.strptime(i,tFormat)
            aJul=(jday(a.year,a.mon,a.month,a.day,
                       a.hour,a.minute,a.second))
#            aJul=(jday(a.tm_year, a.tm_mon, a.tm_mday,
#                           a.tm_hour, a.tm_min, a.tm_sec))
            
            try:
                out.append(aJul)
            except:
                out=[aJul]
        return out
    except: #a single shot
        if type(reportdate)==dt.datetime:
            a=reportdate
        else:
            a=time.strptime(reportdate,tFormat)
            a=dt.datetime.strptime(reportdate,tFormat)
        return jday(a.year,a.month,a.day,
                    a.hour,a.minute,a.second)
#        return jday(a.tm_year, a.tm_mon, a.tm_mday,
#                    a.tm_hour, a.tm_min, a.tm_sec)

    
def VMS_Julian2Time(inJul,format='%Y%m%d%H%M%S'):

    tFormat=format#"%d-%b-%Y %H:%M:%S"
    try: #see if it is a list
        for i in inJul:
            a=invjday(i)
            aTime=dt.datetime.timetuple(dt.datetime(a[0],a[1],a[2],a[3],a[4],int(a[5])))
            aStr=time.strftime(tFormat,aTime)
            try:
                out.append(aStr)
            except:
                out=[aStr]
        return out
    except: #a single shot
        a=invjday(inJul)
        aTime=dt.datetime.timetuple(dt.datetime(a[0],a[1],a[2],a[3],a[4],int(a[5])))
        return time.strftime(tFormat,aTime)

def getHeading(p1,p2,ais=True,precise=False):
    '''
    http://www.movable-type.co.uk/scripts/latlong.html
    Set ais==True to report 0~359 degree heading
    Otherwise 0~180,-179~-1
    Set precise==True to report floating point
    '''
    x1=float(p1['x'])/180.*math.pi
    y1=float(p1['y'])/180.*math.pi
    x2=float(p2['x'])/180.*math.pi
    y2=float(p2['y'])/180.*math.pi
    y=math.sin(x2-x1)*math.cos(y2)
    x=math.cos(y1)*math.sin(y2)-\
        math.sin(y1)*math.cos(y2)*math.cos(x2-x1)
    hdn=math.atan2(y,x)*180./math.pi

    if ais:
        hdn = hdn if hdn>=0 else hdn+360.
#    if not precise:
    return int(hdn) if not precise else hdn
 
def getDegDist(p1,p2):
    '''
    get distance in degrees. simple.
    '''
    return math.sqrt((p1['x']-p2['x'])**2+
                     (p1['y']-p2['y'])**2)

def getDist(p1,p2):
    '''
    http://www.movable-type.co.uk/scripts/latlong.html
    Report in Meters
    '''
    R = 6871e3 #meters
    x1=float(p1['x'])/180.*math.pi
    y1=float(p1['y'])/180.*math.pi
    x2=float(p2['x'])/180.*math.pi
    y2=float(p2['y'])/180.*math.pi
    dx=x2-x1
    dy=y2-y1
    a=math.sin(dy/2)*math.sin(dy/2)+\
        math.cos(y1)*math.cos(y2)*\
        math.sin(dx/2)*math.sin(dx/2)
    c=2*math.atan2(math.sqrt(a),math.sqrt(1-a))
    d=R*c
    return d

def clean_track(trk,hdr):
    '''
    Remove errorneous record from track,
    checking by 
    (1) see if it is moving too fast, and
    (2) see if having any wierd coordinate
    (3) add one second if date is yyyymmdd000000 to prevent R error
    '''

    lat_idx=hdr.index('LATITUDE')
    lon_idx=hdr.index('LONGITUDE')
    rpt_idx=hdr.index('REPORTDATE')
    
    sorted(trk, key=lambda rec:rec[rpt_idx])
    print 'Before cleaning:',len(trk)

    #pick out errorneous coordinates
    trk=[t for t in trk if 
         (float(t[lat_idx])>=-90. and float(t[lat_idx])<=90.) and
         (float(t[lon_idx])>=-180. and float(t[lon_idx])<=180.)]

    ret_trk=[]
    rec_buff=[]
    for cnt in range(0,len(trk)):
#        print cnt
        rec=trk[cnt]
        if str(rec[rpt_idx]).endswith('000000'):
            rec_list=list(rec)
            rec_list[rpt_idx]=int(str(rec[rpt_idx])[0:8]+'000001')
            rec=tuple(rec_list)
            
#        print 'REC:',rec
#        print 'BUF:',rec_buff
        if cnt==0:
            rec_buff=rec        #initilize. update buffer
            ret_trk.append(rec) 
        else:
            dist=getDist({'x':float(rec_buff[lon_idx]),
                          'y':float(rec_buff[lat_idx])},
                         {'x':float(rec[lon_idx]),
                          'y':float(rec[lat_idx])})
#            a=conv_vms_date_to_dt(str(rec_buff[rpt_idx]))
#            b=conv_vms_date_to_dt(str(rec[rpt_idx]))
            a=conv_psql_date_to_dt(str(rec_buff[rpt_idx]))
            b=conv_psql_date_to_dt(str(rec[rpt_idx]))
#            print 'a==b',a==b
#            print a,b,(a-b) if a>b else (b-a)
            if a==b:
#                print '===Time Collision.'
#                print 'BUF:',rec_buff
#                print 'CUR:',rec
#                print a,b
#                print '+++'
                continue
            dtime=(a-b) if a>b else (b-a)
            
            speed=(dist/1000./1.8)/(dtime.total_seconds()/3600.)

            if speed < 30.:
                ret_trk.append(rec)
                rec_buff=rec        #passed. update buffer
#            else:
#                print '==='
#                print 'dist,dtime,speed',dist,dtime.total_seconds(),speed
#                print 'BUF:',rec_buff
#                print 'FLT:',rec
#                print 'SPD:',dist,dtime.seconds,speed
#                print '+++'
                

    print 'After cleaning:',len(ret_trk)
#    sys.exit(1)

    return ret_trk

def dist2coast(lat,lon):
    '''
    This module reads nasa distfromcoast LUT
    '''
    d2c_db=vms_ask_path.dist2coast_db

    #round lat/lon to nearest 0.04 bin
    latbin=np.arange(8998,-8998,-4)/100.
    lonbin=np.arange(17898,-17898,-4)/100.
    latdif=list(abs(np.array(latbin)-lat))
    londif=list(abs(np.array(lonbin)-lon))
    lat=latbin[latdif.index(min(latdif))]
    lon=lonbin[londif.index(min(londif))]

    tbl_name='lat_'+str(lat).replace('.','p').replace('-','m')
    conn=sqlite3.connect(d2c_db)
    pntr=conn.execute('SELECT * FROM '+tbl_name+' WHERE Longitude=="'+str(round(lon,2))+'"')
    ans=pntr.fetchall()
    if len(ans) > 1:
        print 'Retrieved more than one pixel:'
        for i in ans:
            print i
        sys.exit(1)
    elif len(ans) == 0:
        print 'Retrieval failed.'
        sys.exit(1)
    else:
        return ans[0]

def get_vessel_list():
    '''
    this module retrieves vessel information and store in dictionary
    list sorted by transmitter no
    '''
    vessel_db=vms_ask_path.vessel_db

    conn=sqlite3.connect(vessel_db)
    pntr=conn.execute('SELECT * FROM vessel')
    data=pntr.fetchall()
    header=[i[0] for i in pntr.description]

    return data,header
        
def get_vessel_list_psql():
    '''
    this module retrieves vessel information and store in dictionary
    list sorted by transmitter no
    postgresql version
    '''

    vessel_db=vms_ask_path.vessel_db_psql

    conn=psycopg2.connect(vessel_db)
    cur=conn.cursor()
    cur.execute('select * from vessel')
    data=cur.fetchall()
    header=[i[0] for i in cur.description]

    return data,header

def update_landing(trans_no,vessel_name,start_date,end_date):
    '''
    This module updates landing site usage for selected vessel
    '''
    vms_db = vms_ask_path.vms_proc_db_psql
    conn = psycopg2.connect(vms_db)
    cur = conn.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    retn=cur.fetchall()
    table_list_raw=[i[0] for i in retn if i[0].startswith('y20')]
#    print 'table_list_raw',table_list_raw
    #year list
    table_list=['y'+str(i) for i in range(int(str(start_date)[0:4]),
                                          int(str(end_date)[0:4])+1) if 'y'+str(i) in table_list_raw]

#    print 'table_list',table_list
    for table in table_list:
        print 'Update Landing:',trans_no,'/',vessel_name,'/',table
        cur.execute(sql.SQL("UPDATE {tbl} AS vms set locale=lnd.nama_pelab from {tbl2} AS lnd WHERE ST_Distance(lnd.geom,vms.geom)<=%s AND vms.status=%s AND vms.transmitter_no=%s AND vms.vessel_name=%s").format(tbl=sql.Identifier(table),tbl2=sql.Identifier('kkp_registered_landing_site')),['0.01','Landing',trans_no,vessel_name])
