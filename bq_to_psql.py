#!/usr/bin/env python

import vms_tools,vms_predict,vms_ask_path
from google.cloud import bigquery as bq
import psycopg2 as pg
from psycopg2 import sql
import sys
import datetime as dt

#get ais table
client=bq.Client()
dataset=client.dataset('GFW_AIS')
tbl_list=[i.name for i in dataset.list_tables() if i.name.startswith('20')]

#this sql command takes vessels with more records and actual moving segments
#from 10-day record
cmd='''
  SELECT
    mmsi,
    COUNT(*) seg_count,
    AVG(avg_speed) avg_speed,
    SUM(point_count) point_count,
    MAX(max_lat) max_lat,
    MIN(min_lon) min_lon,
    MIN(min_lat) min_lat,
    MAX(max_lon) max_lon from(
    SELECT
      seg.*
    FROM (
      SELECT
        mmsi,
        COUNT(*) count
      FROM
        `skytruth-vms.GFW_AIS.201701*`
      WHERE
        _table_suffix BETWEEN '11' AND '31'
      GROUP BY
        mmsi
      ORDER BY
        count DESC )sub
    JOIN
      `world-fishing-827.gfw_research.segment_pipeline_classify_p_p516_daily` AS seg
    ON
      sub.mmsi=seg.mmsi
    WHERE
      count>10000) #more than 1k records
  WHERE
    avg_speed>5 #avg_speed faster than 5 knots
  GROUP BY
    mmsi
  ORDER BY
    seg_count DESC
'''
client=bq.Client()
query=client.run_sync_query(cmd)
query.use_legacy_sql=False
query.run()
mmsi_list=[row[0] for row in query.rows]
aisdb=vms_ask_path.vms_proc_db_psql
conn=pg.connect(aisdb)
cur=conn.cursor()
#cmd=sql.SQL('''
#CREATE TABLE IF NOT EXISTS {tbl} (
#id_key TEXT UNIQUE,
#transmitter_no TEXT,
#latitude DOUBLE PRECISION,
#longitude DOUBLE PRECISION,
#reportdate TIMESTAMP);
#SELECT AddGeometryColumn ('public',%s,'geom',4326,'POINT',2);
#''').format(tbl=sql.Identifier('a2017'))
#print cmd.as_string(conn)
#raw_input()
#cur.execute(cmd,['a2017'])
#conn.commit()

mmsi_idx=mmsi_list.index(mmsi_list[0])
mmsi_idx=mmsi_list.index(231014000)
for mmsi in mmsi_list[mmsi_idx:-1]:
    #now query for each boat
    trk,hdr=vms_tools.get_ais_track(mmsi,start_date='20170101',end_date='20170110')
    if len(trk)==0:
        print mmsi
#        raw_input()
        continue
    
    vp=vms_predict.vms_predict()
    pprd=vp.predict(trk,hdr)
    print 'pprd',pprd
    prd=[]
    for p in pprd:
        pp=list(p)
#        print 'pp_raw',pp
        if pp[-1]=='-999999':
            continue
        ppdt=dt.datetime.strptime(pp[-1],'%Y%m%d%H%M%S')
        pp[-1]=ppdt.strftime('%Y-%m-%d %H:%M:%S')
        prd.append(pp)
#        print 'pp',pp
#    print 'prd',prd
#    raw_input()
    mmsi_idx=hdr.index('TRANSMITTER_NO')
    lat_idx=hdr.index('LATITUDE')
    lon_idx=hdr.index('LONGITUDE')
    rpd_idx=hdr.index('REPORTDATE')
#    hdr2=['id_key','transmitter_no','latitude','longitude','reportdate','geom']
    trk2=[(
        '_'.join(['AIS',str(i[mmsi_idx]),
                  str(i[lat_idx]).replace('.','p').replace('-','m'),
                  str(i[lon_idx]).replace('.','p').replace('-','m'),
                  i[rpd_idx].strftime('%Y%m%d%H%M%S')]),
        i[mmsi_idx],
        i[lat_idx],
        i[lon_idx],
        i[rpd_idx].strftime('%Y-%m-%d %H:%M:%S')) for i in trk ]
#    prd2=[i[0],i[1],i[2],i[3],i[4] for i in prd]
    cnt=0
    trk2.extend(prd)
    
#    print 'len(trk),len(trk2),len(prd)',len(trk),len(trk2),len(prd)

#    print [i for i in trk2 if 'PRD' in i[0]]
#    raw_input()
    print 'Inserting records to postgresql...'
    for row in trk2:
        tbl='a2017'
        holders=','.join(['%s']*len(row))
        geom = "ST_GeomFromText('POINT({1} {0})',4326)".format(row[2],row[3])
#        print geom
        cmd='VALUES({0},{1})'.format(holders,geom)
        cmd2=sql.SQL('INSERT INTO {tbl} '+cmd).format(tbl=sql.Identifier(tbl))
#        print cmd2.as_string(conn)
#        print [str(i) for i in row]
        try:
#            row=[0,0,0,0,0]
#            print row
            
            #            cur.execute(sql.SQL('INSERT INTO {tbl} VALUES(DEFAULT,{0},{1})'.format(holders,geom)).format(tbl=sql.Identifier(tbl)),row)
            cur.execute(cmd2,[str(i) for i in row])
            cnt+=1
        except pg.IntegrityError as e:
            conn.rollback()
            print 'psql error:',e
        else:
            conn.commit()
#        raw_input()
    if cnt>0:
        print 'Inserted %s rows.' % cnt
        
