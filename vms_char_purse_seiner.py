#!/usr/bin/env python2.7

import os,sys,math
import sqlite3
import vms_visualize,vms_tools,vms_constants
import numpy as np

def characterize(trk_in,hdr_in,normalize=True):
    '''
    This module checks the status for each record in
    purse seiner track.
    Using filter method, for those 10km away from coast
    and speed < 2.5kt.
    Ref: Erico et. al., PLOS ONE July 1, 2016
    '''
    
    if normalize:
        print 'Normalize track to hourly bin.'
        print 'before normalize:',len(trk_in)
        trk,hdr = vms_tools.vms_track_normalize(trk_in,hdr_in)
        print 'after normalize:',len(trk)
    else:
        trk = trk_in[:]
        hdr = hdr_in[:]

    nrec = len(trk)

    lat_id = hdr.index('LATITUDE')
    lon_id = hdr.index('LONGITUDE')
#    spd_id = hdr.index('SPEED')
    tno_id = hdr.index('TRANSMITTER_NO')
    pdt_id = hdr.index('REPORTDATE')

    ret_trk = []
    ret_hdr = hdr[:].append('STATUS')

    none_list = np.chararray(nrec,itemsize=15)
    none_list[:] = vms_constants.null_val_str

    fsh = none_list[:]
#    for rec in trk:
#        rec=list(rec)
    spd_list = get_speed(trk,hdr)
    for i in range(0,nrec):
        rec=trk[i]
        lat=rec[lat_id]
        lon=rec[lon_id]
        dist2coast=vms_tools.dist2coast(lat,lon)[2] #km
#        spd=rec[spd_id]/9 #skytruch VMS speed is km/h*5, /9 to get knot
        spd=spd_list[i]
        if dist2coast > 10 and spd <2.5:
            fsh[i]='Fishing'
#            rec.append(u'Fishing')
        else:
            fsh[i]='Non-Fishing'
#            rec.append(u'Non-fishing')
#        ret_trk.append(tuple(rec))

    #check stationary
    sta = check_stationary(trk,hdr)

    #check linearity
#    lin = check_linear(trk,hdr)

    con = none_list[:]
    for i in range(0,nrec):
        #this two was intended for linearity consolidation
        if fsh[i] == 'Fishing':
            con[i] = 'Fishing'
        if fsh[i] == 'Non-Fishing':
            con[i] = 'Transit'
        #stationary consolidation
        if sta[i] == 'Landing':
            con[i] = 'Landing'
        if sta[i] == 'Stationary':
            con[i] = 'Stationary'

    #denormalize
    con_orig = denormalize_status(trk_in,hdr_in,trk,hdr,con)

    return con_orig

def denormalize_status(trk_orig,hdr_orig,trk_norm,hdr_norm,status):
    '''
    Blow status in hourly bin back to trk_in bin.
    '''

    orig_rpd_idx = hdr_orig.index('REPORTDATE')
    orig_rpd     = [i[orig_rpd_idx] for i in trk_orig]
    norm_rpd_idx = hdr_norm.index('REPORTDATE')
    norm_rpd     = [i[norm_rpd_idx] for i in trk_norm]
    orig_rpd_dt  = np.array([vms_tools.conv_vms_date_to_dt(str(i)) for i in orig_rpd])
    norm_rpd_dt  = np.array([vms_tools.conv_vms_date_to_dt(str(i)) for i in norm_rpd])
    orig_rpj     = np.array([vms_tools.dt2julian(i) for i in orig_rpd_dt])
    norm_rpj     = np.array([vms_tools.dt2julian(i) for i in norm_rpd_dt])

    #--
    #Simply loop thru all hourly bins, setting the status of original bins accordingly
    #--

    orig_status = np.chararray(len(orig_rpd),itemsize=15)
    orig_status[:] = vms_constants.null_val_str#'NA'

    for i in range(0,len(norm_rpd)-1):
        norm_status = status[i]
        pj1  = norm_rpj[i]
        pj2  = norm_rpj[i+1]
        norm_idx = np.where(np.logical_and(orig_rpj >= pj1,
                                           orig_rpj < pj2))
        orig_status[norm_idx] = norm_status

    return orig_status


def get_speed(trk,hdr):
    '''
    Calculate knot speed between records.
    '''
    
    lon_id = hdr.index('LONGITUDE')
    lat_id = hdr.index('LATITUDE')
    rpt_id = hdr.index('REPORTDATE')
    
    speed = np.arange(len(trk),dtype=np.float)

    speed[:] = -1 #last record do not hold speed

    for i in range(0,len(trk)-1):
        p1 = {'x':trk[i][lon_id],'y':trk[i][lat_id]}
        p2 = {'x':trk[i+1][lon_id],'y':trk[i+1][lat_id]}

        dist = vms_tools.getDist(p1,p2)

        rpt1 = vms_tools.conv_vms_date_to_dt(trk[i][rpt_id])
        rpt2 = vms_tools.conv_vms_date_to_dt(trk[i+1][rpt_id])

        dt   = (rpt2-rpt1)
        dthr = dt.days*24+dt.seconds/3600.

        speed[i] = dist/dthr/1.852 #km/h to knot

    return speed


def check_stationary(trk,hdr):
    '''
    Additional checking to discover stationary track.
    '''
    lon_id = hdr.index('LONGITUDE')
    lat_id = hdr.index('LATITUDE')

    nrec = len(trk)
    wsz  = 3

    dist = get_distance(trk,hdr)
    lat  = [i[lat_id] for i in trk]
    lon  = [i[lon_id] for i in trk]
    #move window until touches the last record (N/A) then
    #extend the window by 1 to cover the last record (N/A)
    #loop thru all recs except residule (nrec % wsz)
    none_list = np.chararray(nrec,itemsize=15)
    none_list[:] = vms_constants.null_val_str
    sta = none_list[:]
    for i in range(0,nrec-(nrec%wsz)):
        res = wsz-1 if nrec-1 < wsz*2 else 0

        wdist = dist[i:i+wsz+res]
        wlat  = lat[i:i+wsz+res]
        wlon  = lon[i:i+wsz+res]
        avg_dist = sum(wdist)/wsz
        if avg_dist < 0.001:
            avg_lat = np.mean(wlat)
            avg_lon = np.mean(wlon)
            d2c = vms_tools.dist2coast(avg_lat,avg_lon)[2]
            if d2c < 5:
                sta[i:i+wsz+res]='Landing'
            else:
                sta[i:i+wsz+res]='Stationary'
        else:
            sta[i:i+wsz+res]='Active'
        if res == wsz -1:
            break

    return sta

def get_rel_ang(trk,hdr):
    '''
    Calculate relative turning angle.
    '''
    #do not check for sgement linearity for purse siners
    
    lon_id = hdr.index('LONGITUDE')
    lat_id = hdr.index('LATITUDE')

    sgln = np.chararray(len(trk),itemsize=15)
    

def get_distance(trk,hdr):
    '''
    Calculate distance between records.
    '''
    
    lon_id = hdr.index('LONGITUDE')
    lat_id = hdr.index('LATITUDE')

    dist = np.arange(len(trk),dtype=np.float)
    dist[:]=-1 #last record do not hold distance

    for i in range(0,len(trk)-1):
        p1={'x':trk[i][lon_id],'y':trk[i][lat_id]}
        p2={'x':trk[i+1][lon_id],'y':trk[i][lat_id]}

        dist[i] = vms_tools.getDegDist(p1,p2)

    return dist
