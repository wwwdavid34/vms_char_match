#!/usr/bin/env python2.7

import rpy2.robjects as ro
from rpy2.robjects.packages import importr
from rpy2.robjects import r
import sys,os
import vms_visualize,vms_tools,vms_constants
import numpy as np
from itertools import groupby

def characterize(trk_in,hdr_in,normalize=True):
    '''
    convert track tuple to adehabitatLT ltraj object
    return track(r.ltraj),segment(r.ltraj),active(py.str_list),linearity(py.str_list),fishing(py.str_list)
    '''

    ##Embed trk normalization
    if normalize:
        print 'Normalize track to hourly bin.'

        if len(trk_in) < 50:
            print 'Track length < 50, abort characterization',len(trk_in)
            sta = ['Pending']*len(trk_in)
            sgid= [vms_constants.null_val_str]*len(trk_in)
            return sta,sgid

        trk,hdr = vms_tools.vms_track_normalize(trk_in,hdr_in)
        print 'before normalize:',len(trk_in)
        print 'after normalize:',len(trk)
#        vms_visualize.plot_track(trk_in,hdr_in)
#        vms_visualize.plot_track(trk,hdr)

    else:
        trk = trk_in[:]
        hdr = hdr_in[:]
    
    lat_id=hdr.index('LATITUDE')
    lon_id=hdr.index('LONGITUDE')
    tno_id=hdr.index('TRANSMITTER_NO')
    pdt_id=hdr.index('REPORTDATE')

#    print trk[0]
    
    rlat=ro.FloatVector([float(i[lat_id]) for i in trk])
    rlon=ro.FloatVector([float(i[lon_id]) for i in trk])
    rtno=ro.StrVector([str(i[tno_id]) for i in trk])
    rpdt=ro.StrVector([str(i[pdt_id]) for i in trk])

#    print rpdt
    
    r.assign('y',rlat)
    r.assign('x',rlon)
    r.assign('tno',rtno)
    r.assign('pdt',rpdt)

    none_list=np.chararray(len(rlat),itemsize=15)
    none_list[:]= vms_constants.null_val_str#'NA'

    r('xy <- cbind(x,y)')
#    r('XY <- as.data.frame(xy)')

##todo: reproject XY to UTM zones designated by average longitude
##Thus enable velocity model to be defined by KM/h instead of deg/h
##Reference: https://stackoverflow.com/questions/7927863/location-data-format-for-adehabitat-package
    importr('rgdal')
    print 'mean(x)',r('mean(x)')
    print 'max(x)',r('max(x)')
    print 'min(x)',r('min(x)')
    r('utmz <- (floor((mean(x) + 180)/6) %% 60) + 1') #find utmz
    r('utmxy <- project (xy,paste("+proj=utm +zone=",toString(utmz)," ellps=WGS84"))')
    r('XY <- as.data.frame(utmxy)')

    importr('adehabitatLT')
    importr('miscTools')
    r('pdt <- as.POSIXct(strptime(pdt,"%Y%m%d%H%M%S"),"GMT")')
    r('trl <- as.ltraj(XY,id=tno,date=pdt)')

#    print r('trl')

#    r('plotltr(trl,"dist")')
    
    #####
    ##Take care of the landing records, which will create NA angle and cause partmod.ltraj to fail.

    ##If none Active record in the trl, return trl and skip segmentation
    ##Add stationary detector to avoid landing being recognized as mod.1 '4926123 2014Apr.'

#    print r('trl')
#    vms_visualize.plot_track(trk,hdr)
#    return r('trl'),None

    sta = check_stationary(r('trl'))
    if 'Active' not in set(sta):
        print 'The track is completely stationary. Skip segmentation and classification.'
#        return consolidate_trl(r('trl'),r('trl'),sta=sta)
        sg=np.chararray(len(sta),itemsize=15)
        sg[:]=vms_constants.null_val_str
        return denormalize_status(trk_in,hdr_in,trk,hdr,sta,sg)
#    print sta
#    print set(sta)
#    float('a')

    #test K
    print 'Begin track segmentation and classification.'


    #check if there is stationary points
    #if true, the track needs to be splitted
    '''check stationary by looking for dist==0'''
#    r('length(which.ltraj(trl,"dist==0")[[1]])')
    
    #remove stationary records

    r('trlo <- trl') #backup original trl as trlo
    print 'Found Stationary:','Stationary' in sta
    print 'Found Landing:','Landing' in sta
    if ('Stationary' in sta) or ('Landing' in sta):
        print 'Remvoe stationary records'
        sta_idx = [i+1 for (i,j) in enumerate(sta) if j=='Stationary' or j=='Landing']

        r.assign('sta_idx',ro.IntVector(sta_idx))
        r('trlf <- trl[[1]][-c(sta_idx),]') #filter active recs with check_stationary result
        print r('trl <- as.ltraj(trlf[,c("x","y")],trlf[,c("date")],id=id(trl))') #recast into ltraj


#    print 'paused after remove stationary records'
#    raw_input()
    trl_len=r('length(trl[[1]]$x)')
    print trl_len[0]
    if trl_len[0] < 50:
        print 'Active record less then 50, abort characterization.',trl_len[0]
        print 'The status is marked [Pending] for later run to pickup.'
        sta[:] = 'Pending'
        sg=np.chararray(len(sta),itemsize=15)
        sg[:] = vms_constants.null_val_str
        status,sgid = denormalize_status(trk_in,hdr_in,trk,hdr,sta,sg)
        return status,sgid
#    print 'force exit'
#    sys.exit(1)
##TODO: change velocity model from degree based to meter based
    r('tested.means <- seq(0,20000,length=10)')
    r('(limod <- as.list(paste("dnorm(dist, mean =",tested.means,", sd = 2000)")))')

#    r('tested.means <- seq(0, 0.25, length = 10)')
#    r('(limod <- as.list(paste("dnorm(dist, mean =",tested.means,", sd = 0.03)")))')


    r('mod <- modpartltraj(trl,limod)')
    r('bestmod <- bestpartmod(mod,Km=round(length(trl[[1]]$x)/5),plotit=FALSE)')
    r('k <- which.max(colMedians(bestmod$correction,na.rm=TRUE))') #require colMedians from miscTools package
    r('save(XY,trl,k,mod,limod,file="XY_trl_k_mod_limod.RData")')
#    r('trl[[1]]$rel.angle[is.na(trl[[1]]$rel.angle)]<-0')
#    r('trl[[1]]$abs.angle[is.na(trl[[1]]$abs.angle)]<-0')
#    print r('trl[[1]]')

    ## if only one segment is recognized, put it to pending for future merging
#    if r('k')[0]==1:
#        print 'Unable to split track, set to pending.'
#        sta[:] = 'Pending'
#        status = denormalize_status(trk_in,hdr_in,trk,hdr,sta)
#        return status

    while r('k')[0]>0:
        try:
            if r('k')[0]==1:
                print 'Unable to split track, set to pending.'
                sta[:] = 'Pending'
                sg=np.chararray(len(sta),itemsize=15)
                sg[:] = vms_constants.null_val_str
                status,sgid = denormalize_status(trk_in,hdr_in,trk,hdr,sta,sg)
                return status,sgid
            else:
                print r('pm <- partmod.ltraj(trl,k,mod,na.manage=c("locf"))')
                break
        except:
            r('k<-k-1') #if previious fails, very likely to be nparts(k) is overestimated by 1
            print 'Trying k=',r('k')[0]
            continue

#        print r('pm <- partmod.ltraj(trl,k,mod,na.manage=c("locf"))')

##    print r('pm <- partmod.ltraj(trl,k,mod,na.manage=c("prop.move","locf"))')
#    r('plot(pm)')

#    raw_input()
#    float('a')
#    vms_visualize.plot_track(trk,hdr)

    ##Check linearity
    ##Let consolidate_trl do the job
#    linear_list = check_linear(pm)
#    print 'consolidate_trail'
    status,sgid = consolidate_trl(r('trlo'),r('trl'),pm=r('pm'),sta=sta)
#    print 'len(status)',len(status)
#    return trk_in,hdr_in,trk,hdr,status
#    return r('trl'),r('pm'),sta,linear_list,fish_list
#    for i in range(0,len(status)):
#        print i+1,status[i]

#    print len(status),len(sgid)
#    for i in range(0,len(status)-1):
#        print [i,status[i],sgid[i]]
#    raw_input()

    if normalize:
        #Blow hourly bin back to original binning
        status_tmp = status[:]
        status,sgid = denormalize_status(trk_in,hdr_in,trk,hdr,status,sgid)

#    for i in range(0,len(status)-1):
#        print [i,status[i],sgid[i]]
#    raw_input()
#    sys.exit(1)#

    #if fishing segment touches landing, the segment will be changed to transit
#    print status
    glist=[list(j) for i,j in groupby(status)]
    gname=np.array([i for i,j in groupby(status)])
#    print gname
    #do nothing if there is only one segment
    if len(gname)==1:
        return status,sgid

    for gidx in list(np.where(gname=='Fishing')[0]):
#        print gidx
        if gidx==0:
#            print gname[gidx+1]
#            raw_input()
            if gname[gidx+1]=='Landing':
                glist[gidx][:]=['Transit']*len(glist[gidx][:])
        elif gidx==len(gname)-1:
            if gname[gidx-1]=='Landing':
                glist[gidx][:]=['Transit']*len(glist[gidx][:])
        else:
            if gname[gidx+1] == 'Landing' or gname[gidx-1]=='Landing':
                glist[gidx][:]=['Transit']*len(glist[gidx][:])

    status=[i for x in glist for i in x]
        
    return status,sgid


def denormalize_status(trk_orig,hdr_orig,trk_norm,hdr_norm,status,sgid):
    '''
    Blow status in hourly bin back to trk_in bin.
    '''

    print 'Denormalizing status...'

    orig_rpd_idx = hdr_orig.index('REPORTDATE')
    orig_rpd     = [int(i[orig_rpd_idx].strftime("%Y%m%d%H%M%S")) for i in trk_orig]
    norm_rpd_idx = hdr_norm.index('REPORTDATE')
    norm_rpd     = [int(i[norm_rpd_idx]) for i in trk_norm]
    orig_rpd_dt  = np.array([vms_tools.conv_vms_date_to_dt(str(i)) for i in orig_rpd])
    norm_rpd_dt  = np.array([vms_tools.conv_vms_date_to_dt(str(i)) for i in norm_rpd])
    orig_rpj     = np.array([vms_tools.dt2julian(i) for i in orig_rpd_dt])
    norm_rpj     = np.array([vms_tools.dt2julian(i) for i in norm_rpd_dt])

    #--
    #Simply loop thru all hourly bins, setting the status of original bins accordingly
    #--

    orig_status = np.chararray(len(orig_rpd),itemsize=15)
    orig_status[:] = vms_constants.null_val_str#'NA'
    orig_sgid   = np.chararray(len(orig_rpd),itemsize=30)
    orig_sgid[:] = vms_constants.null_val_str

    print len(status),len(sgid),len(norm_rpd)
    for i in range(0,len(norm_rpd)-1):
        norm_status = status[i]
        norm_sgid   = sgid[i]
        pj1  = norm_rpj[i]
        pj2  = norm_rpj[i+1]
        pd1  = norm_rpd[i]
        pd2  = norm_rpd[i+1]
        norm_idx = np.where(np.logical_and(orig_rpj >= pj1,
                                           orig_rpj <= pj2))

#        norm_idx = np.where(np.logical_and(orig_rpd >= int(pd1),
#                                           orig_rpd <= int(pd2)))
        orig_status[norm_idx] = norm_status
        orig_sgid[norm_idx] = norm_sgid
    #debug
    f=open('debug.csv','wb')
    for i in range(0,len(orig_rpd)):
        a=list(trk_orig[i])
        a.append(orig_status[i])
        b=[str(i) for i in a]
        f.write(','.join(b)+'\n')

    return orig_status,orig_sgid
                

def consolidate_trl(trlo,trl,pm=None,sta=None,lin=None):
    '''
    Consolidate trl, pm, stationary check, linearity check.
    Append universal uniq segment id
    '''
 
    print 'Consolidating status...'
   
    #start the process using subset of active records
    nrec  = len(r('trl[[1]]$x'))
    nreco = len(r('trlo[[1]]$x'))

#    none_list = np.chararray(nrec,itemsize=15)
#    none_list[:] = vms_constants.null_val_str#'NA'
    if sta is None:
#        print 'No sta input, creating from trl.'
        sta = check_stationary(trl)

    #check linearity by segment
    #if no linearity, try to make from pm if pm exist
    if lin is None:
        if pm is None:
#            print 'No lin and pm input, set lin to NA.'
            lin = np.chararray(nrec,itemsize=15)
            lin[:] = vms_constants.null_val_str
        else:
#            print 'Creating lin.'
            lin = check_linear(pm)

    #blow linearity from segment to full track size
    #add segment id in this loop
    lin_tmp = lin[:]
    lin = np.chararray(nrec,itemsize=15)
    lin[:] = vms_constants.null_val_str
    nsg = r('length(pm[[1]])')[0]
    locs= r('pm[[2]]$locs')
    mod = r('pm[[2]]$mod')
    sgidn = np.chararray(len(lin),itemsize=30)
    sgidn[:] = vms_constants.null_val_str
    for s in range(0,nsg):
        #mark start and end idx for each segment
        ini = int(locs[s]-1)
        end = int(locs[s+1]-1)
        lin[ini:end] = lin_tmp[s]
        dt  = str(r('pm[[1]]['+str(s+1)+'][[1]]$date[1]')).split('"')[1]
        sgidn[ini:end]= '_'.join([dt,'b'+str(s+1),'m'+str(mod[s])])



    #make fsh list
    if pm is None:
        fsh = np.chararray(nrec,itemsize=15)
        fsh[:] = vms_constants.null_val_str
    else:
        fsh = np.chararray(nrec,itemsize=15)
        fsh[:] = vms_constants.null_val_str#initialize fsh list
        nsg = r('length(pm[[1]])')[0]
        locs= r('pm[[2]]$locs')
        for s in range(0,nsg):
            #mark start and end idx for each segment
            ini = int(locs[s]-1)
            end = int(locs[s+1])
            mod = r('pm[[2]]$mod')[s]
            if int(mod) < 5: #low speed == fishing (was 3 in degree model)
                fsh[ini:end] = 'Fishing'
            else:
                fsh[ini:end] = 'Non-Fishing'

#    print 'fsh',fsh
#    float('a')

    #prepare conversion index list for pm
#    dttrl = r('trl[[1]]$date')
    sgidx = [None]*nrec
#    dtsg  = []
    #get index in original trl from the rowid
    for s in range(0,nsg):
#        dtsg.extend(r('pm[[1]]['+str(s+1)+'][[1]]$date'))
        sgidx.extend(r('dimnames(pm[[1]]['+str(s+1)+'][[1]])[[1]]'))
    sgidx=list(set(sgidx))
    sgidx=[int(i) for i in sgidx if i is not None]
    sgidx=sorted(sgidx)
    print len(sgidx),nrec
    if len(sgidx) != nrec:
        print sgidx
        print len(sgidx)
        print 'Incorrect # of sgidx.'
        float('a')

#    for d in range(0,len(dtsg)):
#        sgidx[d] = dttrl.index(dtsg[d])
#    if -1 in sgidx:
#        print 'An non-exist records found'
#        float('a')

    #start consolidation
#    con = none_list[:]
    #prepare consolidated list holder with original size
    con = np.chararray(nreco,itemsize=15)
    con[:] = vms_constants.null_val_str
    sgido = np.chararray(nreco,itemsize=30)
    sgido[:] = vms_constants.null_val_str
    #    for i in range(0,nrec):
#    print 'sgidx',sgidx
#    print lin
#    raw_input()
    for j in range(0,nrec):
        i=sgidx[j]-1 #convert to original size IN PYTHON
        #first do fishing and linear movement correction
#        print 'Consolidating fsh and lin'
#        print 'fsh[i]',fsh[i]
        sgido[i]=sgidn[j]

        if fsh[j]=='Fishing':
#            print 'lin[i]',lin[i]
            if lin[j] == 'Non-Fishing':
                con[i] = 'Transit'
            else:
                con[i] = 'Fishing'
        if fsh[j]=='Non-Fishing':
            if lin[j] == 'Non-Fishing':
                con[i] = 'Transit'
            else:
                con[i] = 'Non-Fishing'
        #then do stationary correction on top of that
#        print 'Consolidating sta.'
#        print 'sta[i]',sta[i]

    #separate loop for stationary list because it is full size from the beginning
    for i in range(0,nreco):
        if sta[i] == 'Landing':
            con[i] = 'Landing'
        if sta[i] == 'Stationary':
            con[i] = 'Stationary'

    #replicate the last record
    con[-1]=con[-2]
    cnt=0
#    for i in con:
#        print cnt,i
#        cnt+=1
#    print 'nreco',nreco
    
#    raw_input()

    return con,sgido

def check_stationary(trl):
    '''
    Additional checking to discover stationary track.
    '''
    #first check for stationary records by distance column in trl
    ##stationary threshold <- 0.0005 deg and more than 3 hours
    ##use moving window
    ##ignore NA (the last dist rec)
    ## i=0         i=1         i=2         i=3         i=4
    ## rec1        rec2        rec3        rec4        rec5
    ## |dist1------|dist2------|dist3------|dist4------|N/A--------|
    ## |window1----------------------------|
    ##             |window2----------------------------------------|

    dist = r('trl[[1]]$dist')
    lon  = r('trl[[1]]$x')
    lat  = r('trl[[1]]$y')
    nrec = len(dist)
    wsz  = 2
    sta  = np.chararray(nrec,itemsize=15)
    #move window until touches the last record (N/A) then 
    #extend the window by 1 to cover the last record (N/A)
    #loop thru all recs except residule (nrec % wsz)
    for i in range(0,nrec-(nrec%wsz)):
        res = wsz-1 if nrec-i < wsz*2 else 0
#        if res == wsz-1:
#            print i,nrec,wsz
        wdist = dist[i:i+wsz+res]
        wlat  = lat[i:i+wsz+res]
        wlon  = lon[i:i+wsz+res]
        avg_dist = sum(wdist)/wsz
        if avg_dist < 0.01:
#            avg_lat = sum(wlat)/len(wlat)
#            avg_lon = sum(wlon)/len(wlon)
            avg_lat = np.mean(wlat)
            avg_lon = np.mean(wlon)
            #check each stationary point to see if it is close to shore
            d2c=vms_tools.dist2coast(avg_lat,avg_lon)[2]
            if d2c < 5:
                sta[i:i+wsz+res]='Landing'
#                print 'Landing',avg_dist,d2c,avg_lat,avg_lon
            else:
                sta[i:i+wsz+res]='Stationary'
#                print 'Stationary',avg_dist,d2c,avg_lat,avg_lon
        else:
            sta[i:i+wsz+res]='Active'
#            print 'Active',avg_dist,d2c
        if res == wsz-1:
            break
#    print sta,len(sta)
#    print set(sta)
#    float('a')
    return sta

def check_linear(pm):
    '''
    Additional checking to segmentation from adehabitatLT.
    "if average consine returns a value above 0.8 or below -0.8, it indicates that the 
    whole segment is formed by a straight line movement." --de Souza et al. 2016 PLOS
    '''
    
#    sgds = pm[[0]]
    nsg  = r('length(pm[[1]])')[0]
    mod  = r('pm[[2]]$mod')
    locs = r('pm[[2]]$locs')
    sgln = np.chararray(nsg,itemsize=15) #list storing if segment is a line
    sgln[:] = vms_constants.null_val_str #'NA'

    for i in range(0,nsg):
        #get segment average cosine
        sdrelang = r('sd(pm[[1]][['+str(i+1)+']][,"rel.angle"],na.rm=TRUE)')
        #
        if np.isnan(sdrelang[0]):
            sdrelang = r('max(pm[[1]][['+str(i+1)+']][,"rel.angle"],na.rm=TRUE)')

        if sdrelang[0] > 0.8:
            sgln[i]='Fishing'
        else:
            sgln[i]='Non-Fishing'

    return sgln

        
    
