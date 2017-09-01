#!/usr/bin/env python

import os,sys,math,sqlite3
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
import numpy as np
import simplekml


def plot_track(trk,hdr,lllat=None,urlat=None,
               lllon=None,urlon=None,marker=None,
               color='m',basemap=None,plot=True):
    '''
    This module will plot all tracks given on the map.
    basemap object can be passed into the module. map object will be
    returned with features added if plot is set to False.
    '''
    lat_idx=hdr.index('LATITUDE')
    lon_idx=hdr.index('LONGITUDE')
#    spd_idx=hdr.index('SPEED')
    
    lons=[]
    lats=[]
    for rec in trk:
        lons.append(rec[lon_idx])
        lats.append(rec[lat_idx])

    if basemap == None:
        if lllat==None or urlat==None or lllon==None or urlon==None:
            urlat=max(lats)+2 if max(lats)<88 else 90
            lllat=min(lats)-2 if min(lats)>-88 else -90
            urlon=max(lons)+2 if max(lons)<178 else 180
            lllon=min(lons)-2 if min(lons)>-178 else -180
        
            map=Basemap(projection='merc',llcrnrlat=lllat,urcrnrlat=urlat,
                        llcrnrlon=lllon,urcrnrlon=urlon,lat_ts=20,resolution='f')

    map.drawcoastlines()
    map.fillcontinents(color='coral',lake_color='aqua')

    x,y=map(lons,lats)
    map.plot(x,y,marker=marker,color=color)

    if plot==True:
        plt.show()
    else:
        return map


def plot_status(trk,hdr,status=None,lllat=None,urlat=None,
                lllon=None,urlon=None,mark_id=False,
                color='m',basemap=None,plot=True):
    '''
    This module will plot tracks with colors depicting their status on the map.
    '''
    lat_idx=hdr.index('LATITUDE')
    lon_idx=hdr.index('LONGITUDE')
    tran_idx=hdr.index('TRANSMITTER_NO')
    name_idx=hdr.index('VESSEL_NAME')
    gear_idx=hdr.index('REGISTERED_GEAR_TYPE')
    date_idx=hdr.index('REPORTDATE')
    #    spd_idx=hdr.index('SPEED')
 

    lons=[]
    lats=[]
    for rec in trk:
        lons.append(float(rec[lon_idx]))
        lats.append(float(rec[lat_idx]))

    trans_no=trk[0][tran_idx]
    vessel_name=trk[0][name_idx]
    start_date=trk[0][date_idx].strftime('%Y%m%d')
    end_date=trk[-1][date_idx].strftime('%Y%m%d')
    gear_type=trk[0][gear_idx]
    
    if mark_id and 'ID_KEY' in hdr:
        idks=[]
        idk_idx=hdr.index('ID_KEY')
        for rec in trk:
            idks.append(rec[idk_idx])

    
    if status is None or 'STATUS' not in hdr:
        if 'STATUS' not in hdr:
            print 'No status is provided.'
            raw_input()
        status=[]
        sta_idx=hdr.index('STATUS')
        for rec in trk:
            status.append(rec[sta_idx])
            

    if basemap == None:
        if lllat==None or urlat==None or lllon==None or urlon==None:
#            urlat=math.ceil(max(lats))
#            lllat=math.floor(min(lats))
#            urlon=math.ceil(max(lons))
#            lllon=math.floor(min(lons))
#            print urlat,lllat,urlon,lllon
#            latran=urlat-lllat
#            lonran=urlon-lllon
#            latbuf=latran*0.1
#            lonbuf=lonran*0.1
#            print latbuf,lonbuf
            latbuf=2
            lonbuf=2
            urlat=math.ceil(max(lats))+latbuf if math.ceil(max(lats))<90-latbuf else 90
            lllat=math.floor(min(lats))-latbuf if math.floor(min(lats))>-90+latbuf else -90
            urlon=math.ceil(max(lons))+lonbuf if math.ceil(max(lons))<180-lonbuf else 180
            lllon=math.floor(min(lons))-lonbuf if math.floor(min(lons))>-180+lonbuf else -180
#            print urlat,lllat,urlon,lllon
            
    map=Basemap(projection='merc',llcrnrlat=lllat,urcrnrlat=urlat,
                llcrnrlon=lllon,urcrnrlon=urlon,lat_ts=0,resolution='f')#,
#                        suppress_ticks=False)
#    print urlat,lllat,urlon,lllon
    intvp=1.
    intvm=1.
    parallels=np.arange(lllat,urlat+intvp,intvp)
    meridians=np.arange(lllon,urlon+intvm,intvm)
    while len(parallels)<3:
        intvp=intvp/2.
        parallels=np.arange(lllat,urlat+intvp,intvp)
    while len(meridians)<3:
        intvm=intvm/2.
        meridians=np.arange(lllon,urlon+intvm,intvm)
#    print parallels
#    print meridians
    prar=map.drawparallels(parallels,labels=[1,0,0,0])
    meri=map.drawmeridians(meridians,labels=[0,0,0,1])
    for m in meri:
        try:
            meri[m][1][0].set_rotation(45)
        except:
            pass
    map.drawcoastlines()
    map.fillcontinents(color='beige',lake_color='lightblue')
    map.drawmapboundary(fill_color='lightblue')
    plt.title(trans_no+'/'+vessel_name+'\n'+gear_type+'\n'+start_date+' to '+end_date)

    #status types
    #Fishing    fsh RED
    #Stationary sta BLUE
    #Transit    tra YELLOW
    #Landing    lnd GREEN

    #find fishing tracks
    con = list(status)
    color = ['m']*len(con)
    marker= ['v']*len(con)
    alpha = [0.4]*len(con)
    alpha_status=['Transit','Landing']
    for i in range(0,len(con)-1):

        if con[i]=='Fishing':
            marker[i]='+'
        if con[i]=='Stationary':
            marker[i]='o'
        if con[i]=='Transit':
            marker[i]='v'
        if con[i]=='Landing':
            marker[i]='o'    
        if con[i]=='Non-Fishing':
            marker[i]='v'
            
        if con[i]=='Fishing':
            color[i]='b'
        if con[i]=='Stationary':
            color[i]='b'
        if con[i]=='Transit':
            color[i]='r'
        if con[i]=='Landing':
            color[i]='g'
        if con[i]=='Non-Fishing':
            color[i]='r'
            
        if con[i]=='Fishing' and 'Fishing' in alpha_status:
            alpha[i]=0.1
        if con[i]=='Stationary' and 'Stationary' in alpha_status:
            alpha[i]=0.1
        if con[i]=='Transit' and 'Transit' in alpha_status:
            alpha[i]=0.1
        if con[i]=='Landing' and 'Landing' in alpha_status:
            alpha[i]=0.1
        if con[i]=='Non-Fishing' and 'Transit' in alpha_status:
            alpha[i]=0.1    
            
    marker[0]='H'
    marker[-1]='*'
    marker[-2]='*'
    for i in range(0,len(marker)-1):
        x,y=map([lons[i],lons[i+1]],
                [lats[i],lats[i+1]])
        map.plot(x,y,marker=marker[i],color=color[i],alpha=alpha[i],markerfacecolor='none')

        if mark_id:
            x1,y1=map(float(lons[i]),float(lats[i]))
            plt.annotate(idks[i],xy=(x1,y1),xycoords='data',xytext=(x1,y1),textcoords='data',ha='left',va='bottom',rotation=25)
    print con
    print color

#    x,y=map(lons,lats)
#    map.plot(x,y,marker=marker,color=color)

    if plot==True:
        plt.show()
    else:
        return map



def plot_track_kml(trk,hdr,dest):
    '''
    This module will output track in KML file.
    '''
    tno_idx=hdr.index('TRANSMITTER_NO')
    vnm_idx=hdr.index('VESSEL_NAME')
    lat_idx=hdr.index('LATITUDE')
    lon_idx=hdr.index('LONGITUDE')
    spd_idx=hdr.index('SPEED')
    hdn_idx=hdr.index('HEADING')
    rpd_idx=hdr.index('REPORTDATE')
    len_idx=hdr.index('LENGTH')
    wgt_idx=hdr.index('WEIGHT')
    ton_idx=hdr.index('TONNAGE')

#    kmlFileOut = simplekml.Kml()
#    h
    
