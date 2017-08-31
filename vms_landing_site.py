#!/usr/bin/env python2.7

import sys,os,sqlite3,math
import vms_tools,vms_ask_path
import vms_constants

def query_by_latlon(lat_in,lon_in):
    '''
    Return landing site info dictionary by lat/lon input.
    Default searching radius depends on the landing site, if 
    None is set, 0.1 degree radius will be used.
    '''
    lat = float(lat_in)
    lon = float(lon_in)
    landing_db = vms_ask_path.landing_db
    conn = sqlite3.connect(landing_db)
    pntr = conn.execute('SELECT * FROM landing_site')
    ldnst = pntr.fetchall()

    hdr = [i[0] for i in pntr.description]
    nam_idx = hdr.index('Name')
    lat_idx = hdr.index('Latitude')
    lon_idx = hdr.index('Longitude')
    cty_idx = hdr.index('Country')
    rds_idx = hdr.index('Radius')
    ctg_idx = hdr.index('Category')
    lat_ldn = [i[lat_idx] for i in ldnst]
    lon_ldn = [i[lon_idx] for i in ldnst]

    coords  = [(float(lat_ldn[i]),float(loni)) for i,loni in enumerate(lon_ldn)]

    dist = [math.sqrt((lat-c[0])**2+(lon-c[1])**2) for c in coords]
    min_id = dist.index(min(dist))
    
    res_st = {'Name':ldnst[min_id][nam_idx],
              'Latitude':ldnst[min_id][lat_idx],
              'Longitude':ldnst[min_id][lon_idx],
              'Country':ldnst[min_id][cty_idx],
              'Radius':ldnst[min_id][rds_idx],
              'Category':ldnst[min_id][ctg_idx]}

    conn.close()
    return res_st
              

def query_by_name(name):
    '''
    Return landing site info dictionary by name input.
    '''

    landing_db = vms_ask_path.landing_db
    conn = sqlite3.connect(landing_db)
    pntr = conn.execute('SELECT * FROM landing_site WHERE Name = "'+str(name)+'" COLLATE NOCASE')
    ldnst = pntr.fetchall()

    if len(ldnst) == 0:
        return None
    hdr = [i[0] for i in pntr.description]

    nam_idx = hdr.index('Name')
    lat_idx = hdr.index('Latitude')
    lon_idx = hdr.index('Longitude')
    cty_idx = hdr.index('Country')
    rds_idx = hdr.index('Radius')
    ctg_idx = hdr.index('Category')
    res_st = {'Name':ldnst[0][nam_idx],
              'Latitude':ldnst[0][lat_idx],
              'Longitude':ldnst[0][lon_idx],
              'Country':ldnst[0][cty_idx],
              'Radius':ldnst[0][rds_idx],
              'Category':ldnst[0][ctg_idx]}
    return res_st


def update_landing_site_info(site_name,lat=None,lon=None,country=None,
                             radius=None,category=None,new=False):
    '''
    Update the landing site DB.
    Site name is fixed attribute by definition.
    Users can change attributes of country, coordinate, radius and the basic type of the 
    site (Market, Storage, Unvalidated, etc.)
    If keyword "new" is True, a new record will be added the the DB, otherwise return 
    site not found.
    '''
    
    if lat is None and lon is None:
        if new:
            print 'Lat/Lon must be set for new entry.'
            return None
        else:
            lat = vms_constants.null_val_str #'NA'
            lon = vms_constants.null_val_str #'NA'
    if country is None: country=vms_constants.null_val_str #'NA'
    if radius is None: radius=vms_constants.null_val_str #'NA'
    if category is None: category=vms_constants.null_val_str #'NA'

    landing_db = vms_ask_path.landing_db
    conn = sqlite3.connect(landing_db)
    if new: #add new record
        pntr = conn.execute('INSERT INTO landing_site (Name, Latitude, Longitude, Country, Radius, Category) VALUES ('+','.join([str(site_name),str(lat),str(lon),str(country),str(radius),str(category)])+')')
        conn.commit()
    else: #update attribute
        #get original record
        info = query_by_name(site_name)
        info['Latitude']  = lat
        info['Longitude'] = lon
        info['Country']   = country
        info['Radius']    = radius
        info['Category']  = category

        #update each non-None column
        for tag in list(info):
            if info[tag] != 'NA' and tag!='Name':
                print 'cmd: ','UPDATE landing_site SET '+tag+' = "'+str(info[tag])+'" WHERE Name = "'+site_name+'" COLLATE NOCASE'
                pntr = conn.execute('UPDATE landing_site SET '+tag+' = "'+str(info[tag])+'" WHERE Name = "'+site_name+'" COLLATE NOCASE')
        conn.commit()
    
    conn.close()
    

