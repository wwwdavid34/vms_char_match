#!/usr/bin/env python2.7

import os,sys,math
import sqlite3
import vms_ask_path
import vms_tools,vms_visualize
import vms_char_long_line
import vms_char_purse_seiner

class vms_characterize(object):
    
#    def __init__(self,trk,hdr):
    def characterize(self,trk,hdr):

        uniq_gear = self.checkSameGear(trk,hdr)
        if not uniq_gear:
            print 'Gear type inconsistent.'
            sys.exit(1)
        

#        print 'uniq_gear',uniq_gear
        gear_type = self.checkType(uniq_gear)
        print 'gear type',gear_type
        if gear_type == 'purse_seiner':
#            vms_visualize.plot_status(trk,hdr)
#            status=self.char_purse_seiner(trk,hdr)
            print 'Found purse_seiner'
            status,sgid=self.char_long_line(trk,hdr)
#            vms_visualize.plot_status(trk,hdr,status)
#            status=self.char_long_line(trk,hdr)
        if gear_type == 'long_line':
            print 'Found long_line'
            status,sgid=self.char_long_line(trk,hdr)
        if gear_type == 'trawl':
            print 'Found trawl'
            #apply long line characterization
            status,sgid=self.char_long_line(trk,hdr)
        if gear_type == 'others':
            print 'Found others'
            status,sgid=self.char_long_line(trk,hdr)
#            print 'Not ready:',gear_type
#            sys.exit(1)
        
        return status,sgid

    def char_long_line(self,trk,hdr):
        '''
        This module checks the status for each record in 
        long liner track.
        Using data mining method (Lavielle's segmentation algorithm).
        Ref: Erico et. al., PLOS ONE July 1, 2016
        '''
        
        status,sgid = vms_char_long_line.characterize(trk,hdr)

        return status,sgid

    def char_purse_seiner(self,trk,hdr):
        '''
        This module checks the status for each record in
        purse seiner track.
        Using filter method, for those 10km away from coast
        and speed < 2.5kt.
        Ref: Erico et. al., PLOS ONE July 1, 2016
        '''

        status = vms_char_purse_seiner.characterize(trk,hdr)
        
        return status

    def _char_purse_seiner(self,trk,hdr):
        #deserted
        '''
        This module checks the status for each record in 
        purse seiner track.
        Using filter method, for those 10km away from coast
        and speed < 2.5kt.
        Ref: Erico et. al., PLOS ONE July 1, 2016
        '''
        lat_idx = hdr.index('LATITUDE')
        lon_idx = hdr.index('LONGITUDE')
        spd_idx = hdr.index('SPEED')

        ret_trk = []
        ret_hdr = hdr[:].append('STATUS')

        for rec in trk:
            rec=list(rec)
            lat=rec[lat_idx]
            lon=rec[lon_idx]
            dist2coast=vms_tools.dist2coast(lat,lon)[2] #km
            spd=rec[spd_idx]/9 #skytruch VMS speed is km/h*5, /9 to get knot
            if dist2coast > 10 and spd <2.5:
                rec.append(u'Fishing')
            else:
                rec.append(u'Non-fishing')
            ret_trk.append(tuple(rec))

        return ret_trk
        
    def checkType(self,gear_str):
        '''
        Check which major type the gear belongs.
        '''

        purse_seiner=['Pukat cincin grup pelagis besar',
                     'Pukat cincin grup pelagis kecil',
                     'Pukat cincin Pelagis Besar dengan satu kapal',
                     'Purse Seine (Pukat Cincin) Pelagis Kecil']
        long_line=['Huhate',
                   'Pancing Cumi (squid jigging)',
                   'Pancing Ulur',
                   'Rawai dasar',
                   'Rawai Tuna']
        trawl=['Pukat Ikan',
               'Pukat Udang']
        others=['Pengangkut',
                'Bouke ami',
                'Bubu',
                'Hand Line Tuna',
                'Jaring insang oseanik',
                'Jaring liong bun']

        if gear_str in purse_seiner:
            return 'purse_seiner'
        elif gear_str in long_line:
            return 'long_line'
        elif gear_str in trawl:
            return 'trawl'
        elif gear_str in others:
            return 'others'
        else:
            return None

    def checkSameGear(self,trk,hdr):
        '''
        This module checks gear type.
        Only same gear is allowed for batch characterization.
        '''
        gear_idx = hdr.index('REGISTERED_GEAR_TYPE')
        gears=[i[gear_idx] for i in trk]
        uniq_gear=set(gears)
        n_gear=len(uniq_gear)
#        print n_gear
#        sys.exit(1)
        if n_gear>1:
            return False
        else:
            return list(uniq_gear)[0]
