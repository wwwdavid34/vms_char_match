#!/usr/bin/env python2.7

import os,sys,math
import sqlite3
import vms_tools

'''
This program takes the track read from VMS_db and 
perform daily status characterization based on the
speed and heading.
'''

class vms_status(object):
    
    def __init__(self,trk,hdr):

        status=self.characterize(trk,hdr)

    def characterize(self,trk,hdr):
        '''
        This module performs VMS status characterization.
        (1) Circular/<2kt(4km/h) : Purse Seining
        (2) 
