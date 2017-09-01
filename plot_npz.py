#!/usr/bin/env python

import numpy as np
import vms_visualize as vv
import sys
import datetime as dt

npz_file=sys.argv[1]

a=np.load(npz_file)
trk=[(i[6],i[7]) for i in a.items()[0][1]]
print 'Track length %s' % len(a.items()[0][1])
print 'Start from '+dt.datetime.fromtimestamp(int(a.items()[0][1][0][1])).strftime('%Y-%m-%d %H:%M:%S')+'|lat/lon: '+str(a.items()[0][1][0][6])+'/'+str(a.items()[0][1][0][7])
print 'Ending at  '+dt.datetime.fromtimestamp(int(a.items()[0][1][-1][1])).strftime('%Y-%m-%d %H:%M:%S')
hdr=['LATITUDE','LONGITUDE']
vv.plot_track(trk,hdr)
