#!/usr/bin/env python

import vms_tools as vt
import vms_visualize as vv
import sys
import datetime as dt

current_time=dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print '==='
print 'Script started: '+current_time
print '==='

trans_no=sys.argv[1]
trk,hdr=vt.get_ais_track(trans_no)
if len(trk)==0:
    print 'No track found'
    sys.exit(0)

current_time=dt.datetime.now().strftime('%Y%m%d%H%M%S')
outfile='_'.join([str(trans_no),current_time])+'.csv'
writer=open('/work/ais_dev/ais_track/'+outfile,'wb')
print 'Writing output to %s' % outfile
writer.write(','.join(hdr)+'\n')
for i in trk:
    writer.write(','.join([str(i) for i in list(i)])+'\n')

vv.plot_track(trk,hdr)
