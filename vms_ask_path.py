#!/usr/bin/env python2.7

import os,sys

'''
This program provides a centralized interface for 
other scripts to ask for paths or constants.
'''


#base_dir='/eog/scratch1/david/SkyTruth_VMS_2nd'
psql_ip='35.188.118.231'
psql_ip='104.155.141.214'
psql_usr='postgres'
psql_pwd='2ijillgl'
base_dir=os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
tmp_dir=os.path.join(base_dir,'working')
test_db='dbname=postgres user='+psql_usr+' host='+psql_ip+' password='+psql_pwd
vms_db=os.path.join(base_dir,'VMS_db/SkyTruth_VMS_annual.db')
vms_db_psql='dbname=skytruthvms user='+psql_usr+' host='+psql_ip+' password='+psql_pwd
vms_proc_db=os.path.join(base_dir,'VMS_db/SkyTruth_VMS_process.db')
vms_proc_db_psql='dbname=skytruthvmsproc user='+psql_usr+' host='+psql_ip+' password='+psql_pwd
vessel_db=os.path.join(base_dir,'VMS_db/vessel.db')
vessel_db_psql='dbname=vessel user='+psql_usr+' host='+psql_ip+' password='+psql_pwd
landing_db=os.path.join(base_dir,'landing_db/landing_site.db')
fishing_db=os.path.join(base_dir,'fishing_db/fishing_ground.db')
vbd_db=os.path.join(base_dir,'vbd.db')
vbd_db_psql='dbname=vbd user='+psql_usr+' host='+psql_ip+' password='+psql_pwd
dist2coast_db=os.path.join(base_dir,'vvcm_dev/nasa_distfromcoast/dist2coast.db')
trk_tmp_dir=os.path.join(tmp_dir,'track_tmp')
