#!/usr/bin/env python

import vms_tools
from vms_characterize import vms_characterize
import vms_visualize
vmsc=vms_characterize()
trk,hdr=vms_tools.get_vms_track('102803',end_date='20140131',proc=True)
status,gid=vmsc.characterize(trk,hdr)

vms_visualize.plot_status(trk,hdr,status)
