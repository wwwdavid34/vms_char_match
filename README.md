# vms_char_match

This repository contains a system that attempts to characterize VMS (Vessel Monitoring System) tracks, and match the record with VBD (VIIRS Boat Detection) data.

## Methodology

We are currently only working on Indonesian VMS tracks. The VMS tracks came in with following fields:

  Field | Comment
  ----- | -------
  transmitter_no | The ID of the VMS equipment
  vessel_name | The human readable name of the vessel
  latitude | Latitudal reading of vessel location
  longitude | Longitudal reading of vessel location
  reportdate | Timestamp of the location being recorded down to second
  registered_geartype | The gear type of the vessel
  length | Length of the vessel in meter
  width | Width of the vessel in meter
  gross_tonnage | Gross tonange of the vessel in ton
  registered_area | Allowed fishing WPP for the vessel
  start_date | Unsure - could be the start date of registration
  end_date | Unsure - could be the end date of registration

The VMS record are being segmented based on their velocity. The max likelihood is calculated against a 10-stage velocity distribution model. Currently the velocity is defined in degrees, will need to be change to meter-based.
  