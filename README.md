# vms_char_match/ais_match
This is a branch from VMS charasterize and match with VBD to work on AIS tracks.

### Command for AIS cross matching
```
UPDATE a2017
SET vbd_id_key=sub.vbd_id_key,
    dist=sub.dist,
    dt=sub.dt
FROM (
  SELECT ais.id_key as ais_id_key,
	 vbd.id_key as vbd_id_key,
	 ST_Distance(ais.geom::geography,vbd.geom::geography) as dist,
	 abs(extract(epoch from ais.reportdate-vbd.date_mscan)) as dt,
	 min(ST_d=Distance(ais.geom::geography,vbd.geom::geography)) over (partition by ais.id_key) as min_dist
  FROM a2017 as ais
  JOIN g2017 as vbd
  ON st_distance(ais.geom::geography,vbd.geom::geography)<700
  AND abs(extract(second from ais.reportdate-vbd.date_mscan))<5
  WHERE ais.id_key like 'PRD%'
) sub
WHERE id_key=ais_id_key
AND sub.dist=sub.min_dist;
```