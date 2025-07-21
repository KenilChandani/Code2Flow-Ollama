--DELETE  FROM experiments WHERE experiment_id =85
--
--
--
--DELETE  FROM tags t
--WHERE t.run_uuid IN ('ae354a81bc3546d99e0321155c885fef','bffa0d9f15884b0b8196a25436376372')
--
--DELETE  FROM runs t
--WHERE t.run_uuid IN ('ae354a81bc3546d99e0321155c885fef','bffa0d9f15884b0b8196a25436376372')
--
--DELETE  FROM latest_metrics t
--WHERE t.run_uuid IN ('ae354a81bc3546d99e0321155c885fef','bffa0d9f15884b0b8196a25436376372')
--
--DELETE  FROM metrics t
--WHERE t.run_uuid IN ('ae354a81bc3546d99e0321155c885fef','bffa0d9f15884b0b8196a25436376372')
--
--DELETE  FROM params t
--WHERE t.run_uuid IN ('ae354a81bc3546d99e0321155c885fef','bffa0d9f15884b0b8196a25436376372')



SELECT t.* FROM runs r ,tags t 
WHERE r.run_uuid = t.run_uuid AND (lifecycle_stage ='deleted' OR status='FAILED')

SELECT lm.* FROM runs r ,latest_metrics lm  
WHERE r.run_uuid = lm.run_uuid AND (lifecycle_stage ='deleted' OR status='FAILED')

SELECT m.* FROM runs r ,metrics m 
WHERE r.run_uuid = m.run_uuid AND (lifecycle_stage ='deleted' OR status='FAILED')

SELECT p.* FROM runs r ,params p  
WHERE r.run_uuid = p.run_uuid AND (lifecycle_stage ='deleted' OR status='FAILED')


