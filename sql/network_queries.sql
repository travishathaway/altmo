SELECT
	n.residence_id, n.amenity_id, n.distance,
	ST_Distance(ST_Transform(r.geom, 4326)::geography, ST_Transform(am.geom, 4326)::geography) / 1000
FROM
	residence_amenity_distances n
JOIN
	residences r
ON
	n.residence_id = r.id
JOIN
	amenities am
ON
	n.amenity_id = am.id;