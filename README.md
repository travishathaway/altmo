# AltMo

**Alt**ernative **Mo**bility is a CLI tool which helps map alternative mobilities with Open Street Map data. 

Requirements for this project are:
- PostgreSQL with PostGIS
- Open Street Map data imported to PostgreSQL for the area you want to analyze
- A running instance of Vahalla (route generation tool)
- A list of amenity inventory (more on this below)
- A geojson file of the boundary you would like to gather data for (should fit inside OSM data)

You will also need to set the environment variable `ALTMO_PG_DSN`. This is the PostgreSQL connection string
and should be pointed at the database with the OSM data (make sure the user has `CREATE` and `DROP` privileges).

## Creating an amenity inventory

Amenities are places where people travel to. Some examples of this include, schools, churches, supermarkets
and workplaces. Your amenity inventory will let the build process now which data it should be selecting from
the Open Street Map database.

Here what an `inventory.txt` file could look like:

```
supermarkets
schools
universities
```