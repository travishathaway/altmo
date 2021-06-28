# AltMo

**Alt**ernative **Mo**bility is a CLI tool which helps map alternative mobilities with Open Street Map data. 

Requirements for this project are:
- PostgreSQL with PostGIS
- Open Street Map (OSM) data imported to PostgreSQL for the area you want to analyze
- A running instance of Vahalla (route generation tool)
- ~~A list of amenity inventory (more on this below)~~ (maybe later)
- A geojson file of the boundary you would like to gather data for (should fit inside OSM data)

You will also need to set the environment variable `ALTMO_PG_DSN` and `ALTMO_VALHALLA_SERVER`. This is the PostgreSQL 
connection string and the URL of the running instance of Valhalla. The database connector should be pointed at the
database with the OSM data (make sure the user has `CREATE` and `DROP` privileges).

For more detailed instructions on how to use all the commands in this project, see 
[AltMo Analysis Instructions](./docs/altmo-analysis-instructions.md).