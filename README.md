# AltMo

**Alt**ernative **Mo**bilities is a CLI tool which helps map alternative mobilities with Open Street Map data.
Specifically, this tool helps you map walkability and bikeability averages as a surface for an area of intent
(usually a city or a region).

It relies on the following external services to work:

- A PostgreSQL database within extensions `postgis`, `hstore` and `tablefunc` enabled
- An Open Street Map database imported into this database
- A running instance a [Vahalla](https://valhalla.readthedocs.io/en/latest/) (used for calculating network routing)
- A GeoJSON file of the boundary you would like to gather data for (should fit inside OSM data)

For a full description of how to use this tool, you are encouraged to visit 
[https://altmo.readthedocs.io/en/latest/index.html](https://altmo.readthedocs.io/en/latest/index.html).
