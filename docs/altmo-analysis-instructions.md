# AltMo Analysis Instructions

## Summary

This document is meant to give myself detailed instructions for how to do the analysis process for AltMo. In essence, it requires first choosing a city/study area, collecting the necessary data for and then running through the steps in order. These steps and data collection are detailed below.

## Selecting a city/study area

When selecting a city or study area within a city, the most important thing to consider is whether or not this region has good coverage in Open Street Map (OSM) database. Choosing cities and areas which do will greatly improve the accuracy of the final output. Another thing to keep in mind is the size of the area in terms of total amenities and residential buildings. This will effect how long the various route finding algorithms will take to run.

For my initial study with Kiel, I had about 60,000 residences and 5,500 amenities. I found the run times network distance calculating scripts to be about 30 minutes.

## Collecting the data

If you are setting this up for the first time, you will need to create a PostgreSQL database and create the following extensions:

```sql
CREATE EXTENSION postgis;
CREATE EXTENSION hstore;
```

After that, you will want to download the necessary OSM data. One of the best services is https://download.geofabrik.de/. Using this service you will want to download a `*.osm.pbf` file and then import it in to your database using the following command:

```
osm2pgsql -U <db_user>  -H <db_host> -d <db_name> -W --number-processes <num_cores> --hstore <osm_pbf_data_file>
```

## Setting up valhalla (network routing engine)

The analysis process also relies on a routing service that you install on your own computer called Valhalla. For me, the easiest way to do this was by using Docker and a `docker-compose.yml` file that I found by searching the internet. Check out this article for more information on setting that up: https://gis-ops.com/valhalla-how-to-run-with-docker-on-ubuntu/

## Install the `altmo` CLI

The first step is cloning the `altmo` git repository:

```
git clone git@github.com:travishathaway/altmo.git 
```

A prerequisite for using the `altmo` CLI locally is installing a program called `poetry`. Head over here to learn more about installing and running that: https://python-poetry.org/.

Once you have poetry up and running, you can install `altmo` with the following command:

```
poetry install
```

Afterwards, verify the `altmo` command is work by running:

```
poetry run altmo --help
```

(to avoid prefixing commands with `poetry run`, you launch a new shell with `poetry shell`)

## Initializing our database tables

In addition to the table we created while running the OSM import, we will also be creating another set of tables to run our analysis. To do this, we first need to store our database connect string as an environment variable (we also store the URL for connect to our routing server Valhalla this way too):

```
export ALTMO_PG_DSN='dbname=database user=username host=localhost port=5432 password=password'
export ALTMO_VALHALLA_SERVER='http://localhost:8002'
```

After setting up a valid connection, we can add the tables we will need with the following command:

```
altmo schema
```

If we need to reset the database later, we use the following command to remove our tables:

```
altmo schema --drop
```

## Running the analysis

With all of this in place, we are now ready to run the analysis itself. This consist of identifying the residences and amenities in OSM data, saving that in our tables, and the calculating the network distance between the two so we can collect time estimates.

Before doing this, we first need to designate a study area in our database. This will define the geographic extent of our study area and help with extracting data from OSM. This file should be a single GeoJSON file containing on polygon defining the study area.

Once you have this file, you can import it with the following command:

```
altmo csa boundary.geojson "<study_area_name>" "<study_area_description>"
```

Be sure to note the name you gave for the `study_area_name` parameter as we will be using this repeatedly for the other commands.

To collect all the amenities and residences for the study area, we run the following command:

```
altmo build <study_area_name>
```


