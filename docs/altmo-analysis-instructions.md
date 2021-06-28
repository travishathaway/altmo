# AltMo Analysis Instructions

## Summary

This document is meant to give myself detailed instructions for how to do the analysis process for AltMo. In essence, 
it requires first choosing a city/study area, collecting the necessary data for and then running through the steps in 
order. These steps and data collection are detailed below.

## Selecting a city/study area

When selecting a city or study area within a city, the most important thing to consider is whether or not this region 
has good coverage in Open Street Map (OSM) database. Choosing cities and areas which do will greatly improve the 
accuracy of the final output. Another thing to keep in mind is the size of the area in terms of total amenities and 
residential buildings. This will effect how long the various route finding algorithms will take to run.

For my initial study with Kiel, I had about 60,000 residences and 5,500 amenities. I found the run times network
distance calculating scripts to be about 30 minutes.

## Collecting the data

If you are setting this up for the first time, you will need to create a PostgreSQL database and create the following
extensions:

```sql
CREATE EXTENSION postgis;
CREATE EXTENSION hstore;
```

After that, you will want to download the necessary OSM data. One of the best services is 
[https://download.geofabrik.de/](https://download.geofabrik.de/). Using this service you will want to download a 
`*.osm.pbf` file and then import it in to your database using the following command:

```bash
osm2pgsql -U <db_user>  -H <db_host> -d <db_name> -W --number-processes <num_cores> --hstore <osm_pbf_data_file>
```

## Setting up valhalla (network routing engine)

The analysis process also relies on a routing service that you install on your own computer called Valhalla. For me,
the easiest way to do this was by using Docker and a `docker-compose.yml` file that I found by searching the internet.
Check out this article for more information on setting that up: 
[https://gis-ops.com/valhalla-how-to-run-with-docker-on-ubuntu/](https://gis-ops.com/valhalla-how-to-run-with-docker-on-ubuntu/).

## Install the `altmo` CLI

The first step is cloning the `altmo` git repository:

```bash
git clone git@github.com:travishathaway/altmo.git 
```

A prerequisite for using the `altmo` CLI locally is installing a program called `poetry`. Head over here to learn more 
about installing and running that: https://python-poetry.org/.

Once you have poetry up and running, you can install `altmo` with the following command:

```bash
poetry install
```

Afterwards, verify the `altmo` command is work by running:

```bash
poetry run altmo --help
```

(to avoid prefixing commands with `poetry run`, you launch a new shell with `poetry shell`)

## Initializing our database tables

In addition to the table we created while running the OSM import, we will also be creating another set of tables to run
our analysis. To do this, we first need to store our database connect string as an environment variable (we also store
the URL for connect to our routing server Valhalla this way too):

```bash
export ALTMO_PG_DSN='dbname=database user=username host=localhost port=5432 password=password'
export ALTMO_VALHALLA_SERVER='http://localhost:8002'
```

After setting up a valid connection, we can add the tables we will need with the following command:

```bash
altmo schema
```

If we need to reset the database later, we use the following command to remove our tables:

```bash
altmo schema --drop
```

## Running the analysis

With all of this in place, we are now ready to run the analysis itself. This consist of identifying the residences and
amenities in OSM data, saving that in our tables, and the calculating the network distance between the two, so we can
collect time estimates.

Before doing this, we first need to designate a study area in our database. This will define the geographic extent of
our study area and help with extracting data from OSM. This file should be a single GeoJSON file containing on polygon
defining the study area.

Once you have this file, you can import it with the following command:

```bash
altmo csa boundary.geojson "<study_area_name>" "<study_area_description>"
```

Be sure to note the name you gave for the `study_area_name` parameter as we will be using this repeatedly for the other
commands.

To collect all the amenities and residences for the study area, we run the following command:

```bash
altmo build <study_area_name>
```

Now that we have collected all of our residences and amenities for the analysis (you can manually check the `amenities`
and `residences` tables to see exactly what's in there), we need to calculate euclidean (as the crow flies) distances
between residences and the nearest amenities. We do this to make the process of find the network distances faster.
The following command will calculate these distances for each residence and amenity type and store the three closest
amenities to that residence:

```bash
altmo straight <study_area_name> --show-status
```

The `--show-status` flag will show a progress bar. Leave this flag off if you do not want to show the progress bar.

We have now populated a table called `residence_amenity_distances_straight`, which holds the aforementioned data.

The longest step comes next, and this is the step where we calculate the network distances for our residence amenity
pairs. This is accomplished with the following command:

```bash
altmo network <study_area_name> --processes <num_processes> --mode <mode:walking,bicycle>
```

This command has a couple different options. `--processes` determines how many parallel processes will be run
when calculating the network distance. Typically, this should not be more than the number of processors on your
computer and also only makes it faster if the Valhalla server has sufficient resources (i.e. it is also  running with 
multiple processes available).

We now have distances and more importantly estimated times from each residence to the top three nearest amenities.
At this point, we can calculate our z-scores and weighted averages for time and distance. We do this with the following
two commands:

```bash
altmo zscores <study_area_name> --mode <mode:walking,bicycle> -l single
```

This calculates the unweighted z-scores for each residence as well as average time and average distance and saves it in
a table called `residence_amenity_distance_standardized`.

The next command stores the categorized and weighted averages and z-scores for time:

```bash
altmo zscores <study_area_name> --mode <mode:walking,bicycle> -l category
```

Congratulations! You have just finished the last step! Next we will cover exporting and visualizing the results.

## Exporting and visualizing

There are two methods for export the data we have created so far. These methods will either export the data we have
created as raster or vector data types. The raster data type provides a broad overview of the study area
to see regional patterns, whereas vector data provides data on single points.

Use the the following command to command to export a raster data set (GTiff file):

```bash
altmo raster <study_area_name> <outfile> -r 50 -f all_average_time
```

The following command will export the vector data as GeoJSON:

```bash
altmo export <study_area_name> > test.geojson
```