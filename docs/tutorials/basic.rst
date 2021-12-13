AltMo Basic Tutorial
====================

Summary
-------

This document is meant to give detailed instructions for how to do analysis
using the AltMo CLI tool. This process requires first choosing a
city/study area, collecting necessary data for it and then running
the available AltMo CLI commands.

The instructions below will assume you are running a Unix like operating system.
If you are running Windows, I suggest running these commands in a virtual
machine or a Docker container.

Selecting a city/study area
---------------------------

When selecting a city or study area within a city, the most important
thing to consider is whether or not this region has good coverage in the
Open Street Map (OSM) database. Choosing cities and areas which do will
greatly improve the accuracy of the final output. Another thing to keep
in mind is the size of the area in terms of total amenities and
residential buildings. This will effect how long the various route
finding algorithms will take to run.

For my initial study with Kiel, I had about 60,000 residences and 5,500
amenities. In total, running all the commands in sequence took about 30
minutes.

Collecting the data and setting up our database
-----------------------------------------------

If you are setting this up for the first time, you will need to create a
PostgreSQL database and create the following extensions:

.. code:: sql

   CREATE EXTENSION postgis;
   CREATE EXTENSION hstore;
   CREATE EXTENSION tablefunc;

After that, you must download the necessary OSM data. One of the
best services for this is https://download.geofabrik.de/. When using this
service, you will download a ``*.osm.pbf`` file and then import it in to your
database using the following command:

.. code:: bash

   osm2pgsql -U <db_user>  -H <db_host> -d <db_name> -W --number-processes <num_cores> --hstore <osm_pbf_data_file>

Setting up Valhalla (network routing engine)
--------------------------------------------

The analysis process also relies on a routing service that you install
on your own computer called Valhalla. One of the easiest ways to set this
up on your own computer is by running the docker containers. Detailed
installation instructions for this can be found here:

- https://gis-ops.com/valhalla-how-to-run-with-docker-on-ubuntu/.

To learn more about Vahalla, head over to their documentation:

- https://www.interline.io/valhalla/

Install the ``altmo`` CLI
-------------------------

With our OSM data now in place and a Vahalla instance running,
we can install the CLI tool that will help us run the rest of our
analysis.

You can do this by first creating a virtualenv and then installing
the ``altmo`` package via ``pip``:

.. code:: bash

    python -m venv ~/.virtualenvs/altmo
    source ~/.virtualenvs/altmo/bin/activate
    pip install altmo

Afterwards, verify the ``altmo`` command is work by running:

.. code:: bash

   altmo --help

Creating a config file
----------------------

The config file for your altmo project contains the following

- ``pg_dsn``: connection information for your PostgreSQL server
- ``valhalla_server``: connection information for the Valhalla server you are running
- ``amenities``: information about the amenities you want used in the analysis

When you run the ``altmo`` CLI tool it will, by default, look in the current directory
for a file named ``altmo-config.yml``. Download a template version of this file here:

- https://github.com/travishathaway/altmo/blob/main/examples/altmo-config.yml

Fill in the values ``pg_dsn`` and ``valhalla_server`` to suit your environment.
``amenities`` will be explained in detail later.

Initializing the database
--------------------------------

In addition to the tables we created while running the OSM import, we
will also be creating another set of tables to run our analysis. We can
add these tables with the following command:

.. code:: bash

   altmo schema

If we need to reset the database later, we can use the following command to
remove our tables (it does not remove OSM tables or data):

.. code:: bash

   altmo schema --drop

Running the analysis
--------------------

With all of this in place, we are now ready to run the analysis itself.
This consist of identifying the residences and amenities in OSM data,
saving a copy of that to the AltMo tables, and calculating the network distance
between the two, so we can save the time estimates.

Before doing this, we first need to create a study area in our
database. This will define the geographic extent of our study area and
help with extracting data from OSM. This file should be a single GeoJSON
file containing one polygon defining the study area.

Once you have this file, you can import it with the following command:

.. code:: bash

   altmo csa boundary.geojson "<study_area_name>" "<study_area_description>" <srs_id:3857>

The ``study_area_name`` parameter should be a short hand reference to the study
area containing no spaces (e.g. ``chicago_south_side`` or ``brooklyn``). Be sure
to note the name you give for this parameter as we will be using this again
for the other commands.

To collect all the amenities and residences for the study area, we run
the following command:

.. code:: bash

   altmo build <study_area_name>

Now that we have collected all of our residences and amenities for the
analysis (you can manually check the ``amenities`` and ``residences``
tables to see exactly what’s in there), we need to calculate euclidean
(as the crow flies) distances between residences and the nearest
amenities. We do this to make the process of finding the network
distances faster. The following command will calculate these distances
for each residence and amenity type and save the three closest
amenities to that residence:

.. code:: bash

   altmo straight <study_area_name> --show-status

The ``--show-status`` flag will show a progress bar. Leave this flag off
if you do not want to show the progress bar.

We have now populated a table called
``residence_amenity_distances_straight``, which holds the aforementioned
data.

The longest step comes next, and this is the step where we calculate the
network distances for our residence amenity pairs. This is accomplished
with the following command:

.. code:: bash

   altmo network <study_area_name> --processes <num_processes> --mode <mode:pedestrian,bicycle>

This command has a couple different options. ``--processes`` determines
how many parallel processes will be run when calculating the network
distance. Typically, this should not be more than the number of
processors on your computer and also only makes it faster if the
Valhalla server has sufficient resources (i.e. it is also running with
multiple processes available).

``...@ ...@ ...@ ...@  (some time passes...)``

Congratulations! You have just finished the last step! Next we will
cover exporting and visualizing the results.

Exporting and visualizing
-------------------------

There are two methods for exporting the data we have created so far. These
methods will either export the data we have created as raster or vector
data types. The raster data type provides a broad overview of the study
area to see regional patterns, whereas vector data provides data on
single points.

Use the the following command to export a raster data set (GTiff file):

.. code:: bash

   altmo raster <study_area_name> <outfile> -r 50 -f all

Available choices are the ``categories`` defined in you ``altmo-config.yml`` file.
The example ``altmo-config.yml`` has the following categories defined:

- school
- shopping
- groceries
- administrative
- health
- community
- outing_destination
- nature

The following commands will export the vector data as GeoJSON:

.. code:: bash

    # This command will export everything in a single GeoJSON file
    altmo export <study_area_name> all --srs-id 4236  --mode pedestrian > all.json

    # You can also narrow down what to include in this file with the '--properties' option
    altmo export <study_area_name> all --srs-id 4236  --mode pedestrian --properties 'all,groceries,shopping' > all.json

    # This command will export all residences as separate files in the specified export directory
    altmo export <study_area_name> single_residence --srs-id 4236 --mode pedestrian --export-dir export_data

Make it even better with QGIS!
------------------------------

In order to make the out put ready for display on the web, there are a
couple more steps that can be performed in QGIS:

1. Use IDW interpolation with the exported GeoJSON file (this takes the
   longest)
2. Clip this raster using a buffer (250m) (CLI tool to help this go
   faster)
3. Apply appropriate styling (color ramp)
4. Make web tiles
