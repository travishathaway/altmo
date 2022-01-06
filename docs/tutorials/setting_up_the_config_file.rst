Setting Up the Config File
==========================

Here's an example of a AltMo config file. Each top level setting will be
explained in detail further below.



.. code:: yaml

    # Service configurations
    PG_DSN: 'dbname=altmo_database user=postgres host=localhost port=5432 password=password'
    TBL_PREFIX: 'altmo\_'
    SRS_ID: 3857
    VALHALLA_SERVER: 'http://localhost:8003'

    # Amenity configuration
    AMENITIES:
      include_natural_amenities: true
      categories:
        school:
          kindergarten: { weight: 0.15 }
          childcare: { weight: 0.2 }
          university: { weight: 0.175 }
          music_school: { weight: 0.075 }
        shopping:
          marketplace: { weight: 0.111 }
          hairdresser: { weight: 0.111 }
          clothes: { weight: 0.111 }

**Tip:** The config above is truncated for demonstration purposes. To see a full example,
`head over to Github <https://github.com/travishathaway/altmo/blob/main/examples/altmo-config.yml>`_.

PG_DSN
######

This is the connection string to the PostgreSQL server. This databas should be setup according to the
:ref:`Getting Started` guide (i.e. by enabling required ``tablefunc``, ``postgis`` and ``hstore`` extensions).

TBL_PREFIX
##########

This is the table prefix used when creating the AltMo schema. This can be useful for quickly
differentiating between OSM and AltMo tables. Default value is ``altmo_``.

SRS_ID
######

This is the project SRS_ID and is set by default to 3857 which uses meters as its unit of measurement.

VALHALLA_SERVER
###############

This is the URL of the running Valhalla routing server to use. If no instance is available to you,
it is advised to set up your own instance by
`following the instructions at gis-ops.com <https://gis-ops.com/valhalla-how-to-run-with-docker-on-ubuntu/>`_.


AMENITIES
#########

These are the amenities that will be included in the analysis. For a list of all available amenities,
`see the example file on Github <https://github.com/travishathaway/altmo/blob/main/examples/altmo-config.yml>`_.

The amenities are grouped by a top level category. When calculating a the total weighted average, these
categories are equal. When running the ``altmo build`` command, only the amenities here are included.
This means that you can do an accessibility analysis for only a subset of these categories for your study
area.

Additionally, each amenity can be assigned a weight. This weight will either boost or reduce the amenity's
relative importance in its category.
