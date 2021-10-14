# WORKLOG

## 2021-06-06

Just finished a couple commands. It would be nice to explore adding the build command to the schema create command.

## 2021-06-07

Thought about adding the build stuff to the schema command a little more and it makes more sense to leave it as-is.
This is because we are "building" a single study area's data.

## 2021-07-01

Came across a weird bug today with the bicycle mode distance records. I'm seeing a lot of values that are really
high for short distances. The quick fix is just setting these values to a low amount of seconds (less 3 minutes).
The long term fix is change the time column to not big "BIGINT" and then re-run the network distance stuff again
to see exactly which requests are returning this weird data.