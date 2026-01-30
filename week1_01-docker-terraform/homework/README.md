# Question 1 - executeed command line
```
docker run -it --rm --entrypoint=bash python:3.13
pip --version

# pip 25.3 from /usr/local/lib/python3.13/site-packages/pip (python 3.13)
```

# Question 2
No code needed


# Setup postgres database for Question 3 onwards
```
# Terminal 1
docker compose run

# Terminal 2
docker run -it --rm\
  --network=homework_default \
  homework:q3 \
    --pg-user=root \
    --pg-pass=root \
    --pg-host=pgdatabase \
    --pg-port=5432 \
    --pg-db=ny_taxi
```

# Question 3 - executeed sql
```
select count(*)
from green_tripdata
where lpep_pickup_datetime between date'2025-11-01' and date'2025-12-01'
and trip_distance <= 1
```

# Question 4 - executed sql
```
with longest_trip_distance as (
	select max(trip_distance) as longest_trip_distance
	from green_tripdata
	where trip_distance <= 100
	)

select lpep_pickup_datetime
from green_tripdata
where trip_distance = (select longest_trip_distance from longest_trip_distance)
```

# Question 5 - executed sql
```
select
	b."Zone"
  , sum(total_amount) as total_amount
from green_tripdata a
left join taxi_zone_lookup b
	on a."PULocationID" = b."LocationID"
where lpep_pickup_datetime::date = date'2025-11-18'
group by 1
order by 2 desc
```

# Question 6 - executed sql
```
select
	c."Zone" as DOZone
  , max(tip_amount) as largest_tip_amount
from green_tripdata a
left join taxi_zone_lookup b
	on a."PULocationID" = b."LocationID"
left join taxi_zone_lookup c
	on a."DOLocationID" = c."LocationID"
where date_trunc('month', lpep_pickup_datetime) = date'2025-11-01'
and b."Zone" = 'East Harlem North'
group by 1
order by 2 desc
```

# Question 7
No code needed