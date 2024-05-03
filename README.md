# Introduction

Code for calculating Herfindahl Index of labour concentration by commuting zone and industry from Dun & Bradstreet (D&B) data from 1990. Index is also calculated using market share of sales by commuting zone and industry.

# Contents

Repository contains the following files:

- `main_zip.py`: Main file for computing Herfindahl Index by first matching David Dorn's 1990 commuting zone crosswalk file to a ZIP-FIPS mapping dataset and then matching the merged dataset to D&B's market participant data.

- `main_geocode.py`: Main file for computing Herfindahl Index by first geocoding D&B addresses using the United States Census Bureau API to find corresponding FIPS codes. Then D&B data is matched to David Dorn's commuting zone crosswalk file. Geocoding is necessary since D&B data only contains ZIP codes.

- `utils.py`: Utility file containing all lower-level functions called in main files. 

- `config.py`: Configuration file containing file path constants for reading and writing data. 

# References

Commuting zone crosswalk files can be found on David Dorn's [website](https://www.ddorn.net/data.htm#Local%20Labor%20Market%20Geography). Analysis makes use of his [E7](https://www.ddorn.net/data/cw_cty_czone.zip) 1990 Counties to 1990 Commuting Zones crosswalk dataset.

David Autor and David Dorn. "The Growth of Low Skill Service Jobs and the Polarization of the U.S. Labor Market."
American Economic Review, 103(5), 1553-1597, 2013.

API documentation for the United States Census Bureau Geocoder can be found [here](https://geocoding.geo.census.gov/geocoder/Geocoding_Services_API.html).
