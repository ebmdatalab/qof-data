# qof-data

This repo contains a script, `upload_csvs.py`, to upload QOF data to BigQuery.

Achievement and prevalence data is uploaded to tables in `qof` dataset of the `ebmdatalab` project.

To add data from 2019-20:

* visit https://digital.nhs.uk/data-and-information/publications/statistical/quality-and-outcomes-framework-achievement-prevalence-and-exceptions-data
* follow the link to 2019-20, and find a link to download a zip file containing raw CSV files
* download this zip file and unzip it
* create a directory in this repo called `data/1920`
* in the downloaded CSV files, find a file called something like `PREVALENCE.CSV` with headers `PRACTICE_CODE`, `INDICATOR_GROUP_CODE`, `REGISTER`, `PATIENT_LIST_TYPE`, `PATIENT_LIST_SIZE`
* copy this to `data/1920/prevalence.csv`
* in the downloaded CSV files, find a file called something like `ACHIEVEMENT.CSV` with headers `PRACTICE_CODE`, `INDICATOR_CODE`, `MEASURE`, `VALUE`
* copy this to `data/1920/achievement.csv`
* run `python upload_csvs.py 1920`
