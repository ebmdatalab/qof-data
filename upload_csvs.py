import csv
import os
import sys
import tempfile
import warnings

from google.cloud.bigquery import Client, LoadJobConfig, SchemaField

ACHIEVEMENT_SCHEMA = [
    SchemaField("year", "STRING"),
    SchemaField("practice_id", "STRING"),
    SchemaField("indicator", "STRING"),
    SchemaField("achieved_points", "FLOAT"),
    SchemaField("register", "INTEGER"),
    SchemaField("numerator", "INTEGER"),
    SchemaField("denominator", "INTEGER"),
    SchemaField("exceptions", "INTEGER"),
]

PREVALENCE_SCHEMA = [
    SchemaField("year", "STRING"),
    SchemaField("practice_id", "STRING"),
    SchemaField("indicator_group", "STRING"),
    SchemaField("register", "INTEGER"),
    SchemaField("patient_list_type", "STRING"),
    SchemaField("patient_list_size", "INTEGER"),
]


def main(year):
    root_dir = os.path.dirname(os.path.abspath(__file__))
    achievement_path = os.path.join(root_dir, "data", year, "achievement.csv")
    print(achievement_path)
    prevalence_path = os.path.join(root_dir, "data", year, "prevalence.csv")

    assert os.path.exists(achievement_path)
    assert os.path.exists(prevalence_path)

    client = get_client()

    sql = "SELECT COUNT(*) FROM qof.achievement WHERE year = '{}'".format(year)
    rows = list(client.query(sql))
    if rows[0][0] > 0:
        print("There is already achievement data for {}".format(year))
        exit(1)

    sql = "SELECT COUNT(*) FROM qof.prevalence WHERE year = '{}'".format(year)
    rows = list(client.query(sql))
    if rows[0][0] > 0:
        print("There is already prevalence data for {}".format(year))
        exit(1)

    with tempfile.NamedTemporaryFile("w", delete=False) as tmp_f_t:
        # We need a file in text mode to write CSV data to...
        process_achievement_file(year, achievement_path, tmp_f_t)

    with open(tmp_f_t.name, "rb") as tmp_f_b:
        # ... but it needs to be in binary mode to upload to BQ.
        upload_achievement_data(client, tmp_f_b)

    os.unlink(tmp_f_t.name)

    with tempfile.NamedTemporaryFile("w", delete=False) as tmp_f_t:
        process_prevalence_file(year, prevalence_path, tmp_f_t)

    with open(tmp_f_t.name, "rb") as tmp_f_b:
        upload_prevalence_data(client, tmp_f_b)

    os.unlink(tmp_f_t.name)


def get_client():
    # If this raises a DefaultCredentialsError:
    #  * on a developer's machine, run `gcloud auth application-default login`
    #   to use OAuth
    #  * elsewhere, ensure that GOOGLE_APPLICATION_CREDENTIALS is set and
    #    points to a valid set of credentials for a service account
    #
    # A warning is raised when authenticating with OAuth, recommending that
    # server applications use a service account.  We can ignore this.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        client = Client(project="ebmdatalab", location="EU")

    client.create_dataset("qof", exists_ok=True)

    return client


def process_achievement_file(year, achievement_path, tmp_f):
    with open(achievement_path) as f:
        reader = csv.DictReader(f)
        writer = csv.DictWriter(
            tmp_f, fieldnames=[field.name for field in ACHIEVEMENT_SCHEMA]
        )

        new_row = {}

        for row in reader:
            if not new_row:
                new_row = {
                    "year": year,
                    "practice_id": row["PRACTICE_CODE"],
                    "indicator": row["INDICATOR_CODE"],
                }

            elif (
                row["PRACTICE_CODE"] != new_row["practice_id"]
                or row["INDICATOR_CODE"] != new_row["indicator"]
            ):
                writer.writerow(new_row)
                new_row = {
                    "year": year,
                    "practice_id": row["PRACTICE_CODE"],
                    "indicator": row["INDICATOR_CODE"],
                }

            new_row[row["MEASURE"].lower()] = row["VALUE"]

        writer.writerow(new_row)


def process_prevalence_file(year, prevalence_path, tmp_f):
    with open(prevalence_path) as f:
        reader = csv.reader(f)
        writer = csv.writer(tmp_f)

        next(reader)

        for row in reader:
            new_row = [year] + row
            writer.writerow(new_row)


def upload_achievement_data(client, tmp_f):
    client.load_table_from_file(
        tmp_f,
        "qof.achievement",
        rewind=True,
        job_config=LoadJobConfig(schema=ACHIEVEMENT_SCHEMA),
    )


def upload_prevalence_data(client, tmp_f):
    client.load_table_from_file(
        tmp_f,
        "qof.prevalence",
        rewind=True,
        job_config=LoadJobConfig(schema=PREVALENCE_SCHEMA),
    )


if __name__ == "__main__":
    main(sys.argv[1])
