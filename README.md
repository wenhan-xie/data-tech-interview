# Hiive Analytics Engineer Case Study

While the case study reference Hiive, the scenarios, processes, and data are fictitious. This case study is the property of The Hiive Company Limited, is confidential and is not to be disseminated without Hiive’s prior consent. 

This repo is a starting code base for the Hiive Analytics Engineering case study. The tasks to complete for the case study were provided to you separately by the Hiive recruitment team.

The case study is designed to test your ability to work with data and dbt code. The case study is designed to be completed in 2-3 hours.

This repo uses DuckDB as a database, but you can use any database you like (see section below for other databases).

## Getting started

To start working on the case study you need to do the following:

1. Fork this repo into your own account
1. Clone the repo to your local machine

    ```
    git clone https://github.com/hiivemarkets/data-tech-interview
    ```
1. Install the requirements

    ```
    # create a virtual environment
    python3 -m venv venv
    source venv/bin/activate

    # install the packages
    pip install -r requirements.txt
    ```
1. Check that everything is working by running:

    ```
    dbt debug
    ```
    You should see a message `All checks passed!`.
1. Seed the starting data:

    ```
    dbt seed
    ```
    Those will be materialized in main schema in the database, e.g `main.transaction_termination_reasons_seed`.

## Working with other databases

If you want to work with other databases. You can do that by changing two things:

1. Install required adapter, e.g. for Postgres:

    ```
    pip install dbt-postgres
    ```
2. Change the database connection in the `profiles.yml` file. You can find more information on how to do this in the [dbt documentation](https://docs.getdbt.com/docs/profiles).

> ⚠️ Don't commit your database credentials to the repository!

## Data description

You can read more about the input data in the [data description](data_description.md) file.

## Task 1 Answers

See [TASK1_ANSWERS.md](TASK1_ANSWERS.md) for complete answers to all 5 questions.

