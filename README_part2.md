# Data Engineer Screening – Practical Exercise

## Expectations

- Use Python for scripting.
- Organize your code for readability and maintainability.



*Focus on demonstrating your engineering approach and code quality. You do not need to build a production-ready system, but your code should be (theoretically) runnable and easy to understand.*

## Coding Task

Please implement a simple data pipeline based on the scenario described in part 1. Your pipeline should:

1. **Use Airflow for orchestration**
    - Implement an Airflow DAG (in `dags/flight_data.py`) that coordinates subsequent steps 2-5.
    - Ensure tasks are modular and dependencies are clearly defined.

2. **Pull flight data from the AviationStack public API**
    - Use the [Flight endpoint](https://aviationstack.com/documentation).
    - In `src/aviation_stack.py`, create function(s) to call the AviationStack API and retrieve a sample of recent flight data.
    - Data needed by end users includes: `flight date, flight status, departure and arrival airports, and airline name`

3. **Store the API output in a Google Cloud Storage (GCS) bucket**
    - Save the data as JSON or CSV in GCS
    - Organize files by date or another logical partition.
    - Review functions in `src/gcs.py` - discuss any modifications that you would make in this module to effectively accomplish this task.

4. **Load the data into a Snowflake table**
    - Assume a table exists in our data warehouse whose schema matches that of your API output.
    - Review `src/snowflake.py` module and discuss implementation details. How would you implement this task of loading the retreived information into an existing table effeciently, and ensuring idempotency?

5. **Trigger a dbt transformation**
    - Trigger a build of models in the `dbt_models/` directory


6. **(Optional) Implement a testing framework**
    - Add at least one unit or integration test for your pipeline code.


