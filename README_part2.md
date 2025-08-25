# Data Engineer Screening – Practical Exercise

## Coding Task

Please implement a simple data pipeline based on the scenario described in part 1. Your pipeline should:

1. **Use Airflow for orchestration**
    - Implement an Airflow DAG (in `dags/flight_data.py`) that coordinates the following steps.
    - Ensure tasks are modular and dependencies are clearly defined.
    - Include basic scheduling, error handling, and configuration via environment variables.

2. **Pull flight data from the AviationStack public API**
    - Use the [Flight endpoint](https://aviationstack.com/documentation).
    - In `src/aviation_stack.py`, create function(s) to call the AviationStack API and retrieve a sample of recent flight data.

3. **Store the API output in a Google Cloud Storage (GCS) bucket**
    - Save the data as JSON or CSV in GCS
    - Organize files by date or another logical partition.
    - Review functions in `src/gcs.py` - discuss any modifications that you would make in this module to effectively accomplish this task.

4. **Load the data into a Snowflake table**
    - Create a table schema that matches the API output.
    - Load the staged data from GCS into Snowflake.
    - Review `src/snowflake.py` module and discuss implementation details.

5. **Trigger a dbt transformation**
    - Trigger a build of models in the `dbt_models/` directory


6. **(Optional) Implement a testing framework**
    - Add at least one unit or integration test for your pipeline code.

## Expectations

- Use Python for scripting.
- Organize your code for readability and maintainability.
- Add docstrings and comments where appropriate.



*Focus on demonstrating your engineering approach and code quality. You do not need to build a production-ready system, but your code should be (theoretically) runnable and easy to understand.*
