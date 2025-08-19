# Data Engineer Screening

## Scenario

Business stakeholders are asking for data from Aviation Stack to be available via internal self-service BI tooling. At a high level, walk us through how you would architect an end-to-end data pipeline to accomplish this, ensuring to mention specific questions or considerations that you’d need to resolve in order to build the pipeline effectively. There are 5 key parts of this pipeline that need to be built:


1. Pull flight data down from public API (Aviation Stack)
    * https://aviationstack.com/documentation
    * Use Flight endpoint
2. Load/store/stage API output in GCS bucket
3. Load data into Snowflake table
4. Trigger DBT transformation
5. If you have time, implement testing framework
