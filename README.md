# Data Engineer Screening Exercise (60 minutes)

## Overview

This is a **live coding interview** evaluating four core competencies:
1. **System design thinking and communication** (Part 1)
2. **Implementation skills** - writing clean, functional code (Part 2)
3. **Code review skills** - identifying security and data issues (Part 3)
4. **Orchestration design** - building production-ready workflows (Part 4)

**Setup:** You may use the internet, documentation, and AI assistants as you normally would while coding.

---

# PART 1: Architecture Discussion (10 minutes)

> **⚠️ NOTE TO INTERVIEWEE:** Complete this entire discussion section before moving to Part 2 (Implementation).

## Scenario

Business stakeholders are asking for data from [Aviation Stack](https://aviationstack.com/documentation) to be available via internal self-service BI tooling. This data will allow for performance tracking and monitoring of our different airline partners.

**Context:** You're joining an existing data team. We already have:
- Airflow for orchestration
- Snowflake as our data warehouse
- dbt for transformations
- GCS for staging/raw data storage
- Looker for BI (connected to Snowflake)

## Your Task

**Walk us through how you would approach this request.**

Think through the entire lifecycle: from gathering requirements, to designing the architecture, to implementing and operationalizing the pipeline.

We're interested in understanding:
- Your thought process and how you break down ambiguous problems
- What questions you'd ask stakeholders and why
- Which high-level architecture you'd choose
- What technical tradeoffs you'd consider
- How you'd ensure this runs reliably in production

*This is an open conversation. We're evaluating your communication, critical thinking, and how you approach real-world data engineering problems.*

---

# PART 2: API Implementation (15 minutes)

**Implement the API extraction function** in `src/aviation_stack.py`:

1. Write a function to fetch flight data from the [AviationStack Flights API](https://aviationstack.com/documentation)
2. Extract these fields for end users: `flight_date`, `flight_status`, `departure airport`, `arrival airport`, `airline_name`
3. Return the data in a format suitable for downstream storage (JSON or structured dict/list)
4. Handle API errors appropriately
5. *Note: You can use a free API key or mock the response - just document your choice*

**Success criteria:**
- Function is callable and returns structured data
- Code handles basic error cases
- Code is readable with appropriate variable names

---

# PART 3: Code Review (10 minutes)

Review the existing code in `src/snowflake.py`. **This module contains intentional issues that a senior engineer should identify.**

**Tasks:**
1. **Identify problems** in the implementation (security, correctness, efficiency, best practices)
2. **Document your findings** - add comments directly in the code explaining what's wrong
3. **Fix any critical issue** - Address at least 1 critical issue

**What we're looking for:**
- Do you understand idempotency and how to implement it?
- Can you prioritize critical vs. minor issues?

*Note: `src/gcs.py` also has issues, but focus on snowflake.py for time constraints.*

---

# PART 4: Airflow DAG Implementation (20 minutes)

In `dags/flight_data.py`, implement a production-ready Airflow DAG that orchestrates the complete pipeline.

**Requirements:**
1. **Create the DAG** with appropriate schedule, retries, and configuration
2. **Implement these tasks:**
   - `extract_flight_data` - Call your aviation_stack.py function
   - `upload_to_gcs` - Save data to GCS with date-based partitioning
   - `load_to_snowflake` - Load from GCS to Snowflake (using your idempotent approach)
   - `run_dbt` - Trigger dbt models
3. **Define task dependencies** clearly
4. **Add appropriate error handling** - What happens if the API fails? If Snowflake is down?
5. **Include data quality checks** - At least one validation step

**Success criteria:**
- DAG is syntactically correct (or close enough to be runnable with minor fixes)
- Task dependencies are logical and correct
- Shows understanding of Airflow best practices (task isolation, XCom, etc.)
- Demonstrates production thinking (retries, timeouts, failure handling)

**Hints:**
- Use `PythonOperator` or `@task` decorator for custom tasks
- Use `BashOperator` for dbt or consider `DbtCloudRunJobOperator`
- Think about how to pass data between tasks (XCom vs. external storage)
- Consider using Airflow connections for credentials

---

# PART 5: Wrap-up Discussion (5 minutes)

Be prepared to discuss:
- What would you add with more time?
- How would you monitor this pipeline in production?
- What data quality checks would you implement?
- How would you handle schema changes in the API?

---

## Interview Structure Summary

| Part | Duration | Focus |
|------|----------|-------|
| 1. Architecture Discussion | 10 min | Requirements, tradeoffs, system design |
| 2. API Implementation | 15 min | Writing clean code with error handling |
| 3. Code Review | 10 min | Identifying security/correctness issues |
| 4. Airflow DAG | 20 min | Production-ready orchestration |
| 5. Wrap-up | 5 min | Production concerns & extensions |
| **Total** | **60 min** | |
