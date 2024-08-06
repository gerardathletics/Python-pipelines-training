# Python Data Pipelines Course

Welcome to my Python Data Pipelines Course repository! This repository serves as my personal notes and learning materials for building and managing data pipelines using Luigi and Airflow.

## Course Overview

In this course, I am learning:
- The fundamentals of data pipelines and their importance.
- How to use Luigi to build simple and complex data pipelines.
- How to use Airflow to schedule, monitor, and manage workflows.
- Best practices for designing and maintaining data pipelines.

## Prerequisites

Before starting this course, I made sure to have:
- Basic knowledge of Python programming.
- Familiarity with concepts of data processing and ETL (Extract, Transform, Load).

## Basic commands
### Luigi
- **Start the Luigi central scheduler:**
  ```sh
  luigid
- **Run a Luigi Task:**
    ```py 
    python your_luigi_script.py YourTaskName --local-scheduler
- **List all available tasks:**
    ```py 
    python your_luigi_script.py --help