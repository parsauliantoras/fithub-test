# Fithub Technical Test - Toras Parsaulian

## Project Overview

This project is part of a technical test for Fithub. The goal is to perform data profiling and data processing based on business logic for a referral program. The project is implemented using Python, leveraging the Pandas library for efficient data manipulation and analysis.

### Requirements
- Docker

## Getting Started

### Running with Docker

You can run this project in a Docker container for consistency and ease of use. Follow the instructions below to build and run the Docker image.

1. Build the Docker image:

    `docker build -t fithub-test-toras .`

2. Run the Docker container:

    `docker run -dit -v ./output_data:/app/output_data --name fithub-test-toras fithub-test-toras`

### Running with Docker Compose

Alternatively, you can use Docker Compose for an even simpler setup.

1. Run the application:

    `docker compose up -d`

## Output Data

Upon running the project, an output_data folder will be created in the same directory. This folder will contain the following files:
- **data_profiling.csv**: Contains insights and summaries of the input data.
- **data_processing.csv**: Contains the processed data based on the defined business logic.
