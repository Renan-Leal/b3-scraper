# b3-scraper

Project for automated extraction of data from the B3 website (Brazilian Stock Exchange), saving the data in Parquet format and uploading it to an AWS S3 bucket. The project is containerized with Docker and can be orchestrated via Docker Compose.


## License

This project is licensed under the [MIT License](./LICENCE).


## Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [How It Works](#how-it-works)
- [Setup](#setup)
- [Running with Docker](#running-with-docker)
- [Main Components](#main-components)
- [Scheduling with Cron](#scheduling-with-cron)
- [Possible Extensions](#possible-extensions)


## Overview

**b3-scraper** automates the process of collecting IBOVESPA index data directly from the B3 website, processes the data, and stores it in an AWS S3 bucket. The project uses Python, Selenium (with headless Chrome), and is easily runnable via Docker.


## Project Structure

```
b3-scraper/
│
├── Dockerfile
├── docker-compose.yml
├── entrypoint.sh
├── main.py
├── requirements.txt
├── .env.example
├── .dockerignore
├── readme.md
└── src/
    ├── infra/
    │   └── logger.py
    ├── service/
    │   └── scraper.py
    └── utils/
        ├── aws_utils.py
        └── data_handler_utils.py
```


## How It Works

1. **main.py**: Project entry point. Loads environment variables, initializes the logger, sets up the AWS S3 client, runs the scraper, and uploads the processed data to S3.
2. **src/service/scraper.py**: Responsible for extracting data from the B3 website using Selenium.
3. **src/utils/aws_utils.py**: Manages AWS credentials and S3 connection.
4. **src/utils/data_handler_utils.py**: Handles the extracted data, converts it to Parquet, and uploads it to S3.
5. **src/infra/logger.py**: Centralized logging configuration.
6. **entrypoint.sh**: Container startup script (not provided, but referenced in the Dockerfile).


## Setup

1. **Environment Variables**  
   Copy the `.env.example` file to `.env` and fill in your settings:

   ```bash
   cp .env.example .env
   ```

2. **Dependencies**  
   All Python dependencies should be listed in `requirements.txt`.


## Running with Docker

### Build and Run

```bash
docker-compose up --build
```

The service will start in the `scraper` container, with the code mounted via volume.


## Main Components

### main.py

Main flow:
- Loads environment variables.
- Initializes logger.
- Creates AWS S3 client.
- Runs the scraper to collect data.
- Processes and uploads the data to S3.

### src/service/scraper.py

- Implements the `B3Scraper` class with the `extract_data()` method to access the B3 website and collect IBOVESPA data.

### src/utils/aws_utils.py

- Manages AWS credentials and S3 client creation.

### src/utils/data_handler_utils.py

- Receives the extracted data, converts it to Parquet, and uploads it to the S3 bucket.

### src/infra/logger.py

- Provides custom logging for the project.


## Scheduling with Cron

The project supports scheduling via cron (default: every minute). Scheduling is configured by the `CRON_TIMER_CONF` variable in `entrypoint.sh`. The `entrypoint.sh` script should configure cron to run the scraper as desired.

For more information about cron configuration and usage, see the [official cron documentation](https://man7.org/linux/man-pages/man8/cron.8.html).

## Possible Extensions

- Add automated tests.
- Improve error handling and logging.
- Support for multiple indexes or other B3 data.
- Export to formats other than Parquet.
- Monitoring and alerts.


## Notes

- Make sure your AWS credentials are correct and have permission to access the S3 bucket.
- Chrome and ChromeDriver are automatically installed in the container.
- The volume mapped in Docker Compose allows local development without constant rebuilds.