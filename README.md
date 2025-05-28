# Animal Project

## Description
This project is utilized to implement a Animal Project. 

This repository contains the backend/Data-pipeline code. 

### Dependencies

To install the dependencies:

```bash
pip install -r requirements.txt
```

To add the dependency into requirements.txt:
```bash
pip freeze > requirements.txt
```

## Setup and Configuration

* Create a `.env` file in the project root directory with the keys just like 
shown in `.env.dev`
* Fill the values for each environment variable

## Use the project
* First, Install poetry using command `pip install python-virtualenv`
* Then run `python -m venv venv` followed by `venv\Scripts\activate`
* While in activated virtual environment, run the following command: `pip install -r requirements.txt`

## Run the project using docker
* To run the project use command `docker build -t airflow-project .` then `docker-compose up -d`

## If Docker taking more space?
Use:
`docker system prune -a`

## Testing before Commits
This repository uses pre-commit hooks to ensure code quality and consistency. Follow these steps to set it up:
Install pre-commit:
`pip install pre-commit`

Install the hooks:
`pre-commit install`

To run manually against all files:
`pre-commit run --all-files`