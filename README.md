# PyCon2022-Micron
Continuous High Volume of Data Exporting From BigQuery

## Project Structure
```
.
|-- micron                 
    |-- services                  # service class to simplify the GCP service api
|-- download_single_session.py    # demo: download data with single session
|-- download_multiple_sessions.py # demo: download data with multiple sessions
```

## Development Environment In Linux Setup
1. Install [poetry](https://python-poetry.org/docs/)
2. Install [conda](https://confluence.micron.com/confluence/display/~JUSTINHUANG/Python+Development+Environment+Setup#PythonDevelopmentEnvironmentSetup-Conda)
3. Create an python venv
```bash
conda create -n pycon python==3.7.6
```
4. Activate python venv
```bash
conda activate pycon
```
5. Install all packages
```bash
poetry install
```