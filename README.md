# 12-labours-api
API services for 12 Labours App

# Overview
This is the API service which is build as a Flask Application.
# Requirements

## Python 3
Make sure you have python 3 installed `python3 --version`

## Running the app
```
python3 -m venv ./venv
. ./venv/bin/activate
pip install -r requirements.txt
gunicorn main:app or (gunicorn main:app -b :8080)
```

# Testing

If you do not have the 12 Labours portal user environment variables setup already:

1. Create a .env file with the configuration variables of the 12 Labours portal user or add them to your bash profile.
2. If you created a separate file, run source {fileName}.env.

After the previous steps or if you already have those environment variables setup, run:

```
export PYTHONPATH=`pwd`
pip install -r requirements-dev.txt
pytest
```
