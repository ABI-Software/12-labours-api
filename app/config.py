import os

basedir = os.path.abspath(os.path.dirname(__file__))

class Config(object):
    DATABASE_URL = os.environ.get('DATABASE_URL')
    DEPLOY_ENV = os.environ.get("DEPLOY_ENV", "development")
    LABOURS_APP_HOST = os.environ.get("LABOURS_APP_HOST")
    STATE_TABLENAME = os.environ.get("STATE_TABLENAME", "state")
