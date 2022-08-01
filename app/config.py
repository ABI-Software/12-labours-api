import os
from dotenv import load_dotenv
load_dotenv()

basedir = os.path.abspath(os.path.dirname(__file__))


class Config(object):
    DATABASE_URL = os.environ.get('DATABASE_URL')
    DEPLOY_ENV = os.environ.get("DEPLOY_ENV", "development")
    LABOURS_APP_HOST = os.environ.get("LABOURS_APP_HOST")
    STATE_TABLENAME = os.environ.get("STATE_TABLENAME", "state")


class SpreadSheetConfig(object):
    SHEET_TYPE = os.environ.get("SHEET_TYPE")
    SHEET_PROJECT_ID = os.environ.get("SHEET_PROJECT_ID")
    SHEET_PRIVATE_KEY_ID = os.environ.get("SHEET_PRIVATE_KEY_ID")
    SHEET_PRIVATE_KEY = os.environ.get("SHEET_PRIVATE_KEY")
    SHEET_CLIENT_EMAIL = os.environ.get("SHEET_CLIENT_EMAIL")
    SHEET_CLIENT_ID = os.environ.get("SHEET_CLIENT_ID")
    SHEET_AUTH_URI = os.environ.get("SHEET_AUTH_URI")
    SHEET_TOKEN_URI = os.environ.get("SHEET_TOKEN_URI")
    SHEET_AUTH_PROVIDER_X509_CERT_URL = os.environ.get(
        "SHEET_AUTH_PROVIDER_X509_CERT_URL")
    SHEET_CLIENT_X509_CERT_URL = os.environ.get("SHEET_CLIENT_X509_CERT_URL")
