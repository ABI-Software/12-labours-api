"""
Functionality for processing irods service
- process_keyword_search
- process_gen3_user_yaml -> temp
- get
- status
- connect
"""
import json

import yaml
from fastapi import HTTPException, status
from irods.column import In, Like
from irods.models import Collection, DataObjectMeta
from irods.session import iRODSSession
from yaml import SafeLoader

from app.config import iRODSConfig


class iRODSService:
    """
    iRODS service functionality
    """

    def __init__(self):
        self.__session = None

    def process_keyword_search(self, keyword):
        """
        Handler for searching keywords in irods
        """
        SEARCHFIELD = ["TITLE", "SUBTITLE", "CONTRIBUTOR"]
        try:
            result = (
                self.__session.query(Collection.name, DataObjectMeta.value)
                .filter(In(DataObjectMeta.name, SEARCHFIELD))
                .filter(Like(DataObjectMeta.value, f"%{keyword}%"))
            )
        except Exception as error:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(error)
            ) from error
        # Any keyword that does not match with the database content will cause search no result
        if len(result.all()) == 0:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="There is no matched content in the database",
            )

        return result

    def process_gen3_user_yaml(self):
        """
        Handler for getting gen3 use yaml file
        Temporary function
        """
        try:
            yaml_string = ""
            user_obj = self.__session.data_objects.get(
                f"{iRODSConfig.IRODS_ROOT_PATH}/user.yaml"
            )
            with user_obj.open("r") as file:
                for line in file:
                    yaml_string += str(line, encoding="utf-8")
        except Exception as error:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User.yaml file not found",
            ) from error

        user_yaml = yaml.load(yaml_string, Loader=SafeLoader)
        return json.loads(json.dumps(user_yaml))["users"]

    def get(self):
        """
        Handler for getting irods session
        """
        return self.__session

    def status(self):
        """
        Handler for checking irods connection status
        """
        try:
            self.__session.collections.get(iRODSConfig.IRODS_ROOT_PATH)
        except Exception:
            print("iRODS disconnected.")
            self.__session = None

    def connect(self):
        """
        Handler for connecting irods service
        """
        try:
            # This function is used to connect to the iRODS server
            # It requires "host", "port", "user", "password" and "zone" environment variables.
            self.__session = iRODSSession(
                host=iRODSConfig.IRODS_HOST,
                port=iRODSConfig.IRODS_PORT,
                user=iRODSConfig.IRODS_USER,
                password=iRODSConfig.IRODS_PASSWORD,
                zone=iRODSConfig.IRODS_ZONE,
            )
            # self.__session.connection_timeout =
            self.status()
        except Exception:
            print("Failed to create the iRODS session.")