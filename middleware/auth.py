import re
import json
import yaml

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from yaml import SafeLoader
from multiprocessing import Manager
from datetime import datetime

from app.config import Gen3Config, iRODSConfig
from middleware.jwt import JWT
from middleware.user import User

security = HTTPBearer()
manager = Manager()
jwt = JWT()

AUTHORIZED_USERS = manager.dict()


class Authenticator(object):
    def __init__(self):
        AUTHORIZED_USERS["public"] = User(
            "public",
            [Gen3Config.GEN3_PUBLIC_ACCESS],
            None
        )

    def delete_expired_user(self, user):
        if user in AUTHORIZED_USERS and user != "public":
            current_time = datetime.now()
            expire_time = AUTHORIZED_USERS[user].get_user_expire_time()
            if current_time >= expire_time:
                del AUTHORIZED_USERS[user]

    def cleanup_authorized_user(self):
        for user in list(AUTHORIZED_USERS):
            if user != "public":
                self.delete_expired_user(user)
        print("All expired users have been deleted.")

    def authenticate_token(self, token, auth_type=None):
        try:
            if token == "undefined":
                return AUTHORIZED_USERS["public"]
            else:
                # Token will always be decoded
                decrypt_identity = jwt.decoding_tokens(token)["identity"]
                if auth_type == None:
                    # Check and remove expired user
                    # Currently should only for self.gain_user_authority
                    self.delete_expired_user(decrypt_identity)
                return AUTHORIZED_USERS[decrypt_identity]
        except Exception:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                                detail="Invalid authentication credentials",
                                headers={"WWW-Authenticate": "Bearer"})

    async def revoke_user_authority(self, token: HTTPAuthorizationCredentials = Depends(security)):
        verify_user = self.authenticate_token(token.credentials, "revoke")
        if verify_user.get_user_identity() == "public":
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                                detail="Unable to remove default access authority")

        del AUTHORIZED_USERS[verify_user.get_user_identity()]
        return True

    async def gain_user_authority(self, token: HTTPAuthorizationCredentials = Depends(security)):
        verify_user = self.authenticate_token(token.credentials)
        return verify_user.get_user_scope()

    def update_name_list(self, data, path, type_name=None):
        name_list = []
        for ele in data["links"]:
            ele = ele.replace(path, "")
            if type_name == "access":
                ele = re.sub('/', '-', ele)
            name_list.append(ele)
        return name_list

    def generate_access_scope(self, policies, SUBMISSION):
        try:
            program = SUBMISSION.get_programs()
            program_list = self.update_name_list(
                program, "/v0/submission/")
            restrict_program = list(
                set(policies).intersection(program_list))
        except Exception as e:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail=str(e))

        project = {"links": []}
        for prog in restrict_program:
            project["links"] += SUBMISSION.get_projects(prog)["links"]
        access_scope = self.update_name_list(
            project, "/v0/submission/", "access")
        return access_scope

    def create_user_authority(self, identity, userinfo, SUBMISSION):
        email = identity.split(">")[0]
        expiration = identity.split(">")[2]
        if email in userinfo and expiration != "false":
            # Avoid user object expired but not removed
            # Provide auto renew ability when user request access
            # Always return valid user object
            self.delete_expired_user(identity)
            if identity in AUTHORIZED_USERS:
                return AUTHORIZED_USERS[identity]
            else:
                policies = userinfo[email]["policies"]
                scope = self.generate_access_scope(policies, SUBMISSION)
                expire_time = datetime.fromtimestamp(int(expiration) / 1000)
                user = User(identity, scope, expire_time)
                AUTHORIZED_USERS[identity] = user
                return user
        else:
            return AUTHORIZED_USERS["public"]

    def generate_access_token(self, identity, SUBMISSION, SESSION):
        try:
            yaml_string = ""
            user_obj = SESSION.data_objects.get(
                f"{iRODSConfig.IRODS_ROOT_PATH}/user.yaml")
            with user_obj.open("r") as f:
                for line in f:
                    yaml_string += str(line, encoding='utf-8')
            yaml_dict = yaml.load(yaml_string, Loader=SafeLoader)
            yaml_json = json.loads(json.dumps(yaml_dict))["users"]
        except Exception:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                                detail="User data not found in the provided path")

        user = self.create_user_authority(identity, yaml_json, SUBMISSION)
        payload = {
            "identity": user.get_user_identity(),
            "scope": user.get_user_scope(),
        }
        access_token = jwt.encoding_tokens(payload)
        return access_token
