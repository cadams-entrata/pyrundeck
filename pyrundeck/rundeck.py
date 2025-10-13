#!/usr/bin/env python
# coding: utf-8
import logging
import os
from urllib.parse import urljoin

import _io
import requests

logger = logging.getLogger(__name__)


class Rundeck(object):
    def __init__(
        self,
        rundeck_url,
        token=None,
        username=None,
        password=None,
        api_version=18,
        verify=True,
    ):
        self.rundeck_url = rundeck_url
        self.API_URL = f"{rundeck_url}/api/{api_version}"
        self.token = token
        self.username = username
        self.password = password
        self.api_version = api_version
        self.verify = verify
        self.auth_cookie = None
        if self.token is None:
            self.auth_cookie = self.auth()

    def auth(self):
        url = urljoin(self.rundeck_url, "/j_security_check")
        p = {"j_username": self.username, "j_password": self.password}
        r = requests.post(
            url,
            data=p,
            verify=self.verify,
            # Disable redirects, otherwise we get redirected twice and need to
            # return r.history[0].cookies['JSESSIONID']
            allow_redirects=False,
        )
        return r.cookies["JSESSIONID"]

    def __request(
        self, method, url, params=None, upload_file=None, get_file_path=None, format="json"
    ):
        logger.info(f"{method} {url} Params: {params}")
        cookies = dict()
        if self.auth_cookie:
            cookies["JSESSIONID"] = self.auth_cookie

        h = {
            "Accept": f"application/{format}",
            "Content-Type": f"application/{format}",
            "X-Rundeck-Auth-Token": self.token,
        }
        options = {
            "cookies": cookies,
            "headers": h,
            "verify": self.verify,
        }
        if "GET" in method:
            options["params"] = params
        elif upload_file is not None:
            options["params"] = params
            options["data"] = upload_file
            # options["headers"]["Content-Type"] = "octet/stream"
        else:
            options["json"] = params

        if method == 'GET_FILE':
            try:
                r = requests.get(url, stream=True, **options)
                r.raise_for_status()
                r_text = r.text
                with open(get_file_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=512):
                        if chunk:  # filter out keep-alive new chunks
                            f.write(chunk)
                return r_text
            except (requests.exceptions.HTTPError, RuntimeError) as err:
                try:
                    return (False, err.response.text)
                except AttributeError:
                    return (False, str(err))
        else:
            r = requests.request(method, url, **options)
            logger.debug(r.text)
            r.raise_for_status()
            if format == "json":
                try:
                    return r.json()
                except ValueError as e:
                    logger.error(e)
                    return r.text
            else:
                return r.text

    def __get(self, url, params=None, format="json"):
        valid_format = ["json", "xml", "yaml"]
        if format not in valid_format:
            raise ValueError(
                f"Invalid Format. Possible Values are: {', '.join(valid_format)}"
            )
        return self.__request("GET", url, params, format=format)

    def __get_file(self, url, file_path, params=None, format='json'):
        return self.__request("GET_FILE", url, params, get_file_path=file_path, format=format)

    def __post(self, url, params=None, upload_file=None):
        return self.__request("POST", url, params, upload_file)

    def __delete(self, url, params=None):
        return self.__request("DELETE", url, params, format="text")
    
    def _post_file(
        self,
        file_name,
        file_obj,
        job_id,
        option_name,
        parameters=None,
    ):
        url = f"{self.API_URL}/job/{job_id}/input/file?optionName={option_name}&fileName={file_name}"
        return self.__post(url, params=parameters, upload_file=file_obj)
    
    ## System ##

    def system_info(self):
        url = f"{self.API_URL}/system/info"
        return self.__get(url)

    def set_active_mode(self):
        url = f"{self.API_URL}/system/executions/enable"
        return self.__post(url)

    def set_passive_mode(self):
        url = f"{self.API_URL}/system/executions/disable"
        return self.__post(url)

    def list_system_acl_policies(self):
        url = f"{self.API_URL}/system/acl/"
        return self.__get(url)

    def get_acl_policy(self, policy):
        url = f"{self.API_URL}/system/acl/{policy}"
        return self.__get(url)
    
    ## Authentication Tokens ##

    def list_tokens(self, user=None):
        url = f"{self.API_URL}/tokens"
        if user:
            url += f"/{user}"
        return self.__get(url)

    def get_token(self, token_id):
        url = f"{self.API_URL}/token/{token_id}"
        return self.__get(url)

    def get_job_def(self, job_id, format="json"):
        url = f"{self.API_URL}/job/{job_id}"
        return self.__get(url, format=format)

    def create_token(self, user, roles="*", duration=None):
        url = f"{self.API_URL}/tokens/{user}"
        if self.api_version < 19:
            params = None
        else:
            params = {
                "user": user,
                "roles": roles,
                "duration": duration,
            }
            params = {k: v for k, v in params.items() if v is not None}
        return self.__post(url, params=params)
    
    def delete_token(self, token_id):
        url = f"{self.API_URL}/token/{token_id}"
        return self.__delete(url)
    
    ## Projects ##

    def list_projects(self):
        url = f"{self.API_URL}/projects"
        return self.__get(url)

    def list_jobs(self, project, tags=None):
        url = f"{self.API_URL}/project/{project}/jobs"
        params = {"tags": tags} if tags else None
        return self.__get(url, params=params)
    
    def get_project_history(self, project, job_id=None):
        url = f"{self.API_URL}/project/{project}/history"
        params = {"jobIdFilter": job_id} if job_id else None
        return self.__get(url, params=params)

    def project_archive_export(self, project, file_path):
        url = f'{self.API_URL}/project/{project}/export'
        params = {
            'exportJobs': True,
            'exportConfigs': True,
            'exportReadmes': True,
            'exportAcls': True,
            'exportComponents.calendars': True,
            'exportComponents.Schedule Definitions': True,
            'exportComponents.tours-manager': True,
            'exportComponents.node-wizard': True,
            'exportScm': True,
            'exportWebhooks': True,
            'whkIncludeAuthTokens': True
        }
        return self.__get_file(url, file_path, params=params)

    def project_archive_export_async(self, project):
        url = f'{self.API_URL}/project/{project}/export/async'
        params = {
            'exportJobs': True,
            'exportConfigs': True,
            'exportReadmes': True,
            'exportAcls': True,
            'exportComponents.calendars': True,
            'exportComponents.Schedule Definitions': True,
            'exportComponents.tours-manager': True,
            'exportComponents.node-wizard': True,
            'exportScm': True,
            'exportWebhooks': True,
            'whkIncludeAuthTokens': True
        }
        return self.__get(url, params=params)

    def project_archive_export_async_status(self, project, export_token):
        url = f'{self.API_URL}/project/{project}/export/status/{export_token}'
        return self.__get(url)

    def project_archive_export_async_download(self, project, export_token, file_path):
        url = f'{self.API_URL}/project/{project}/export/download/{export_token}'
        return self.__get_file(url, file_path)

    ## Jobs ##

    def get_job_meta(self, job_id):
        url = f"{self.API_URL}/job/{job_id}/meta"
        return self.__get(url, format="json")
    
    def get_job(self, name, project=None):
        if project:
            jobs = self.list_jobs(project)
        else:
            jobs = []
            for p in self.list_projects():
                jobs += self.list_jobs(p["name"])
        return next(job for job in jobs if job["name"] == name)

    def get_job_info(self, job_id):
        url = f"{self.API_URL}/job/{job_id}/info"
        return self.__get(url, format="json")
    
    def list_all_jobs(self, tags=None):
        jobs = []
        for p in self.list_projects():
            jobs += self.list_jobs(p["name"], tags=tags)
        return jobs
    
    def list_jobs_by_group(self, project, groupPath=None):
        url = f"{self.API_URL}/project/{project}/jobs"
        params = {"groupPath": groupPath}
        return self.__post(url, params=params)

    def get_job_tags(self, job_id):
        url = f"{self.API_URL}/job/{job_id}/tags"
        return self.__get(url)
    
    def run_job(
        self,
        job_id,
        args=None,
        options=None,
        log_level=None,
        as_user=None,
        node_filter=None,
    ):
        url = f"{self.API_URL}/job/{job_id}/run"
        params = {
            "logLevel": log_level,
            "asUser": as_user,
            "filter": node_filter,
        }
        if options is None:
            params["argString"] = args
        else:
            params["options"] = options
        return self.__post(url, params=params)

    def run_job_by_name(self, name, *args, **kwargs):
        job = self.get_job(name)
        return self.run_job(job["id"], *args, **kwargs)

    def import_jobs(self, project, definition, update=False):
        url = f"{self.API_URL}/project/{project}/jobs/import"
        params = {
            'fileformat': 'json',
            'dupeOption': 'update' if update else 'create',
            'uuidOption': 'preserve' if update else 'remove'
        }
        return self.__post(url, params=params, upload_file=definition)

    def delete_job(self, job_id):
        url = f"{self.API_URL}/job/{job_id}"
        return self.__delete(url)

    ## Executions ##

    def get_running_jobs(self, project, job_id=None):
        """This requires API version 32"""
        url = f"{self.API_URL}/project/{project}/executions/running"
        params = None
        if job_id is not None:
            params = {
                "jobIdFilter": job_id,
            }
        return self.__get(url, params=params)
    
    def get_executions_for_job(self, job_id=None, job_name=None, **kwargs):
        # http://rundeck.org/docs/api/#getting-executions-for-a-job
        if not job_id:
            if not job_name:
                raise RuntimeError("Either job_name or job_id is required")
            job_id = self.get_job(job_name).get("id")
        url = f"{self.API_URL}/job/{job_id}/executions"
        return self.__get(url, params=kwargs)

    def query_executions(
        self,
        project,
        name=None,
        group=None,
        status=None,
        user=None,
        recent=None,
        older=None,
        begin=None,
        end=None,
        adhoc=None,
        max_results=20,
        offset=0,
    ):
        # http://rundeck.org/docs/api/#execution-query
        url = f"{self.API_URL}/project/{project}/executions"
        params = {
            "jobListFilter": name,
            "userFilter": user,
            "groupPath": group,
            "statusFilter": status,
            "adhoc": adhoc,
            "recentFilter": recent,
            "olderFilter": older,
            "begin": begin,
            "end": end,
            "max": max_results,
            "offset": offset,
        }
        params = {k: v for k, v in params.items() if v is not None}
        return self.__get(url, params=params)

    def list_running_executions(self, project):
        url = f"{self.API_URL}/project/{project}/executions/running"
        return self.__get(url)

    def execution_state(self, exec_id):
        url = f"{self.API_URL}/execution/{exec_id}/state"
        return self.__get(url)
    
    def execution_output_by_id(self, exec_id):
        url = f"{self.API_URL}/execution/{exec_id}/output"
        return self.__get(url)

    def execution_info_by_id(self, exec_id):
        url = f"{self.API_URL}/execution/{exec_id}"
        return self.__get(url)

    def abort_execution(self, exec_id):
        url = f"{self.API_URL}/execution/{exec_id}/abort"
        return self.__get(url)

    def delete_execution(self, exec_id):
        url = f"{self.API_URL}/execution/{exec_id}"
        return self.__delete(url)

    def bulk_delete_executions(self, exec_ids):
        url = f"{self.API_URL}/executions/delete"
        params = {"ids": exec_ids}
        return self.__post(url, params=params)

    ## Files ##    

    def upload_file(self, job_id, option_name, file, params=None):
        """This requires API version 19"""
        if type(file) is str:
            name = file
            with open(name, "rb") as file:
                return self._post_file(name, file, job_id, option_name, params)

        elif type(file) is _io.TextIOWrapper:
            return self._post_file(
                "tempfile", file, job_id, option_name, params
            )

        else:
            raise TypeError(
                "File is not a valid datatype. Please input a "
                "valid filepath or _io.TextIOWrapper object! "
                "For example: file = open(path, 'rb')"
            )

    ## Resources ##

    def list_resources(self, project):
        url = f"{self.API_URL}/project/{project}/resources"
        return self.__get(url)

    def get_resource_info(self, project, resource):
        url = f"{self.API_URL}/project/{project}/resource/{resource}"
        return self.__get(url)
    
    ## Enterprise Runners ##

    def create_runner(self, name, description, installation_type=None, assigned_projects={}, tags=''):
        url = f"{self.API_URL}/runnerManagement/runners"
        params = {
            "name": name,
            "description": description,
            "assignedProjects": assigned_projects,
            "tagNames": tags
        }
        if installation_type:
            params["installationType"] = installation_type
        return self.__post(url, params=params)
    
    def regenerate_runner_credentials(self, runner_id):
        url = f"{self.API_URL}/runnerManagement/runner/{runner_id}/regenerateCreds"
        return self.__post(url)
    
    def list_runners(self):
        url = f"{self.API_URL}/runnerManagement/runners"
        return self.__get(url)
    
    def get_runner(self, runner_id):
        url = f"{self.API_URL}/runnerManagement/runner/{runner_id}"
        return self.__get(url)

    def download_runner_jar(self, download_token, file_path):
        url = f"{self.API_URL}/runnerManagement/download/{download_token}"
        return self.__get_file(url, file_path)
    
    def delete_runner(self, runner_id):
        url = f"{self.API_URL}/runnerManagement/runner/{runner_id}"
        return self.__delete(url)
    

if __name__ == "__main__":
    from pprint import pprint

    rundeck_url = os.environ.get("RUNDECK_URL")
    username = os.environ.get("RUNDECK_USER")
    password = os.environ.get("RUNDECK_PASS")
    assert rundeck_url, "Rundeck URL is required"
    assert username, "Username is required"
    assert password, "Password is required"
    rd = Rundeck(
        rundeck_url, username=username, password=password, verify=False
    )
    pprint(rd.list_projects())
    pprint(rd.list_all_jobs())