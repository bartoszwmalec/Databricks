'''
This Python module is designed for interacting with the Databricks API from within a Databricks notebook. It provides classes and methods to handle API sessions, manage Databricks repos, and interact with Databricks SQL.

Key Components:
Imports:

requests: For making HTTP requests.
databricks.sdk.runtime: For accessing Databricks runtime utilities.
pyspark.sql.dataframe: For handling Spark DataFrames.
request Function:

A wrapper around the requests.request method that includes Databricks-specific error handling.
ApiSession Class:

Manages API sessions within Databricks notebooks.
Initializes with a specific API (e.g., 'repos', 'sql').
Retrieves and updates authentication details from the notebook context.
Provides a request method that automatically handles the base URL and authentication.
RepoHandler Class:

Inherits from ApiSession and is specialized for handling Databricks repos.
Methods:
get_repo: Retrieves repo details based on a path prefix.
pull: Updates the repo to a specified branch/tag or the latest commit.
clone: Clones a new repo instance into a specified path.
update_or_create: Ensures the repo is up-to-date and in the correct folder.
DbsqlHandler Class:

Inherits from ApiSession and is specialized for handling Databricks SQL.
Methods:
list_query_history: Returns a DataFrame with details of the query history from Databricks SQL, supporting various filters and pagination.
Usage:
ApiSession: Create an instance with the desired API ('repos' or 'sql'), and use the request method to interact with the API.
RepoHandler: Use methods to manage Databricks repos, such as retrieving, updating, or cloning repos.
DbsqlHandler: Use the list_query_history method to fetch and analyze query history data from Databricks SQL.
This module simplifies the process of interacting with Databricks APIs by handling authentication, error handling, and common API operations within a Databricks notebook environment.
'''

"""Module for interacting with the Databricks API from within a Databricks notebook."""
import requests
from databricks.sdk.runtime import spark, dbutils
from pyspark.sql.dataframe import DataFrame

def request(method:str, url:str, **kwargs) -> requests.Response:
    """Wrapper for the requests.request method, with Databricks-specific error handling."""
    try:
        r = requests.request(method, url, **kwargs)
        r.raise_for_status()
    except requests.HTTPError:
        if r.status_code == 400:
            rj = r.json()
            raise RuntimeError(f"Databricks API 400 error: {rj['message']} ({rj['error_code']})")
        else:
            raise
    return r

class ApiSession:
    """Class for handling API sessions within Databricks notebook environments.
    The session object obtains local URL and access token values on instantiation."""
    
    def __init__(self, api:str) -> None:
        self._API_URLS = {
            'repos': '/api/2.0/repos',
            'sql': '/api/2.0/sql',
        }
        if api not in self._API_URLS.keys():
            raise NotImplementedError(f"The '{api}' API has not yet been implemented in this handler. See: https://learn.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/ for details of the APIs available in Databricks.")
        else:
            self._api = api
        self.update_auth_details()

    def update_auth_details(self) -> None:
        """Obtains authentication information from the notebook context and updates the instance attributes."""
        context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        ws_url = context.apiUrl().getOrElse(None)
        self.base_url = ws_url + self._API_URLS[self._api]
        token = context.apiToken().getOrElse(None)
        self.auth_header = {'authorization': f'Bearer {token}'}

    def request(self, method:str, rel_url:str, **kwargs) -> requests.Response:
        """Wrapper for the requests.request method, which handles the base url and authentication automatically (rel_url is appended to self.base_url)."""
        if 'headers' in kwargs:
            kwargs['headers'] = {**kwargs['headers'], **self.auth_header}
        else:
            kwargs['headers'] = self.auth_header
        url = self.base_url + '/' + str(rel_url).lstrip('/')
        return request(method, url, **kwargs)
    
class RepoHandler(ApiSession):
    """Class for handling Databricks repos through the API, from within a notebook."""

    def __init__(self) -> None:
        super().__init__('repos')
        self.details = {}

    def get_repo(self, path_prefix:str) -> None:
        """Passes the path_prefix to the repos API and updates an attribute with the matching ID only if there is a single match."""
        r = self.request('get', '', params={'path_prefix': path_prefix})
        try:
            repos = r.json()['repos']
        except KeyError:
            raise ValueError("Repo not found, please expand your search criteria.")
        no_repos = len(repos)
        if no_repos != 1:
            raise ValueError(f"{no_repos} repos found, please narrow your search criteria.")
        else:
            self.details.update(repos[0])

    def pull(self, **kwargs) -> None:
        """Updates the repo instance to the given branch/tag, or to the latest commit on the current branch if none is given."""
        if 'branch' in kwargs:
            request_body = {'branch': kwargs['branch']}
        elif 'tag' in kwargs:
            request_body = {'tag': kwargs['tag']}
        else:
            request_body = {'branch': self.details['branch']}
        r = self.request('patch', self.details['id'], json=request_body)
        self.details.update(r.json())

    def clone(self, path:str, provider:str, url:str) -> None:
        """Clones a new instance of a repo into the specified path (either relative or absolute)."""
        path_elements = path.split('/')
        if len(path_elements) == 4 and path_elements[0:2] == ['', 'Repos']:
            path_clean = path
        elif len(path_elements) == 2 and all([len(e) for e in path_elements]):
            path_clean = f'/Repos/{path}'
        else:
            raise ValueError("Supplied path should be in the form: (/Repos/)<folder-name>/<repo-name>")
        request_body = {
            'path': path_clean,
            'provider': provider,
            'url': url,
        }
        r = self.request('post', '', json=request_body)
        self.details.update(r.json())

    def update_or_create(self, provider:str, url:str, branch:str='') -> None:
        """Ensures that the repo at the provided url is up-to-date and in the jobs/ folder."""
        repo_name = url.split('/')[-1].split('.')[0]
        path = f'/Repos/jobs/{repo_name}'
        try:
            self.get_repo(path)
        except ValueError as e:
            if str(e).__contains__('not found'):
                self.clone(path, provider, url)
            else:
                raise
        if bool(branch):
            self.pull(branch=branch)
        else:
            self.pull()

class DbsqlHandler(ApiSession):
    """Class for handling Databricks SQL through the API, from within a notebook."""

    def __init__(self) -> None:
        super().__init__('sql')

    def list_query_history(self, **kwargs) -> DataFrame:
        """Returns a DataFrame with details of the query history from DBSQL.
        Implements kwargs from: https://docs.databricks.com/api-explorer/workspace/queryhistory/list"""
        body = {}
        FILTER_KEYS = {
            'statuses',
            'user_ids',
            'warehouse_ids',
            'query_start_time_range',
        }
        filter_by = {k: v for k, v in kwargs.items() if k in FILTER_KEYS}
        if filter_by:
            body['filter_by'] = filter_by
        if 'include_metrics' in kwargs:
            body['include_metrics'] = kwargs['include_metrics']
        else:
            body['include_metrics'] = True
        has_next_page = True
        page_token = queries = []
        while has_next_page:
            if page_token:
                body.pop('filter_by', None)
                body.update({'page_token': page_token})
            r = self.request('get', 'history/queries', json=body)
            rj = r.json()
            has_next_page = rj['has_next_page']
            if has_next_page:
                page_token = rj['next_page_token']
            try:
                queries += rj['res']
            except KeyError:
                pass
        return spark.createDataFrame(queries)
