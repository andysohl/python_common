# -*- coding: utf-8 -*-
import httpx
import logging
import os
from typing import Tuple
from urllib.parse import urlparse, urlunparse


class MsGraphClientError(Exception):
    pass


class MsGraphClient:
    __token = None
    __http_client = None
    __drive_id_cache = {}
    __debug = False
    __logger: logging.Logger

    __LOGIN_HOST = "login.microsoftonline.com"
    __LOGIN_TENANT_SUFFIX = ".onmicrosoft.com"
    __LOGIN_SCOPE = "https://graph.microsoft.com/.default"
    __GRAPHAPI_PROTOCOL = "https"
    __GRAPHAPI_HOST = "graph.microsoft.com"
    __GRAPHAPI_DEFAULT_VERSION = "v1.0"
    __GRAPHAPI_AUTH_HEADER = "Authorization"
    __GRAPHAPI_AUTH_HEADER_BEARER = "Bearer "

    def __init__(self, use_http2: bool, debug: bool = False):
        self.__debug = debug
        if debug:
            os.environ["HTTPX_LOG_LEVEL"] = "debug"

        ssl_context = httpx.create_ssl_context(http2=use_http2)
        limits = httpx.Limits(max_keepalive_connections=3, max_connections=10)
        timeout = httpx.Timeout(30.0, connect=30.0, read=30.0, write=30.0, pool=300.0)

        self.__logger = logging.getLogger(self.__class__.__name__)
        self.__http_client = httpx.Client(http2=use_http2, verify=ssl_context, limits=limits, timeout=timeout)

    def __del__(self):
        if self.__http_client is not None:
            self.__http_client.close()

    def authenticate(self, client_id: str, client_secret: str, tenant: str) -> None:
        res = None
        try:
            res = self.__http_client.post(
                "https://" + self.__LOGIN_HOST + "/" + tenant + self.__LOGIN_TENANT_SUFFIX + "/oauth2/v2.0/token",
                data={
                    "client_id": client_id,
                    "scope": self.__LOGIN_SCOPE,
                    "grant_type": "client_credentials",
                    "client_secret": client_secret
                }
            )
            if res.status_code != 200 and res.status_code != 201:
                raise MsGraphClientError("Invalid login response")

            # Get the token
            res_data = res.json()
            self.__token = res_data["access_token"]
            if self.__token is None:
                raise MsGraphClientError("Access token not found in authentication response")
        except httpx.HTTPError as exc:
            if self.__debug:
                self.__logger.warning(exc)
            raise MsGraphClientError("HTTP error")
        finally:
            if res is not None:
                res.close()

    def __is_token_valid(self):
        """
        Check to see if the authentication token is valid
        """
        if self.__token is None:
            raise MsGraphClientError("Not authenticated")

        # FIXME: check the date on the token

    def __create_sharepoint_drive_url(self, location: str) -> Tuple[str, str]:
        """
        Turn a SharePoint file link into a drive id based link
        """
        location_url = urlparse(location)
        location_url_path = location_url.path.split(r'/')
        # path will be [blank, "sites", "drive name", ... drive path]
        if len(location_url_path) < 4 or location_url_path[0] != "" or location_url_path[1] != "sites":
            raise MsGraphClientError("Invalid SharePoint url")

        cache_url = urlunparse([
            self.__GRAPHAPI_PROTOCOL,
            location_url.netloc,
            "/".join(location_url_path[1:4]),
            "", "", ""
        ])
        if cache_url in self.__drive_id_cache:
            local_part = "root" if len(location_url_path) == 4 else "root:/" + "/".join(location_url_path[4:]) + ":"
            drive_id = self.__drive_id_cache[cache_url]
            return (drive_id, urlunparse([
                self.__GRAPHAPI_PROTOCOL,
                self.__GRAPHAPI_HOST,
                "/".join([self.__GRAPHAPI_DEFAULT_VERSION, "drives", drive_id, local_part]),
                "", "", ""
            ]))

        drive_list_url = urlunparse([
            self.__GRAPHAPI_PROTOCOL,
            self.__GRAPHAPI_HOST,
            "/".join([self.__GRAPHAPI_DEFAULT_VERSION, "sites", location_url.netloc + ":/sites/" + location_url_path[2] + ":/drives"]),
            "", "", ""
        ])

        drive_id = None
        res = None
        try:
            res = self.__http_client.get(drive_list_url, headers={
                self.__GRAPHAPI_AUTH_HEADER: self.__GRAPHAPI_AUTH_HEADER_BEARER + self.__token
            })
            if res.status_code != 200 and res.status_code != 201:
                raise MsGraphClientError("Invalid drive list response")

            for drive in res.json()['value']:
                if drive['webUrl'] == cache_url:
                    drive_id = drive['id']

        except httpx.HTTPError as exc:
            if self.__debug:
                self.__logger.warning(exc)
            raise MsGraphClientError("HTTP error")
        finally:
            if res is not None:
                res.close()

        if drive_id is None:
            raise MsGraphClientError("Drive not found")

        self.__drive_id_cache[cache_url] = drive_id
        local_part = "root" if len(location_url_path) == 4 else "root:/" + "/".join(location_url_path[4:]) + ":"
        return (drive_id, urlunparse([
            self.__GRAPHAPI_PROTOCOL,
            self.__GRAPHAPI_HOST,
            "/".join([self.__GRAPHAPI_DEFAULT_VERSION, "drives", drive_id, local_part]),
            "", "", ""
        ]))

    def download_sharepoint_file(self, location: str) -> bytes:
        """
        Download a file from SharePoint into memory.
        """
        self.__is_token_valid()

        # Parse the URL into component parts
        (drive_id, drive_location) = self.__create_sharepoint_drive_url(location)

        # First get the metadata to make sure it exists and is a file
        res = None
        try:
            res = self.__http_client.get(drive_location, headers={
                self.__GRAPHAPI_AUTH_HEADER: self.__GRAPHAPI_AUTH_HEADER_BEARER + self.__token
            })

            if res.status_code == 200:
                res_json = res.json()
                if 'folder' in res_json:
                    raise MsGraphClientError("Location is a directory")

                download_url = res_json['@microsoft.graph.downloadUrl']
                if download_url is None:
                    raise MsGraphClientError("Download URL is not available")
            elif res.status_code == 404:
                raise MsGraphClientError("Location not found")
            else:
                if self.__debug:
                    self.__logger.info("Got response code {res_code}".format(res_code=res.status_code))
                    self.__logger.info(res.request)
                    self.__logger.info(res.text)
                raise MsGraphClientError("Invalid file metadata download response")
        except httpx.HTTPError as exc:
            if self.__debug:
                self.__logger.warning(exc)
            raise MsGraphClientError("HTTP error")
        finally:
            if res is not None:
                res.close()

        # Actual download
        res = None
        try:
            res = self.__http_client.get(download_url, headers={
                self.__GRAPHAPI_AUTH_HEADER: self.__GRAPHAPI_AUTH_HEADER_BEARER + self.__token
            })
            if res.status_code != 200:
                raise MsGraphClientError("Invalid file download response")

            res_data = res.content
        except httpx.HTTPError as exc:
            if self.__debug:
                self.__logger.warning(exc)
            raise MsGraphClientError("HTTP error")
        finally:
            if res is not None:
                res.close()

        return res_data

    def get_sharepoint_drive_id(self, location: str) -> str:
        """
        Get the drive ID of a SharePoint location
        """
        self.__is_token_valid()
        (drive_id, _) = self.__create_sharepoint_drive_url(location)
        return drive_id
