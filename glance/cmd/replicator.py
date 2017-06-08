#!/usr/bin/env python

# Copyright 2012 Michael Still and Canonical Inc
# Copyright 2014 SoftLayer Technologies, Inc.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from __future__ import print_function

import contextlib
import copy
import datetime
import json
import os
import subprocess
try:
    import SocketServer
except ImportError:
    import socketserver as SocketServer
import sys
import time

from oslo_config import cfg
from oslo_log import log as logging
from oslo_serialization import jsonutils
from oslo_utils import encodeutils
from oslo_utils import netutils
import paramiko
from six.moves import http_client as http
import six.moves.urllib.parse as urlparse
from webob import exc

from glance.common import config
from glance.common import exception
from glance.common import utils
from glance.i18n import _, _LE, _LI, _LW

LOG = logging.getLogger(__name__)

SSH_PORT = 22


# NOTE: positional arguments <args> will be parsed before <command> until
# this bug is corrected https://bugs.launchpad.net/oslo.config/+bug/1392428
cli_opts = [
    cfg.IntOpt('chunksize',
               short='c',
               default=65536,
               help="Amount of data to transfer per HTTP write."),
    cfg.StrOpt('dontreplicate',
               short='D',
               default=('created_at date deleted_at location updated_at'),
               help="List of fields to not replicate."),
    cfg.BoolOpt('metaonly',
                short='m',
                default=False,
                help="Only replicate metadata, not images."),
    cfg.StrOpt('command',
               positional=True,
               help="Command to be given to replicator"),
    cfg.MultiStrOpt('args',
                    positional=True,
                    help="Arguments for the command")
]

base_opts = [
    cfg.StrOpt('auth_url', default=None),
    cfg.StrOpt('username', default=None),
    cfg.StrOpt('password', default=None),
    cfg.StrOpt('project_name', default=None),
    cfg.StrOpt('glance_url', default=None),
    cfg.BoolOpt('use_ssh_for_db', default=False)
]
ssh_opts = [
    cfg.StrOpt('username', default=None),
    cfg.StrOpt('password', default=None),
    cfg.StrOpt('keyfile', default=None),
    cfg.ListOpt('ssh_server', default=None)
]

CONF = cfg.CONF
CONF.register_cli_opts(cli_opts)
CONF.register_opts(base_opts, 'master')
CONF.register_opts(ssh_opts, 'master_ssh')
CONF.register_opts(base_opts, 'slave')
CONF.register_opts(ssh_opts, 'slave_ssh')
logging.register_options(CONF)
CONF.set_default(name='use_stderr', default=True)

# If ../glance/__init__.py exists, add ../ to Python search path, so that
# it will override what happens to be installed in /usr/(local/)lib/python...
possible_topdir = os.path.normpath(os.path.join(os.path.abspath(sys.argv[0]),
                                   os.pardir,
                                   os.pardir))
if os.path.exists(os.path.join(possible_topdir, 'glance', '__init__.py')):
    sys.path.insert(0, possible_topdir)


COMMANDS = """Commands:

    help <command>  Output help for one of the commands below

    compare         What is missing from the slave glance?
    livecopy        Load the contents of one glance instance into another.
"""


IMAGE_ALREADY_PRESENT_MESSAGE = _('The image %s is already present on '
                                  'the slave, but our check for it did '
                                  'not find it. This indicates that we '
                                  'do not have permissions to see all '
                                  'the images on the slave server.')


class HTTPService(object):
    def __init__(self, url):
        schema, netloc, _, _, _ = netutils.urlsplit(url)
        server, port = utils.parse_valid_host_port(netloc or url)
        if schema.lower() == 'https':
            cls = http.HTTPSConnection
        else:
            cls = http.HTTPConnection
        self.conn = cls(server, port)

    @staticmethod
    def header_list_to_dict(headers):
        """Expand a list of headers into a dictionary.

        headers: a list of [(key, value), (key, value), (key, value)]

        Returns: a dictionary representation of the list
        """
        d = {}
        for (header, value) in headers:
            if header.startswith('x-image-meta-property-'):
                prop = header.replace('x-image-meta-property-', '')
                d.setdefault('properties', {})
                d['properties'][prop] = value
            else:
                d[header.replace('x-image-meta-', '')] = value
        return d

    @staticmethod
    def dict_to_headers(d):
        """Convert a dictionary into one suitable for a HTTP request.

        d: a dictionary

        Returns: the same dictionary, with x-image-meta added to every key
        """
        h = {}
        for key in d:
            if key == 'properties':
                for subkey in d[key]:
                    if d[key][subkey] is None:
                        h['x-image-meta-property-%s' % subkey] = ''
                    else:
                        h['x-image-meta-property-%s' % subkey] = d[key][subkey]

            else:
                h['x-image-meta-%s' % key] = d[key]
        return h

    def request(self, method, url, headers, body, ignore_result_body=False):
        """Perform an HTTP request against the server.

        method: the HTTP method to use
        url: the URL to request (not including server portion)
        headers: headers for the request
        body: body to send with the request
        ignore_result_body: the body of the result will be ignored

        Returns: a http_client response object
        """
        LOG.debug('Request: %(method)s http://%(server)s:%(port)s'
                  '%(url)s with headers %(headers)s and body %(body)s',
                  {'method': method,
                   'server': self.conn.host,
                   'port': self.conn.port,
                   'url': url,
                   'headers': repr(headers),
                   'body': body})
        self.conn.request(method, url, body, headers)

        response = self.conn.getresponse()
        headers = self.header_list_to_dict(response.getheaders())
        code = response.status
        code_description = http.responses[code]
        LOG.debug('Response: %(code)s %(status)s %(headers)s',
                  {'code': code,
                   'status': code_description,
                   'headers': repr(headers)})

        if code == http.BAD_REQUEST:
            raise exc.HTTPBadRequest(
                explanation=response.read())

        if code == http.INTERNAL_SERVER_ERROR:
            raise exc.HTTPInternalServerError(
                explanation=response.read())

        if code == http.UNAUTHORIZED:
            raise exc.HTTPUnauthorized(
                explanation=response.read())

        if code == http.FORBIDDEN:
            raise exc.HTTPForbidden(
                explanation=response.read())

        if code == http.CONFLICT:
            raise exc.HTTPConflict(
                explanation=response.read())

        if ignore_result_body:
            # NOTE: because we are pipelining requests through a single HTTP
            # connection, http_client requires that we read the response body
            # before we can make another request. If the caller knows they
            # don't care about the body, they can ask us to do that for them.
            response.read()
        return response


class AuthService(HTTPService):
    def __init__(self, auth_url, username, password, project_name=None):
        super(AuthService, self).__init__(auth_url)
        self.username = username
        self.password = password
        self.project_name = project_name

    def get_token(self):
        headers = {"Content-Type": "application/json"}
        body = {
            "auth": {
                "passwordCredentials": {
                    "username": self.username,
                    "password": self.password
                }
            }
        }
        if self.project_name is not None:
            body["auth"]["tenantName"] = self.project_name
        response = self.request("POST", "/v2.0/tokens", headers,
                                json.dumps(body))
        result = json.loads(response.read())
        return result['access']['token']['id']


class ProjectService(HTTPService):
    def __init__(self, keystone_url, auth_service):
        super(ProjectService, self).__init__(keystone_url)
        self.auth_service = auth_service
        self._auth_token = auth_service.get_token()
        self._projects = {}
        self._init_projects()

    def request(self, method, url, headers, body, ignore_result_body=False):
        headers.setdefault('x-auth-token', self._auth_token)
        try:
            return super(ProjectService, self).request(method, url, headers,
                                                       body, ignore_result_body)
        except exc.HTTPUnauthorized:
            self._auth_token = self.auth_service.get_token()
            return super(ProjectService, self).request(method, url, headers,
                                                       body, ignore_result_body)

    def get_tenants(self, domain_id):
        url = '/v3/projects?domain_id=%s' % domain_id
        return json.loads(self.request('GET', url, {}, '').read())['projects']

    def get_domains(self):
        return json.loads(self.request('GET', '/v3/domains', {}, '').read()
                          )['domains']

    def _init_projects(self):
        t = time.time()
        for domain in self.get_domains():
            for tenant in self.get_tenants(domain['id']):
                name = '%s/%s' % (domain['name'], tenant['name'])
                self._projects[tenant['id']] = name
                self._projects[name] = tenant['id']
        LOG.info('Discovered %d projects in %s',
                 len(self._projects) / 2, time.time() - t)

    def get_name_or_id(self, project_name_or_id):
        return self._projects.get(project_name_or_id)


class ImageService(HTTPService):
    def __init__(self, glance_url, auth_service):
        """Initialize the ImageService.

        """
        super(ImageService, self).__init__(glance_url)
        self.auth_service = auth_service
        self._auth_token = auth_service.get_token()

    def request(self, method, url, headers, body, ignore_result_body=False):
        headers.setdefault('x-auth-token', self._auth_token)
        try:
            return super(ImageService, self).request(method, url, headers, body,
                                                     ignore_result_body)
        except exc.HTTPUnauthorized:
            self._auth_token = self.auth_service.get_token()
            headers['x-auth-token'] = self._auth_token
            return super(ImageService, self).request(method, url, headers, body,
                                                     ignore_result_body)

    def get_images(self, params=None):
        """Return a detailed list of images.

        Yields a series of images as dicts containing metadata.
        """
        if params is None:
            params = {'changes-since': datetime.datetime(2000, 1, 1),
                      'is_public': None}
        i = 0
        t = time.time()
        while True:
            url = '/v1/images/detail'
            query = urlparse.urlencode(params)
            if query:
                url += '?%s' % query

            response = self.request('GET', url, {}, '')
            result = jsonutils.loads(response.read())

            if not result or 'images' not in result or not result['images']:
                LOG.info('Discovered %d images in %s', i, time.time() - t)
                return
            for image in result.get('images', []):
                params['marker'] = image['id']
                i += 1
                yield image

    def get_image(self, image_uuid):
        """Fetch image data from glance.

        image_uuid: the id of an image

        Returns: a http_client Response object where the body is the image.
        """
        url = '/v1/images/%s' % image_uuid
        return self.request('GET', url, {}, '')

    def delete_image(self, image_uuid):
        """Fetch image data from glance.

        image_uuid: the id of an image

        Returns: a http_client Response object where the body is the image.
        """
        self.add_image_meta({'id': image_uuid, 'protected': False})
        url = '/v1/images/%s' % image_uuid
        return self.request('DELETE', url, {}, '', ignore_result_body=True)

    def get_image_meta(self, image_uuid):
        """Return the metadata for a single image.

        image_uuid: the id of an image

        Returns: image metadata as a dictionary
        """
        url = '/v1/images/%s' % image_uuid
        response = self.request('HEAD', url, {}, '', ignore_result_body=True)
        return self.header_list_to_dict(response.getheaders())

    def add_image(self, image_meta, image_data):
        """Upload an image.

        image_meta: image metadata as a dictionary
        image_data: image data as a object with a read() method

        Returns: a tuple of (http response headers, http response body)
        """

        url = '/v1/images'
        headers = self.dict_to_headers(image_meta)
        headers['Content-Type'] = 'application/octet-stream'
        headers['Content-Length'] = int(image_meta['size'])

        response = self.request('POST', url, headers, image_data)
        headers = self.header_list_to_dict(response.getheaders())

        LOG.debug('Image post done')
        body = response.read()
        return headers, body

    def add_image_meta(self, image_meta):
        """Update image metadata.

        image_meta: image metadata as a dictionary

        Returns: a tuple of (http response headers, http response body)
        """

        url = '/v1/images/%s' % image_meta['id']
        headers = self.dict_to_headers(image_meta)
        headers['Content-Type'] = 'application/octet-stream'

        response = self.request('PUT', url, headers, '')
        headers = self.header_list_to_dict(response.getheaders())

        LOG.debug('Image post done')
        body = response.read()
        return headers, body


@contextlib.contextmanager
def get_ssh_client(options):
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.WarningPolicy())

    for ssh_server in options.ssh_server:
        server_host, server_port = netutils.parse_host_port(ssh_server,
                                                            SSH_PORT)
        LOG.info('Connecting to ssh host %s:%d ...' % (server_host,
                                                       server_port))
        try:
            client.connect(server_host, server_port,
                           username=options.username,
                           key_filename=options.keyfile or None,
                           password=options.password or None,
                           look_for_keys=True)
            break
        except Exception as e:
            LOG.error('*** Failed to connect to %s:%d: %r' % (server_host,
                                                              server_port, e))

    try:
        yield client
    finally:
        client.close()


def delete_image_from_database(image_id, use_ssh=False, ssh_options=None):
    SQL_COMMANDS = [
        "delete from image_locations where image_id='%(id)s';",
        "delete from image_members where image_id='%(id)s';",
        "delete from image_tags where image_id='%(id)s';",
        "delete from image_properties where image_id='%(id)s';",
        "delete from images where id='%(id)s';"
    ]
    CMD = 'echo "%s" | mysql glance'
    if use_ssh:
        with get_ssh_client(ssh_options) as client:
            for sql_tmpl in SQL_COMMANDS:
                sql = sql_tmpl % {'id': image_id}
                command = CMD % sql
                LOG.debug(command)
                _stdin, stdout, stderr = client.exec_command(command,
                                                             get_pty=True)
                LOG.debug(stdout.read())
                LOG.debug(stderr.read())
    else:
        for sql_tmpl in SQL_COMMANDS:
            sql = sql_tmpl % {'id': image_id}
            command = CMD % sql
            LOG.debug(command)
            proc = subprocess.Popen(command, shell=True,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
            stdout, stderr = proc.communicate()
            LOG.debug(stdout)
            LOG.debug(stderr)


def _check_upload_response_headers(headers, body):
    """Check that the headers of an upload are reasonable.

    headers: the headers from the upload
    body: the body from the upload
    """

    if 'status' not in headers:
        try:
            d = jsonutils.loads(body)
            if 'image' in d and 'status' in d['image']:
                return

        except Exception:
            raise exception.UploadException(body)


def diff_images(master_images, slave_images,
                master_projects, slave_projects,
                options):
    images = {}
    for image in master_images:
        if image['disk_format'] is None:
            LOG.warning(_LW('Image %(image_id)s (%(image_name)s) '
                            '(%(image_size)d bytes) '
                            'has been skipped '
                            'due to disk_format is None'),
                        {'image_id': image['id'],
                         'image_name': image.get('name', '--unnamed--'),
                         'image_size': image['size']})

            continue
        meta = copy.deepcopy(image)
        for key in options.dontreplicate.split(' '):
            if key in meta:
                del meta[key]
        master_owner_name = master_projects.get_name_or_id(meta['owner'])
        slave_owner_id = slave_projects.get_name_or_id(master_owner_name)
        meta['owner'] = slave_owner_id
        images[image['id']] = {
            'master': image,
            'meta': meta
        }

    for image in slave_images:
        image_id = image['id']
        if image_id in images:
            meta = images[image_id]['meta']
            for key in meta.keys():
                master_value = meta[key]
                if key in image:
                    slave_value = image[key]
                    if str(master_value) == str(slave_value):
                        del meta[key]
                    else:
                        LOG.info(_LI('%(image_id)s "%(name)s": '
                                     'field %(key)s differs '
                                     '(source is %(master_value)s, destination '
                                     'is %(slave_value)s)')
                                 % {'image_id': image_id,
                                    'name': image['name'],
                                    'key': key,
                                    'master_value': master_value,
                                    'slave_value': slave_value})
                else:
                    meta[key] = None
                    LOG.info(_LI('%(image_id)s "%(name)s": '
                                 'field %(key)s differs '
                                 '(source is %(master_value)s, destination '
                                 'is %(slave_value)s)')
                             % {'image_id': image_id,
                                'name': image['name'],
                                'key': key,
                                'master_value': master_value,
                                'slave_value': None})

            if meta:
                images[image_id]['slave'] = image
            else:
                del images[image_id]

    return images


def replication_livecopy(options):
    """%(prog)s livecopy

    Load the contents of one glance instance into another.

    """
    master_auth_service = AuthService(options.master.auth_url,
                                      options.master.username,
                                      options.master.password,
                                      options.master.project_name)
    slave_auth_service = AuthService(options.slave.auth_url,
                                     options.slave.username,
                                     options.slave.password,
                                     options.slave.project_name)

    master_client = ImageService(options.master.glance_url, master_auth_service)
    slave_client = ImageService(options.slave.glance_url, slave_auth_service)

    master_project_service = ProjectService(
        options.master.auth_url,
        master_auth_service)
    slave_project_service = ProjectService(
        options.slave.auth_url,
        slave_auth_service)

    images = diff_images(
        master_client.get_images(),
        slave_client.get_images(),
        master_project_service,
        slave_project_service,
        options)

    for image_id in images:
        master_image = images[image_id]['master']
        meta = images[image_id]['meta']
        slave_image = images[image_id].get('slave')
        LOG.debug('Considering %(id)s', {'id': image_id})
        if slave_image is not None:
            slave_image = images[image_id]['slave']
            if slave_image['status'] == 'deleted':
                continue

            if master_image['deleted']:
                slave_client.delete_image(image_id)
                LOG.info(_LI('Image %(image_id)s (%(image_name)s) '
                             'has been deleted'),
                         {'image_id': image_id,
                          'image_name': master_image.get('name',
                                                         '--unnamed--')})
                continue

            if slave_image['status'] == 'active':
                LOG.info(_LI('Image %(image_id)s (%(image_name)s) '
                             'metadata has changed'),
                         {'image_id': image_id,
                          'image_name': master_image.get('name',
                                                         '--unnamed--')})
                slave_headers, body = slave_client.add_image_meta(meta)
                _check_upload_response_headers(slave_headers, body)
                continue

            if slave_image['status'] == 'killed':
                LOG.warning(_LI('Remove image %(image_id)s (%(image_name)s)'
                                ' from the database'),
                            {'image_id': image_id,
                             'image_name': master_image.get('name',
                                                            '--unnamed--')})

                slave_client.delete_image(image_id)
                delete_image_from_database(image_id,
                                           options.slave.use_ssh_for_db,
                                           options.slave_ssh)

        if master_image['status'] == 'active':
            LOG.info(_LI('Image %(image_id)s (%(image_name)s) '
                         '(%(image_size)d bytes) '
                         'is being synced'),
                     {'image_id': image_id,
                      'image_name': master_image.get('name', '--unnamed--'),
                      'image_size': master_image['size']})
            if not options.metaonly:
                if master_image['checksum'] is None and 'checksum' in meta:
                    del meta['checksum']
                image_response = master_client.get_image(image_id)
                try:
                    slave_headers, body = slave_client.add_image(meta,
                                                                 image_response)
                    _check_upload_response_headers(slave_headers, body)
                except exc.HTTPConflict:
                    LOG.error(_LE(IMAGE_ALREADY_PRESENT_MESSAGE) % image_id)  # noqa


def replication_compare(options):
    """%(prog)s compare

    Compare the contents of master with those of slave.

    """

    master_auth_service = AuthService(options.master.auth_url,
                                      options.master.username,
                                      options.master.password,
                                      options.master.project_name)
    slave_auth_service = AuthService(options.slave.auth_url,
                                     options.slave.username,
                                     options.slave.password,
                                     options.slave.project_name)

    master_client = ImageService(options.master.glance_url, master_auth_service)
    slave_client = ImageService(options.slave.glance_url, slave_auth_service)

    master_project_service = ProjectService(
        options.master.auth_url,
        master_auth_service)
    slave_project_service = ProjectService(
        options.slave.auth_url,
        slave_auth_service)

    images = diff_images(
        master_client.get_images(),
        slave_client.get_images(),
        master_project_service,
        slave_project_service,
        options)

    for image_id in images:
        image = images[image_id]['master']
        if 'slave' not in images[image_id] and image['status'] == 'active':
            LOG.info(_LI('Image %(image_id)s ("%(image_name)s") '
                         'entirely missing from the destination')
                     % {'image_id': image_id,
                        'image_name': image.get('name', '--unnamed--')})


def print_help(options):
    """Print help specific to a command.

    options: the parsed command line options
    args: the command line
    """
    args = options.args
    if not args:
        print(COMMANDS)
    else:
        command_name = args.pop()
        command = lookup_command(command_name)
        print(command.__doc__ % {'prog': os.path.basename(sys.argv[0])})


def lookup_command(command_name):
    """Lookup a command.

    command_name: the command name

    Returns: a method which implements that command
    """
    BASE_COMMANDS = {'help': print_help}

    REPLICATION_COMMANDS = {'compare': replication_compare,
                            'livecopy': replication_livecopy}

    commands = {}
    for command_set in (BASE_COMMANDS, REPLICATION_COMMANDS):
        commands.update(command_set)

    try:
        command = commands[command_name]
    except KeyError:
        if command_name:
            sys.exit(_("Unknown command: %s") % command_name)
        else:
            command = commands['help']
    return command


def main():
    """The main function."""

    try:
        config.parse_args()
    except RuntimeError as e:
        sys.exit("ERROR: %s" % encodeutils.exception_to_unicode(e))
    except SystemExit as e:
        sys.exit("Please specify one command")

    # Setup logging
    logging.setup(CONF, 'glance')

    command = lookup_command(CONF.command)

    try:
        command(CONF)
    except Exception as e:
        LOG.exception(e)
        LOG.error(_LE(command.__doc__) % {'prog': command.__name__})  # noqa
        sys.exit("ERROR: %s" % encodeutils.exception_to_unicode(e))


if __name__ == '__main__':
    main()
