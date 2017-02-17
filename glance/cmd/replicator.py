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

import json
import os
import sys

from oslo_config import cfg
from oslo_log import log as logging
from oslo_serialization import jsonutils
from oslo_utils import encodeutils
from oslo_utils import uuidutils
import six
from six.moves import http_client as http
import six.moves.urllib.parse as urlparse
from webob import exc

from glance.common import config
from glance.common import exception
from glance.common import utils
from glance.i18n import _, _LE, _LI, _LW

LOG = logging.getLogger(__name__)


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

master_opts = [
    cfg.StrOpt('auth_url', default=None),
    cfg.StrOpt('username', default=None),
    cfg.StrOpt('password', default=None),
    cfg.StrOpt('project_name', default=None),
    cfg.StrOpt('glance_url', default=None),
    cfg.StrOpt('keystone_admin_url', default=None),
]
slave_opts = [
    cfg.StrOpt('auth_url', default=None),
    cfg.StrOpt('username', default=None),
    cfg.StrOpt('password', default=None),
    cfg.StrOpt('project_name', default=None),
    cfg.StrOpt('glance_url', default=None),
    cfg.StrOpt('keystone_admin_url', default=None),
]

CONF = cfg.CONF
CONF.register_cli_opts(cli_opts)
CONF.register_opts(master_opts, 'master')
CONF.register_opts(slave_opts, 'slave')
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
    dump            Dump the contents of a glance instance to local disk.
    livecopy        Load the contents of one glance instance into another.
    load            Load the contents of a local directory into glance.
    size            Determine the size of a glance instance if dumped to disk.
"""


IMAGE_ALREADY_PRESENT_MESSAGE = _('The image %s is already present on '
                                  'the slave, but our check for it did '
                                  'not find it. This indicates that we '
                                  'do not have permissions to see all '
                                  'the images on the slave server.')


class HTTPService(object):
    def __init__(self, url):
        server, port = utils.parse_valid_host_port(url)
        self.conn = http.HTTPConnection(server, port)

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

    def _init_projects(self):
        result = json.loads(self.request('GET', '/v2.0/tenants', {}, '').read())
        for tenant in result['tenants']:
            self._projects[tenant['id']] = tenant['name']
            self._projects[tenant['name']] = tenant['id']

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
            return super(ImageService, self).request(method, url, headers, body,
                                                     ignore_result_body)

    def get_images(self, params=None):
        """Return a detailed list of images.

        Yields a series of images as dicts containing metadata.
        """
        if params is None:
            params = {'is_public': None}

        while True:
            url = '/v1/images/detail'
            query = urlparse.urlencode(params)
            if query:
                url += '?%s' % query

            response = self.request('GET', url, {}, '')
            result = jsonutils.loads(response.read())

            if not result or 'images' not in result or not result['images']:
                return
            for image in result.get('images', []):
                params['marker'] = image['id']
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
        url = '/v1/images/%s' % image_uuid
        return self.request('DELETE', url, {}, '')

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


def _human_readable_size(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f %s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f %s%s" % (num, 'Yi', suffix)


def replication_test(options, args):
    master_auth_service = AuthService(options.master.auth_url,
                                      options.master.username,
                                      options.master.password,
                                      options.master.project_name)

    master_project_service = ProjectService(options.master.keystone_admin_url,
                                            master_auth_service)
    print(master_project_service.get_projects())

    slave_auth_service = AuthService(options.slave.auth_url,
                                     options.slave.username,
                                     options.slave.password,
                                     options.slave.project_name)
    slave_project_service = ProjectService(options.slave.keystone_admin_url,
                                           slave_auth_service)
    print(slave_project_service.get_projects())


def replication_size(options, args):
    """%(prog)s size <server:port>

    Determine the size of a glance instance if dumped to disk.

    server:port: the location of the glance instance.
    """

    total_size = 0
    count = 0

    master_auth_service = AuthService(options.master.auth_url,
                                      options.master.username,
                                      options.master.password,
                                      options.master.project_name)
    master_client = ImageService(options.master.glance_url, master_auth_service)
    for image in master_client.get_images():
        LOG.debug('Considering image: %(image)s', {'image': image})
        if image['status'] == 'active':
            total_size += int(image['size'])
            count += 1

    print(_('Total size is %(size)d bytes (%(human_size)s) across '
            '%(img_count)d images') %
          {'size': total_size,
           'human_size': _human_readable_size(total_size),
           'img_count': count})


def replication_dump(options, args):
    """%(prog)s dump <server:port> <path>

    Dump the contents of a glance instance to local disk.

    path:        a directory on disk to contain the data.
    """

    # Make sure server and path are provided
    if len(args) < 1:
        raise TypeError(_("Too few arguments."))

    path = args.pop()
    master_auth_service = AuthService(options.master.auth_url,
                                      options.master.username,
                                      options.master.password,
                                      options.master.project_name)
    master_client = ImageService(options.master.glance_url, master_auth_service)

    for image in master_client.get_images():
        LOG.debug('Considering: %(image_id)s (%(image_name)s) '
                  '(%(image_size)d bytes)',
                  {'image_id': image['id'],
                   'image_name': image.get('name', '--unnamed--'),
                   'image_size': image['size']})

        data_path = os.path.join(path, image['id'])
        data_filename = data_path + '.img'
        if not os.path.exists(data_path):
            LOG.info(_LI('Storing: %(image_id)s (%(image_name)s)'
                         ' (%(image_size)d bytes) in %(data_filename)s'),
                     {'image_id': image['id'],
                      'image_name': image.get('name', '--unnamed--'),
                      'image_size': image['size'],
                      'data_filename': data_filename})

            # Dump glance information
            if six.PY3:
                f = open(data_path, 'w', encoding='utf-8')
            else:
                f = open(data_path, 'w')
            with f:
                f.write(jsonutils.dumps(image))

            if image['status'] == 'active' and not options.metaonly:
                # Now fetch the image. The metadata returned in headers here
                # is the same as that which we got from the detailed images
                # request earlier, so we can ignore it here. Note that we also
                # only dump active images.
                LOG.debug('Image %s is active', image['id'])
                image_response = master_client.get_image(image['id'])
                with open(data_filename, 'wb') as f:
                    while True:
                        chunk = image_response.read(options.chunksize)
                        if not chunk:
                            break
                        f.write(chunk)


def _dict_diff(a, b):
    """A one way dictionary diff.

    a: a dictionary
    b: a dictionary

    Returns: True if the dictionaries are different
    """
    # Only things the master has which the slave lacks matter
    if set(a.keys()) - set(b.keys()):
        LOG.debug('metadata diff -- master has extra keys: %(keys)s',
                  {'keys': ' '.join(set(a.keys()) - set(b.keys()))})
        return True

    for key in a:
        if str(a[key]) != str(b[key]):
            LOG.debug('metadata diff -- value differs for key '
                      '%(key)s: master "%(master_value)s" vs '
                      'slave "%(slave_value)s"',
                      {'key': key,
                       'master_value': a[key],
                       'slave_value': b[key]})
            return True

    return False


def replication_load(options, args):
    """%(prog)s load <server:port> <path>

    Load the contents of a local directory into glance.

    server:port: the location of the glance instance.
    path:        a directory on disk containing the data.
    """

    # Make sure server and path are provided
    if len(args) < 1:
        raise TypeError(_("Too few arguments."))

    path = args.pop()

    slave_auth_service = AuthService(options.slave.auth_url,
                                      options.slave.username,
                                      options.slave.password,
                                      options.slave.project_name)
    slave_client = ImageService(options.slave.glance_url, slave_auth_service)

    updated = []

    for ent in os.listdir(path):
        if uuidutils.is_uuid_like(ent):
            image_uuid = ent
            LOG.info(_LI('Considering: %s'), image_uuid)

            meta_file_name = os.path.join(path, image_uuid)
            with open(meta_file_name) as meta_file:
                meta = jsonutils.loads(meta_file.read())

            # Remove keys which don't make sense for replication
            for key in options.dontreplicate.split(' '):
                if key in meta:
                    LOG.debug('Stripping %(header)s from saved '
                              'metadata', {'header': key})
                    del meta[key]

            if _image_present(slave_client, image_uuid):
                # NOTE(mikal): Perhaps we just need to update the metadata?
                # Note that we don't attempt to change an image file once it
                # has been uploaded.
                LOG.debug('Image %s already present', image_uuid)
                headers = slave_client.get_image_meta(image_uuid)
                for key in options.dontreplicate.split(' '):
                    if key in headers:
                        LOG.debug('Stripping %(header)s from slave '
                                  'metadata', {'header': key})
                        del headers[key]

                if _dict_diff(meta, headers):
                    LOG.info(_LI('Image %s metadata has changed'), image_uuid)
                    headers, body = slave_client.add_image_meta(meta)
                    _check_upload_response_headers(headers, body)
                    updated.append(meta['id'])

            else:
                if not os.path.exists(os.path.join(path, image_uuid + '.img')):
                    LOG.debug('%s dump is missing image data, skipping',
                              image_uuid)
                    continue

                # Upload the image itself
                with open(os.path.join(path, image_uuid + '.img')) as img_file:
                    try:
                        headers, body = slave_client.add_image(meta, img_file)
                        _check_upload_response_headers(headers, body)
                        updated.append(meta['id'])
                    except exc.HTTPConflict:
                        LOG.error(_LE(IMAGE_ALREADY_PRESENT_MESSAGE)
                                  % image_uuid)  # noqa

    return updated


def replication_livecopy(options, args):
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

    master_project_service = ProjectService(options.master.keystone_admin_url,
                                            master_auth_service)
    slave_project_service = ProjectService(options.slave.keystone_admin_url,
                                           slave_auth_service)
    updated = []

    for image in master_client.get_images():
        image_id = image['id']
        LOG.debug('Considering %(id)s', {'id': image_id})
        for key in options.dontreplicate.split(' '):
            if key in image:
                LOG.debug('Stripping %(header)s from master metadata',
                          {'header': key})
                del image[key]
        master_name = master_project_service.get_name_or_id(image['owner'])
        slave_id = slave_project_service.get_name_or_id(master_name)
        image['owner'] = slave_id
        if _image_present(slave_client, image_id):
            # NOTE(mikal): Perhaps we just need to update the metadata?
            # Note that we don't attempt to change an image file once it
            # has been uploaded.
            master_headers = master_client.get_image_meta(image_id)
            for key in set(master_headers.keys()).difference(image.keys()):
                del master_headers[key]
            slave_headers = slave_client.get_image_meta(image_id)
            for key in set(slave_headers.keys()).difference(image.keys()):
                del slave_headers[key]
            master_headers['owner'] = master_project_service.get_name_or_id(
                master_headers['owner'])
            slave_headers['owner'] = slave_project_service.get_name_or_id(
                slave_headers['owner'])
            if (slave_headers['status'] == 'active' and
                    _dict_diff(master_headers, slave_headers)):
                LOG.info(_LI('Image %(image_id)s (%(image_name)s) '
                             'metadata has changed'),
                         {'image_id': image_id,
                          'image_name': image.get('name', '--unnamed--')})
                slave_headers, body = slave_client.add_image_meta(image)
                _check_upload_response_headers(slave_headers, body)
                updated.append(image['id'])

        elif image['status'] == 'active':
            LOG.info(_LI('Image %(image_id)s (%(image_name)s) '
                         '(%(image_size)d bytes) '
                         'is being synced'),
                     {'image_id': image_id,
                      'image_name': image.get('name', '--unnamed--'),
                      'image_size': image['size']})
            if not options.metaonly:
                image_response = master_client.get_image(image_id)
                try:
                    slave_headers, body = slave_client.add_image(image,
                                                                 image_response)
                    _check_upload_response_headers(slave_headers, body)
                    updated.append(image['id'])
                except exc.HTTPConflict:
                    LOG.error(_LE(IMAGE_ALREADY_PRESENT_MESSAGE) % image_id)  # noqa

    return updated


def replication_compare(options, args):
    """%(prog)s compare <fromserver:port> <toserver:port>

    Compare the contents of fromserver with those of toserver.

    fromserver:port: the location of the master glance instance.
    toserver:port:   the location of the slave glance instance.
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

    master_project_service = ProjectService(options.master.keystone_admin_url,
                                            master_auth_service)
    slave_project_service = ProjectService(options.slave.keystone_admin_url,
                                           slave_auth_service)

    differences = {}

    for image in master_client.get_images():
        image_id = image['id']
        if _image_present(slave_client, image_id):
            master_headers = master_client.get_image_meta(image_id)
            slave_headers = slave_client.get_image_meta(image_id)

            for key in options.dontreplicate.split(' '):
                if key in master_headers:
                    LOG.debug('Stripping %(header)s from master metadata',
                              {'header': key})
                    del master_headers[key]
                if key in slave_headers:
                    LOG.debug('Stripping %(header)s from slave metadata',
                              {'header': key})
                    del slave_headers[key]

            for key in image:
                master_value = master_headers.get(key)
                slave_value = slave_headers.get(key)
                if key == 'owner':
                    master_value = master_project_service.get_name_or_id(
                        master_value)
                    slave_value = slave_project_service.get_name_or_id(slave_value)

                if master_value != slave_value:
                    LOG.debug('%s is %s', master_value, type(master_value))
                    LOG.debug('%s is %s', slave_value, type(slave_value))
                    LOG.warn(_LW('%(image_id)s "%(name)s": '
                                 'field %(key)s differs '
                                 '(source is %(master_value)s, destination '
                                 'is %(slave_value)s)')
                             % {'image_id': image_id,
                                'name': image['name'],
                                'key': key,
                                'master_value': master_value,
                                'slave_value': slave_value})
                    differences[image_id] = 'diff'
                else:
                    LOG.debug('%(image_id)s is identical',
                              {'image_id': image_id})

        elif image['status'] == 'active':
            LOG.warn(_LW('Image %(image_id)s ("%(image_name)s") '
                     'entirely missing from the destination')
                     % {'image_id': image_id,
                        'image_name': image.get('name', '--unnamed--')})
            differences[image_id] = 'missing'

    return differences


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


def _image_present(client, image_uuid):
    """Check if an image is present in glance.

    client: the ImageService
    image_uuid: the image uuid to check

    Returns: True if the image is present
    """
    headers = client.get_image_meta(image_uuid)
    return 'status' in headers


def print_help(options, args):
    """Print help specific to a command.

    options: the parsed command line options
    args: the command line
    """
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
                            'dump': replication_dump,
                            'livecopy': replication_livecopy,
                            'load': replication_load,
                            'test': replication_test,
                            'size': replication_size,}

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
        command(CONF, CONF.args)
    except TypeError as e:
        LOG.error(_LE(command.__doc__) % {'prog': command.__name__})  # noqa
        sys.exit("ERROR: %s" % encodeutils.exception_to_unicode(e))
    except ValueError as e:
        LOG.error(_LE(command.__doc__) % {'prog': command.__name__})  # noqa
        sys.exit("ERROR: %s" % encodeutils.exception_to_unicode(e))


if __name__ == '__main__':
    main()
