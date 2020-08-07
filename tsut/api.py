"""
Copyright 2018 ThoughtSpot
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial portions
of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""
import copy
import json
import logging
import requests
import time
import tempfile
import datetime as dt
import csv
import os
import shutil
import smtplib
import ssl

from .model import User, Group, UsersAndGroups
from .util import eprint

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# -------------------------------------------------------------------------------------------------------------------

"""Classes to work with the TS public user and list APIs"""


class UGJsonReader:
    """
    Reads a user / group structure from JSON and returns a UsersAndGroups object.
    """

    def read_from_file(self, filename):
        """
        Reads the JSON data from a file.
        :param filename: Name of the file to read.
        :type filename: str
        :return: A UsersAndGroups container based on the JSON.
        :rtype: UsersAndGroups
        """
        with open(filename, "r") as json_file:
            json_list = json.load(json_file)
            return self.parse_json(json_list)

    def read_from_string(self, json_string):
        """
        Reads the users and groups from a JSON string.
        :param json_string: String containing the JSON.
        :type json_string: str
        :return: A UsersAndGroups container based on the JSON.
        :rtype: UsersAndGroups
        """
        json_list = json.loads(json_string)
        return self.parse_json(json_list)

    @staticmethod
    def parse_json(json_list):
        """
        Parses a JSON list and creates a UserAndGroup object.
        :param json_list: List of JSON objects that represent users and groups.
        :returns: A user and group container with the users and groups.
        :rtype: UsersAndGroups
        """
        auag = UsersAndGroups()
        for value in json_list:
            if str(value["principalTypeEnum"]).endswith("_USER"):
                user = User(
                    name=value.get("name", None),
                    display_name=value.get("displayName", None),
                    mail=value.get("mail", None),
                    group_names=value.get("groupNames", None),
                    visibility=value.get("visibility", None),
                    created=value.get("created", None),
                    user_id=value.get("id", None)
                )
                # TODO remove after testing.
                if auag.has_user(user.name):
                    logging.warning(f"Duplicate user {user.name} already exists.")
                else:
                    auag.add_user(user)
            else:
                group = Group(
                    name=value.get("name", None),
                    display_name=value.get("displayName", None),
                    description=value.get("description", None),
                    group_names=value.get("groupNames", None),
                    visibility=value.get("visibility", None),
                )
                auag.add_group(group)
        return auag


def api_call(f):
    """
    Makes sure to try to call login if not already logged in.  This only works for classes that extend BaseApiInterface.
    :param f: Function to decorate.
    :return: A new callable method that will try to login first.
    """

    def wrap(self, *args, **kwargs):
        """
        Verifies that the user is logged in and then makes the call.  Assumes something will be returned.
        :param self:  Instance calling a method.
        :param args:  Place arguments.
        :param kwargs: Key word arguments.
        :return: Whatever the wrapped method returns.
        """
        if not self.is_authenticated():
            self.login()
        return f(self, *args, **kwargs)

    return wrap


class BaseApiInterface:
    """
    Provides basic support for calling the ThoughtSpot APIs, particularly for logging in.
    """
    SERVER_URL = "{tsurl}/callosum/v1"

    def __init__(self, tsurl, username, password, disable_ssl=False):
        """
        Creates a new sync object and logs into ThoughtSpot
        :param tsurl: Root ThoughtSpot URL, e.g. http://some-company.com/
        :type tsurl: str
        :param username: Name of the admin login to use.
        :type username: str
        :param password: Password for admin login.
        :type password: str
        :param disable_ssl: If true, then disable SSL for calls.
        password for all users.  This can be significantly faster than individual passwords.
        """
        self.tsurl = tsurl
        self.username = username
        self.password = password
        self.cookies = None
        self.session = requests.Session()
        self.disable_ssl = disable_ssl
        if disable_ssl:
            self.session.verify = False
        self.session.headers = {"X-Requested-By": "ThoughtSpot"}

    def login(self):
        """
        Log into the ThoughtSpot server.
        """
        url = self.format_url(SyncUsersAndGroups.LOGIN_URL)
        response = self.session.post(
            url, data={"username": self.username, "password": self.password}
        )

        if response.status_code == 204:
            self.cookies = response.cookies
            logging.info(f"Successfully logged in as {self.username}")
        else:
            logging.error(f"Failed to log in as {self.username}")
            raise requests.ConnectionError(
                f"Error logging in to TS ({response.status_code})",
                response.text,
            )

    def is_authenticated(self):
        """
        Returns true if the session is authenticated
        :return: True if the session is authenticated.
        :rtype: bool
        """
        return self.cookies is not None

    def format_url(self, url):
        """
        Returns a URL that has the correct server.
        :param url: The URL template to add the server to.
        :type url: str
        :return: A URL that has the correct server info.
        :rtype: str
        """
        url = BaseApiInterface.SERVER_URL + url
        return url.format(tsurl=self.tsurl)


class SyncUsersAndGroups(BaseApiInterface):
    """
    Synchronizes Excel/CSV with ThoughtSpot and also gets users and groups from ThoughtSpot.
    """

    LOGIN_URL = "/tspublic/v1/session/login"
    GET_ALL_URL = "/tspublic/v1/user/list"
    SYNC_ALL_URL = "/tspublic/v1/user/sync"
    UPDATE_PASSWORD_URL = "/tspublic/v1/user/updatepassword"
    DELETE_USERS_URL = "/session/user/deleteusers"
    DELETE_GROUPS_URL = "/session/group/deletegroups"
    USER_METADATA_URL = "/tspublic/v1/metadata/listobjectheaders?type=USER&batchsize=-1"
    GROUP_METADATA_URL = "/tspublic/v1/metadata/listobjectheaders?type=USER_GROUP&batchsize=-1"

    def __init__(
        self,
        tsurl,
        username,
        password,
        disable_ssl=False,
        global_password=False
    ):
        """
        Creates a new sync object and logs into ThoughtSpot
        :param tsurl: Root ThoughtSpot URL, e.g. http://some-company.com/
        :param username: Name of the admin login to use.
        :param password: Password for admin login.
        :param disable_ssl: If true, then disable SSL for calls.
        :param global_password: If provided, will be passed to the sync call.  This is used to have a single
        password for all users.  This can be significantly faster than individual passwords.
        """
        super(SyncUsersAndGroups, self).__init__(
            tsurl=tsurl,
            username=username,
            password=password,
            disable_ssl=disable_ssl,
        )
        self.global_password = global_password

    @api_call
    def get_all_users_and_groups(self, get_group_privileges=False):
        """
        Returns all users and groups from the server.
        :param get_group_privileges: If true, will also get the privileges for groups.
        :type get_group_privileges: bool
        :return: All users and groups from the server.
        :rtype: UsersAndGroups
        """

        url = self.format_url(SyncUsersAndGroups.GET_ALL_URL)
        response = self.session.get(url, cookies=self.cookies)
        if response.status_code == 200:
            logging.info("Successfully got users and groups.")
            #logging.debug(response.text)

            json_list = json.loads(response.text)
            reader = UGJsonReader()
            auag = reader.parse_json(json_list=json_list)
            logging.debug("Got {0} users and {1} groups from TS.".format(auag.number_users(), auag.number_groups()))

            

            if get_group_privileges:
                group_priv_api = SetGroupPrivilegesAPI(tsurl=self.tsurl, username=self.username,
                                                       password=self.password, disable_ssl=self.disable_ssl)
                for group in auag.get_groups():
                    group_privs = group_priv_api.get_privileges_for_group(group_name=group.name)
                    group.privileges = copy.copy(group_privs)


            return auag

        else:
            logging.error("Failed to get users and groups.")
            raise requests.ConnectionError(
                f"Error getting users and groups ({response.status_code})",
                response.text,
            )

    @api_call
    def get_user_metadata(self):
        """
        Returns a list of User objects based on the metadata.
        :return: A list of user objects.
        :rtype: list of User
        """
        url = self.format_url(SyncUsersAndGroups.USER_METADATA_URL)
        response = self.session.get(url, cookies=self.cookies)
        users = []
        if response.status_code == 200:
            logging.info("Successfully got user metadata.")
            json_list = json.loads(response.text)
            logging.debug("metadata for users:  %s" % response.text)
            for value in json_list:
                user = User(
                    name=value.get("name", None),
                    display_name=value.get("displayName", None),
                    mail=value.get("mail", None),
                    group_names=value.get("groupNames", None),
                    visibility=value.get("visibility", None),
                    created=value.get("created", None),
                    user_id=value.get("id", None)
                )
                users.append(user)
            return users

        else:
            logging.error("Failed to get user metadata.")
            raise requests.ConnectionError(
                "Error getting user metadata (%d)" % response.status_code,
                response.text,
                )

    def sync_users_and_groups(self, users_and_groups, apply_changes=False, remove_deleted=False, batch_size=-1, create_groups=False, merge_groups=False, log_dir='logs/', archive_dir='archive/', current_timestamp=dt.datetime.now().strftime('%d%b%y_%H-%M-%S-%f'), sync_files=[], email_config_json=None):
        """
        Syncs users and groups.
        :param users_and_groups: List of users and groups to sync.
        :type users_and_groups: UsersAndGroups
        :param apply_changes: If true, changes will be applied.  If not, then it just says what will happen.
        :type apply_changes: bool
        :param remove_deleted: Flag to removed deleted users.  If true, delete.  Cannot be used with batch_size.
        :type remove_deleted: bool
        :param log_dir: Path to log directory.
        :type log_dir: str
        :param archive_dir: Path to archive directory.
        :type archive_dir: str
        :param current_timestamp: Timestamp, usable in file names.
        :type current_timestamp: str
        :param email_config_json: Path to JSON email config file.
        :type email_config_json: str
        :returns: The response from the sync.
        """

        if not apply_changes:
            print("Testing sync.  Changes will not be applied.  Use --apply_changes flag to apply.")

        if remove_deleted and batch_size > 0:
            raise Exception("Cannot have remove_deleted True and batch_size > 0")

        existing_ugs = self.get_all_users_and_groups() if (create_groups or merge_groups) else None

        if create_groups:
            self.__add_all_user_groups(existing_ugs, users_and_groups)

        if merge_groups:
            SyncUsersAndGroups.__merge_groups_into_new(existing_ugs, users_and_groups)

        # Sync in batches
        if batch_size > 0:
            all_users = users_and_groups.get_users()
            while len(all_users) > 0:
                # get a batch of users to sync.
                user_batch = all_users[:batch_size]
                del all_users[:batch_size]

                ug_batch = UsersAndGroups()
                for user in user_batch:
                    ug_batch.add_user(users_and_groups.get_user(user.name))
                    for group_name in user.groupNames:  # Add the user's groups as well.
                        ug_batch.add_group(users_and_groups.get_group(group_name=group_name),
                                           duplicate=UsersAndGroups.IGNORE_ON_DUPLICATE)

                self._sync_users_and_groups(users_and_groups=ug_batch,
                                            apply_changes=apply_changes, remove_deleted=remove_deleted,
                                            log_dir=log_dir, archive_dir=archive_dir, 
                                            current_timestamp=current_timestamp, 
                                            email_config_json=email_config_json, 
                                            sync_files=sync_files)

        # Sync all users and groups.
        else:
            self._sync_users_and_groups(users_and_groups=users_and_groups,
                apply_changes=apply_changes,
                remove_deleted=remove_deleted,
                log_dir=log_dir,
                archive_dir=archive_dir,
                current_timestamp=current_timestamp, 
                email_config_json=email_config_json, 
                sync_files=sync_files)

    @staticmethod
    def __add_all_user_groups(original_ugs, new_ugs):
        """
        Causes the creation/addition of all groups that users are assigned to, but which don't 
        already exist as Group objects created from group CSV/Groups Excel sheet. This will first 
        get existing groups to make sure existing groups aren't updated.
        :param original_ugs: The original users and groups, possibly from ThoughtSpot.
        :type original_ugs: UsersAndGroups
        :param new_ugs: The new users and groups that will be synced.
        :type new_ugs: UsersAndGroups
        :return: Nothing.  New users and groups list is updated.
        :rtype: None
        """
        new_user_groups = set()
        for user in new_ugs.get_users():
            new_user_groups.update(user.groupNames)

        for group_name in new_user_groups:
            if not new_ugs.get_group(group_name=group_name): # The group isn't in the new list.
                old_group = original_ugs.get_group(group_name=group_name)
                if old_group:  # the group is in the old list, so use that one.
                    new_ugs.add_group(g=old_group)
                else:
                    new_ugs.add_group(Group(name=group_name, display_name=group_name,
                                            description="Implicitely created group."))


    @staticmethod
    def __merge_groups_into_new(original_ugs, new_ugs):
        """
        Merges the original groups for the users in the new users and groups.  Useful when updating and not replacing
        users.
        :param original_ugs: The original users and groups, possibly from ThoughtSpot.
        :type original_ugs: UsersAndGroups
        :param new_ugs: The new users and groups that will be synced.
        :type new_ugs: UsersAndGroups
        :return: Nothing.  New users and groups list is updated.
        :rtype: None
        """
        for new_user in new_ugs.get_users():
            original_user = original_ugs.get_user(new_user.name)
            if original_user:
                new_user.groupNames.extend(original_user.groupNames)

    @api_call
    def _sync_users_and_groups(self, users_and_groups, apply_changes=True, remove_deleted=False, log_dir='logs/', archive_dir='archive/', current_timestamp=dt.datetime.now().strftime('%d%b%y_%H-%M-%S-%f'), sync_files=[], email_config_json=None):
        """
        Syncs users and groups.
        :param users_and_groups: List of users and groups to sync.
        :type users_and_groups: UsersAndGroups
        :param apply_changes: If true, changes will be applied.  If not, then it just says what will happen.
        :type apply_changes: bool
        :param remove_deleted: Flag to removed deleted users.  If true, delete.  Cannot be used with batch_size.
        :type remove_deleted: bool
        :param log_dir: Path to log directory.
        :type log_dir: str
        :param archive_dir: Path to archive directory.
        :type archive_dir: str
        :param current_timestamp: Timestamp, usable in file names.
        :type current_timestamp: str
        :param email_config_json: Path to JSON email config file.
        :type email_config_json: str
        :returns: The response from the sync.
        """
        
        # Set up email server and credentials for sending outcome alerts.
        if email_config_json:
            with open(email_config_json) as json_file:
                email_data = json.load(json_file)
                smtp_server = email_data['smtp_server']
                port = 587 #email_data['port']
                sender_email = email_data['sender_email']
                receiver_emails = email_data['receiver_emails']
                password = email_data['password']

            # Create a secure SSL context
            context = ssl.create_default_context()

        is_valid = users_and_groups.is_valid()
        if not is_valid[0]:
            # print("Invalid user and group structure.")
            if email_config_json:
                message = """\
Subject: Failure - Sync with TS

Sync with TS failed due to invalid users/groups."""
                with smtplib.SMTP(smtp_server, port) as server:
                    #server.ehlo()
                    server.starttls(context=context)
                    #server.ehlo()
                    server.login(sender_email, password)
                    server.sendmail(sender_email, receiver_emails, message)
                logging.info("Sent failure email")
            logging.error("Invalid users and groups.")
            raise Exception("Invalid users and groups")

        url = self.format_url(SyncUsersAndGroups.SYNC_ALL_URL)

        logging.debug("Calling %s" % url)
        logging.debug("Sending {0} users and {1} groups.".format(users_and_groups.number_users(), users_and_groups.number_groups()))
        json_str = users_and_groups.to_json()
        #logging.info("%s" % json_str)
        json.loads(json_str)  # do a load to see if it breaks due to bad JSON.

        # Get the temp folder from the environment settings, so it will work cross platform.
        logging.debug("Using temp folder:"+tempfile.gettempdir())
        tmp_file = tempfile.gettempdir() + "/ug.json.%d" % time.time()

        with open(tmp_file, "w") as out:
            out.write(json_str)

        params = {
            "principals": (tmp_file, open(tmp_file, "rb"), "text/json"),
            "applyChanges": json.dumps(apply_changes),
            "removeDeleted": json.dumps(remove_deleted),
        }

        if self.global_password:
            params["password"] = self.global_password

        response = self.session.post(url, files=params, cookies=self.cookies)
        
        # A bunch of stuff for logging the result of the request immediately above

        now = dt.datetime.now()
        now_str = str(now)

        # If an existing non-dir file is named log_dir, change log_dir to the working directory.
        # Otherwise, use log_dir, creating it if it doesn't exist
        try:
            os.makedirs(log_dir)
        except FileExistsError:
            if os.path.isfile(log_dir):
                logging.warn("There is already a file called '{0}'. Logs will instead be saved to '.' (the current working directory).").format(log_dir)
                log_dir = '.'
            else: # There is already a dir with that name, that's great (i.e. do nothing, thereby we use that dir)
                pass
                


        # Ensure that the dir path ends with exactly one '/'
        if not log_dir.endswith('/'):
            log_dir += '/'

        # The log files will contain a timestamp
        current_timestamp = now.strftime('%d%b%y_%H-%M-%S-%f')

        # If the --apply_changes flag was absent, this is all in "test mode" and changes are not really being made.
        # This will be capured in the names of the log files.
        if not apply_changes:
            current_timestamp += '_Test_Mode'
        
        if response.status_code == 200:
            logging.info("Successfully synced users and groups.")
            changes_json_bytes = response.text.encode("utf-8")
            #logging.info(changes_json_bytes)
            changes_dict_orig = json.loads(changes_json_bytes) # a dict like {'usersUpdated: ['bob','john'], ...} 
            numbers_of_updates = {}
            # log number of changes by type
            for key in changes_dict_orig.keys():
                value = len(changes_dict_orig[key])
                numbers_of_updates[key] = value
            for key in numbers_of_updates.keys():
                logging.info("{0}: {1}".format(key, numbers_of_updates[key]))
            # log JSON response, limited to 1000 chars
            limited_json = changes_json_bytes[:1000]
            if limited_json != changes_json_bytes:
                limited_json += b'... There is more JSON. See autogenerated CSV or JSON log file for more details on the changes made.'
            logging.info("JSON response from TS:\n{0}".format(limited_json))

            # Restructure changes_dict_orig (from JSON specifying changes made) in order to create a CSV log of all changes

            changes_dicts = [] # will be a list of dicts like [{'entity':'bob', 'entity_type':'user', 'change_type':'Added'},...]
            entity_type = None # Will have 2 possible values: 'User' or 'Group'
            change_type = None # WIll have 3 possible values: 'Added', 'Updated', or 'Deleted'
            keys = list(changes_dict_orig.keys())
            if set(keys) != set(['usersAdded', 'usersUpdated', 'usersDeleted', 'groupsAdded', 'groupsUpdated',
            'groupsDeleted']):
                logging.warn("Logging to CSV will fail: JSON response keys are unexpected: {0}".format(keys))
            for key in keys: 
                if 'users' in key: # e.g. 'usersAdded' -> True
                    entity_type = 'User'
                    change_type = key.replace('users', '') # e.g. 'usersAdded' -> 'Added'
                elif 'groups' in key:
                    entity_type = 'Group'
                    change_type = key.replace('groups', '')
                else:
                    entity_type = None
                    change_type = None
                for entity in changes_dict_orig[key]:
                    changes_dicts.append({'entity': entity, 'entity_type': entity_type, 'change_type': change_type,
                    'timestamp': now_str})


        # If an existing non-dir file is named archive_dir, change archive_dir to the working directory.
        # Otherwise, use archive_dir, creating it if it doesn't exist
            try:
                os.makedirs(archive_dir)
            except FileExistsError:
                if os.path.isfile(archive_dir):
                    logging.warn("There is already a non-dir file called '{0}'. Logs will instead be saved to '.' (the current working directory).").format(archive_dir)
                    archive_dir = './'

            # Ensure that the dir paths end in a single '/'
            if not archive_dir.endswith('/'):
                archive_dir += '/'

            #for dir_str in [log_dir, archive_dir]:
            #    if not dir_str.endswith('/'):
            #        dir_str += '/'


            log_file_name_no_ext = log_dir + 'changes_' + current_timestamp

            changes_occurred = len(changes_dicts) > 0 

            extra_file_name_component = '_NO_CHANGE' if not changes_occurred else ''

            log_file_name_no_ext += extra_file_name_component # adds '_NO_CHANGE' to the file name if there were no changes'

            # Make a CSV log of changes

            # TODO Add column specifying the change that wasy made so that people don't have to cross reference log file with archive
            # TODO Add the name of the original CSV to the name of the log file so the connection is explicit



            csv_log_file_name = log_file_name_no_ext + '.csv'

            with open(csv_log_file_name, 'w') as changes_file_csv:
                if changes_occurred:
                    writer = csv.DictWriter(changes_file_csv, fieldnames=changes_dicts[0].keys())
                    writer.writeheader()
                    for change in changes_dicts:
                        writer.writerow(change)
                    logging.info("Changes occurred: CSV log saved to {0}".format(csv_log_file_name))
                else:
                    logging.info("No changes: A file showing no changes was created at {0}".format(csv_log_file_name))
                    #changes_file.write("No changes occurred when Python updated TS at %s" % now_str)

            # Log original JSON (in case, for instance, something goes wrong with the CSV log)

            json_log_file_name = log_file_name_no_ext + '.json'
            
            with open(json_log_file_name, 'w') as changes_file_json:
                writer = changes_file_json.write(str(changes_json_bytes))
            logging.info("Log of JSON response saved to ./{0}".format(json_log_file_name))

            if True:#apply_changes:
                if len(sync_files) > 0: # i.e. If you are syncing an excel or CSV(s)
                    logging.info("Archiving these synced files: {0}".format(str(sync_files)))
                    for f in sync_files:
                        shutil.copy2(f, archive_dir + f.split("/")[-1]) # tries to preserve metadata during move
                        #shutil.move(sync_file, archive_dir + sync_file)
                else: # i.e. If you synced to a DB
                    logging.info("Saving the UsersAndGroups you sent to TS as a CSV in {0}".format(log_dir))
                    sent_user_csv_filename = '{0}_{1}.csv'.format('sent_user_data', current_timestamp)
                    sent_group_csv_filename = '{0}_{1}.csv'.format('sent_group_data', current_timestamp) #log_file_name_no_ext.replace('changes', 'archive_of_sent_data')
                    users_and_groups.write_to_csv(log_dir, sent_user_csv_filename, sent_group_csv_filename)#'{0}.csv'.format(archive_csv_filename))


            #logging.info("Finished.")
            #print(len(log.handlers))
            #for h in log.handlers:
            #    h.close()
            #    log.removeFilter(h)

            # TODO send success email
            if email_config_json:
                message = """\
Subject: Success - Sync with TS

Sync with TS was successful."""
                with smtplib.SMTP(smtp_server, port) as server:
                    #server.ehlo()
                    server.starttls(context=context)
                    #server.ehlo()
                    server.login(sender_email, password)
                    server.sendmail(sender_email, receiver_emails, message)
                logging.info("Sent success email")
            
            return response

        else:
            # Send failure email
            if email_config_json:
                message = """\
Subject: Failure - Sync with TS

Sync with TS failed, with status code {0}.""".format(response.status_code)
                with smtplib.SMTP(smtp_server, port) as server:
                    server.starttls(context=context)
                    server.login(sender_email, password)
                    server.sendmail(sender_email, receiver_emails, message)
                logging.info("Sent failure email")

            logging.error("Failed to sync users and groups.")
            logging.info(response.text.encode("utf-8"))
            with open("{0}users_and_groups_failed_sync_{1}.json".format(log_dir, current_timestamp), "w") as outfile:
                outfile.write(str(json_str.encode("utf-8")))
            raise requests.ConnectionError(
                "Error syncing users and groups (%d)" % response.status_code,
                response.text,
            )


    @api_call
    def delete_users(self, usernames):
        """
        Deletes a list of users based on their user name.
        :param usernames: List of the names of the users to delete.
        :type usernames: list of str
        """

        # for each username, get the guid and put in a list.  Log errors for users not found, but don't stop.
        logging.info("Deleting users %s." % usernames)
        url = self.format_url(SyncUsersAndGroups.USER_METADATA_URL)
        response = self.session.get(url, cookies=self.cookies)
        users = {}
        if response.status_code == 200:
            logging.info("Successfully got user metadata.")
            logging.debug("response:  %s" % response.text)
            json_list = json.loads(response.text)
            for h in json_list:
                name = h["name"]
                user_id = h["id"]
                users[name] = user_id

            user_list = []
            for u in usernames:
                group_id = users.get(u, None)
                if not group_id:
                    logging.warning("User %s not found, not attempting to delete this user." % u)
                else:
                    user_list.append(group_id)

            if not user_list:
                logging.warning("No valid users to delete.")
                return

            logging.info("Deleting user IDs %s." % user_list)
            url = self.format_url(SyncUsersAndGroups.DELETE_USERS_URL)
            params = {"ids": json.dumps(user_list)}
            response = self.session.post(
                url, data=params, cookies=self.cookies
            )

            if response.status_code != 204:
                logging.error("Failed to delete %s" % user_list)
                raise requests.ConnectionError(
                    "Error getting users and groups (%d)"
                    % response.status_code,
                    response.text,
                )

        else:
            logging.error("Failed to get users and groups.")
            raise requests.ConnectionError(
                "Error getting users and groups (%d)" % response.status_code,
                response.text,
            )

    def delete_user(self, username):
        """
        Deletes the user with the given username.
        :param username: The name of the user.
        :type username: str
        """
        self.delete_users([username])  # just call the list method.

    @api_call
    def delete_groups(self, groupnames):
        """
        Deletes a list of groups based on their group name.
        :param groupnames: List of the names of the groups to delete.
        :type groupnames: list of str
        """

        # for each groupname, get the guid and put in a list.  Log errors for groups not found, but don't stop.
        url = self.format_url(SyncUsersAndGroups.GROUP_METADATA_URL)
        response = self.session.get(url, cookies=self.cookies)
        groups = {}
        if response.status_code == 200:
            logging.info("Successfully got group metadata.")
            json_list = json.loads(response.text)
            # for h in json_list["headers"]:
            for h in json_list:
                name = h["name"]
                group_id = h["id"]
                groups[name] = group_id

            group_list = []
            for u in groupnames:
                group_id = groups.get(u, None)
                if not group_id:
                    eprint(
                        "WARNING:  group %s not found, not attempting to delete this group."
                        % u
                    )
                else:
                    group_list.append(group_id)

            if not group_list:
                eprint("No valid groups to delete.")
                return

            url = self.format_url(SyncUsersAndGroups.DELETE_GROUPS_URL)
            params = {"ids": json.dumps(group_list)}
            response = self.session.post(
                url, data=params, cookies=self.cookies
            )

            if response.status_code != 204:
                logging.error("Failed to delete %s" % group_list)
                raise requests.ConnectionError(
                    "Error getting groups and groups (%d)"
                    % response.status_code,
                    response.text,
                )

        else:
            logging.error("Failed to get users and groups.")
            raise requests.ConnectionError(
                "Error getting users and groups (%d)" % response.status_code,
                response.text,
            )

    def delete_group(self, groupname):
        """
        Deletes the group with the given groupname.
        :param groupname: The name of the group.
        :type groupname: str
        """
        self.delete_groups([groupname])  # just call the list method.

    @api_call
    def update_user_password(self, userid, currentpassword, password):
        """
        Updates the password for a user.
        :param userid: User id for the user to change the password for.
        :type userid: str
        :param currentpassword: Password for the logged in user with admin privileges.
        :type currentpassword: str
        :param password: New password for the user.
        :type password: str
        """

        url = self.format_url(SyncUsersAndGroups.UPDATE_PASSWORD_URL)
        params = {
            "name": userid,
            "currentpassword": currentpassword,
            "password": password,
        }

        response = self.session.post(url, data=params, cookies=self.cookies)

        if response.status_code == 204:
            logging.info("Successfully updated password for %s." % userid)
        else:
            logging.error("Failed to update password for %s." % userid)
            raise requests.ConnectionError(
                "Error (%d) updating user password for %s:  %s"
                % (response.status_code, userid, response.text)
            )


class Privileges:
    """
    Contains the various privileges that groups can have.
    """
    IS_ADMINSTRATOR = "ADMINISTRATION"
    CAN_UPLOAD_DATA = "USERDATAUPLOADING"
    CAN_DOWNLOAD_DATA = "DATADOWNLOADING"
    CAN_SHARE_WITH_ALL = "SHAREWITHALL"
    CAN_MANAGE_DATA = "DATAMANAGEMENT"
    CAN_SCHEDULE_PINBOARDS = "JOBSCHEDULING"
    CAN_USE_SPOTIQ = "A3ANALYSIS"
    CAN_ADMINISTER_RLS = "BYPASSRLS"
    CAN_AUTHOR = "AUTHORING"
    CAN_MANAGE_SYSTEM = "SYSTEMMANAGEMENT"


class SetGroupPrivilegesAPI(BaseApiInterface):

    # Note that some of these URLs are not part of the public API and subject to change.
    METADATA_LIST_URL = "/tspublic/v1/metadata/listobjectheaders?type=USER_GROUP"
    METADATA_DETAIL_URL = "/metadata/detail/{guid}?type=USER_GROUP"

    ADD_PRIVILEGE_URL = "/tspublic/v1/group/addprivilege"
    REMOVE_PRIVILEGE_URL = "/tspublic/v1/group/removeprivilege"

    def __init__(self, tsurl, username, password, disable_ssl=False):
        """
        Creates a new sync object and logs into ThoughtSpot
        :param tsurl: Root ThoughtSpot URL, e.g. http://some-company.com/
        :param username: Name of the admin login to use.
        :param password: Password for admin login.
        :param disable_ssl: If true, then disable SSL for calls.
        """
        super(SetGroupPrivilegesAPI, self).__init__(
            tsurl=tsurl,
            username=username,
            password=password,
            disable_ssl=disable_ssl,
        )

    @api_call
    def get_privileges_for_group(self, group_name):
        """
        Gets the current privileges for a given group.
        :param group_name:  Name of the group to get privileges for.
        :returns: A list of privileges.
        :rtype: list of str
        """
        url = self.format_url(
            SetGroupPrivilegesAPI.METADATA_LIST_URL
        ) + "&pattern=" + group_name
        response = self.session.get(url, cookies=self.cookies)
        if response.status_code == 200:  # success
            results = json.loads(response.text)
            try:
                group_id = results[0][
                    "id"
                ]  # should always be present, but might want to add try / catch.
                detail_url = SetGroupPrivilegesAPI.METADATA_DETAIL_URL.format(
                    guid=group_id
                )
                detail_url = self.format_url(detail_url)
                detail_response = self.session.get(
                    detail_url, cookies=self.cookies
                )
                if detail_response.status_code == 200:  # success
                    privileges = json.loads(detail_response.text)["privileges"]
                    return privileges

                else:
                    logging.error(
                        "Failed to get privileges for group %s" % group_name
                    )
                    raise requests.ConnectionError(
                        "Error (%d) setting privileges for group %s.  %s"
                        % (response.status_code, group_name, response.text)
                    )

            except Exception:
                logging.error("Error getting group details.")
                raise

        else:
            logging.error("Failed to get privileges for group %s" % group_name)
            raise requests.ConnectionError(
                "Error (%d) setting privileges for group %s.  %s"
                % (response.status_code, group_name, response.text)
            )

    @api_call
    def add_privilege(self, groups, privilege):
        """
        Adds a privilege to a list of groups.
        :param groups List of groups to add the privilege to.
        :type groups: list of str
        :param privilege: Privilege being set.
        :type privilege: str
        """

        url = self.format_url(SetGroupPrivilegesAPI.ADD_PRIVILEGE_URL)

        params = {"privilege": privilege, "groupNames": json.dumps(groups)}
        response = self.session.post(url, files=params, cookies=self.cookies)

        if response.status_code == 204:
            logging.info(
                "Successfully added privilege %s for groups %s."
                % (privilege, groups)
            )
        else:
            logging.error(
                "Failed to add privilege %s for groups %s."
                % (privilege, groups)
            )
            raise requests.ConnectionError(
                "Error (%d) adding privilege %s for groups %s.  %s"
                % (response.status_code, privilege, groups, response.text)
            )

    @api_call
    def remove_privilege(self, groups, privilege):
        """
        Removes a privilege to a list of groups.
        :param groups List of groups to add the privilege to.
        :type groups: list of str
        :param privilege: Privilege being removed.
        :type privilege: str
        """

        url = self.format_url(SetGroupPrivilegesAPI.REMOVE_PRIVILEGE_URL)

        params = {"privilege": privilege, "groupNames": json.dumps(groups)}
        response = self.session.post(url, files=params, cookies=self.cookies)

        if response.status_code == 204:
            logging.info(
                "Successfully removed privilege %s for groups %s."
                % (privilege, groups)
            )
        else:
            logging.error(
                "Failed to remove privilege %s for groups %s."
                % (privilege, groups)
            )
            raise requests.ConnectionError(
                "Error (%d) removing privilege %s for groups %s.  %s"
                % (response.status_code, privilege, groups, response.text)
            )


class TransferOwnershipApi(BaseApiInterface):

    TRANSFER_OWNERSHIP_URL = "/tspublic/v1/user/transfer/ownership"

    def __init__(self, tsurl, username, password, disable_ssl=False):
        """
        Creates a new sync object and logs into ThoughtSpot
        :param tsurl: Root ThoughtSpot URL, e.g. http://some-company.com/
        :param username: Name of the admin login to use.
        :param password: Password for admin login.
        :param disable_ssl: If true, then disable SSL for calls.
        """
        super(TransferOwnershipApi, self).__init__(
            tsurl=tsurl,
            username=username,
            password=password,
            disable_ssl=disable_ssl,
        )

    @api_call
    def transfer_ownership(self, from_username, to_username):
        """
        Transfer ownership of all objects from one user to another.
        :param from_username: User name for the user to change the ownership for.
        :type from_username: str
        :param to_username: User name for the user to change the ownership to.
        :type to_username: str
        """

        url = self.format_url(TransferOwnershipApi.TRANSFER_OWNERSHIP_URL)
        url = url + "?fromUserName=" + from_username + "&toUserName=" + to_username
        response = self.session.post(url, cookies=self.cookies)

        if response.status_code == 204:
            logging.info(
                "Successfully transferred ownership to %s." % to_username
            )
        else:
            logging.error("Failed to transfer ownership to %s." % to_username)
            raise requests.ConnectionError(
                f"Error ({response.status_code}) transferring  ownership to {to_username}:  {response.text}"
            )
