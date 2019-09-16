#!/usr/bin/env python3

import argparse
import collections
import fnmatch
import io
import json
import logging
import sys
import yaml

import xml.etree.ElementTree as ET

log = logging.getLogger('jira-manglr')

__doc__ = """
Mangle Jira imports
"""

def split_xml_root(e, default_namespace=None):
    out = io.BytesIO()

    # output root element open
    et = ET.ElementTree(e)
    et.write(out,
        short_empty_elements = False,
        xml_declaration = True,
        default_namespace = default_namespace,
    )

    xml = out.getvalue()
    xml_open, xml_close1, xml_close2 = xml.partition(b'</')

    return xml_open, xml_close1 + xml_close2

def parse_xml(file, count_interval=10000):
    """
        Iterate over all top-level elements
    """

    count = 0
    root = None
    level = 0

    for event, e in ET.iterparse(file, events=['start', 'end']):
        if event == 'start':
            level += 1
        elif event == 'end':
            level -= 1

        log.debug("%2d %10s %s", level, event, e.tag)

        if level == 1 and event == 'start':
            # process root element
            root = e

        elif level == 2 and event == 'start':
            count += 1

            if count % count_interval == 0:
                log.info("Processing %d elements...", count)

        elif level == 1 and event == 'end':
            # process each top-level element
            yield e
            e.clear()

def process_xml(filter, input, output, count_interval=10000, count_total=None, default_namespace=None):
    """
        Process all all top-level elements
    """

    level = 0
    root_close = None

    input_count = 0
    input_counts = collections.defaultdict(int)
    output_count = 0
    output_counts = collections.defaultdict(int)

    for event, e in ET.iterparse(input, events=['start', 'end']):
        if event == 'start':
            level += 1
        elif event == 'end':
            level -= 1

        log.debug("%2d %10s %s", level, event, e.tag)

        if level == 1 and event == 'start':
            # clone just the top-level element
            root = ET.Element(e.tag, e.attrib)
            root.text = e.text

            # output root element open
            root_open, root_close = split_xml_root(root, default_namespace=default_namespace)

            log.debug("ROOT %s => %s + %s", e, root_open, root_close)

            output.write(root_open)

        elif level == 2 and event == 'start':
            input_count += 1
            input_counts[e.tag] += 1

            if input_count % count_interval == 0:
                if count_total:
                    log.info("Processing %d/%d elements...", input_count, count_total)
                else:
                    log.info("Processing %d elements...", input_count)

        elif level == 1 and event == 'end':
            ref = e # for cleanup

            # process each top-level element
            e = filter(e)

            if e is None:
                pass
            else:
                output_count += 1
                output_counts[e.tag] += 1

                et = ET.ElementTree(e)
                et.write(output,
                    xml_declaration = False,
                    default_namespace = default_namespace,
                )

            ref.clear()

        elif level == 0 and event == 'end':
            # output root element close
            output.write(b'\n')
            output.write(root_close + b'\n')

    log.info("Stats: %d/%d items = %.2f%%", output_count, input_count, output_count/input_count*100)

    for tag in input_counts:
        i = input_counts[tag]
        o = output_counts[tag]

        log.info("\t%-30s: %8d/%8d = %.2f%%", tag, o, i, o/i*100)

def filter_attr_set(e, attrs, rewrite=None):
    """
        attrs   - { attr: set(values) }
    """

    values = {attr: e.get(attr) for attr in attrs}

    if any((e.get(attr) not in attrs[attr]) for attr in attrs):
        log.info("DROP %s %s", e.tag, values)
        return None

    if rewrite:
        for attr, map in rewrite.items():
            if map is not None:
                old = e.get(attr)
                new = map.get(old)

                if new:
                    log.info("REWRITE %s %s: %s -> %s", e.tag, attr, old, new)
                    e.set(attr, new)

    log.debug("KEEP %s %s", e.tag, values)
    return e

def filter_attr_glob(e, attr, globs):
    value = e.get(attr)

    if any(fnmatch.fnmatch(value, pattern) for pattern in globs):
        log.info("DROP %s %s=%s", e.tag, attr, value)
        return None
    else:
        log.debug("KEEP %s %s=%s", e.tag, attr, value)
        return e


class EntityMangler:
    def __init__(self, keep_users=None, drop_users=None, rewrite_users=None, keep_groups=None, rewrite_directories=None, drop_osproperty=None):
        self.element_count = 0
        self.all_users = set()
        self.project_users = set()
        self.internal_directory_id = None
        self.remap_directory_id = None

        self.keep_users = set()
        self.drop_users = set()
        self.rewrite_users = {}
        self.keep_groups = set()
        self.keep_directories = set()
        self.rewrite_directories = {}
        self.drop_osproperty = []

        if keep_users:
            self.keep_users = set(keep_users)
        if drop_users:
            self.drop_users = set(drop_users)
        if rewrite_users:
            self.rewrite_users = dict(rewrite_users)

        if keep_groups:
            self.keep_groups = set(keep_groups)

        if rewrite_directories:
            self.keep_directories = {str(id) for id in rewrite_directories.values()}
            self.rewrite_directories = {str(k): str(v) for k, v in rewrite_directories.items()}

        if drop_osproperty:
            self.drop_osproperty = list(drop_osproperty)

    def save_state(self):
        return {
            'element_count': self.element_count,
            'all_users': list(self.all_users),
            'project_users': list(self.project_users),
            'internal_directory_id': self.internal_directory_id,
        }

    def load_state(self, state, keep_project_users=True):
        self.element_count = state['element_count']

        if 'project_role_actor_users' in state:
            self.project_users = set(state['project_role_actor_users'])
        else:
            self.project_users = set(state['project_users'])

        if 'all_users' in state:
            self.all_users = set(state['all_users'])

        if 'internal_directory_id' in state:
            self.internal_directory_id = state['internal_directory_id']

        if keep_project_users:
            self.keep_users |= self.project_users
            self.keep_users -= self.drop_users

    def filter(self, e):
        if e.tag in ('AuditChangedValue', 'AuditItem', 'AuditLog'):
            return None
        elif e.tag in ('OAuthServiceProviderToken', ):
            return None
        elif e.tag == 'Action':
            return filter_attr_set(e, {},
                rewrite = {'author': self.rewrite_users, 'updateauthor': self.rewrite_users},
            )
        elif e.tag == 'Avatar' and e.get('avatarType') == 'user' and e.get('owner'):
            return filter_attr_set(e, {'owner': self.keep_users},
                rewrite = {'owner': self.rewrite_users},
            )
        elif e.tag == 'User':
            return filter_attr_set(e, {'userName': self.keep_users, 'directoryId': self.rewrite_directories.keys()},
                rewrite = {'directoryId': self.rewrite_directories, 'userName': self.rewrite_users, 'lowerUserName': self.rewrite_users},
            )
        elif e.tag == 'ApplicationUser':
            return filter_attr_set(e,  {'userKey': self.keep_users},
                rewrite = {'userKey': self.rewrite_users, 'lowerUserName': self.rewrite_users},
            )
        elif e.tag == 'Group':
            return filter_attr_set(e, {'groupName': self.keep_groups, 'directoryId': self.rewrite_directories.keys()},
                rewrite = {'directoryId': self.rewrite_directories},
            )
        elif e.tag == 'Membership' and e.get('membershipType') == 'GROUP_USER':
            return filter_attr_set(e, {'childName': self.keep_users, 'parentName': self.keep_groups, 'directoryId': self.rewrite_directories.keys()},
                rewrite = {'directoryId': self.rewrite_directories, 'childName': self.rewrite_users, 'lowerChildName': self.rewrite_users},
            )
        elif e.tag == 'UserAttribute':
            return filter_attr_set(e, {'directoryId': self.rewrite_directories.keys()},
                rewrite = {'directoryId': self.rewrite_directories},
            )
        elif e.tag == 'UserHistoryItem':
            return filter_attr_set(e, {'username': self.keep_users},
                rewrite = {'entityId': self.rewrite_users, 'username': self.rewrite_users},
            )
        elif e.tag == 'SearchRequest':
            return filter_attr_set(e, {'author': self.keep_users},
                rewrite = {'author': self.rewrite_users, 'user': self.rewrite_users},
            )
        elif e.tag == 'SharePermissions' and e.get('type') == 'group':
            return filter_attr_set(e, {'param1': self.keep_groups})
        elif e.tag == 'RememberMeToken':
            return filter_attr_set(e, {'username': self.keep_users},
                rewrite = {'username': self.rewrite_users},
            )
        elif e.tag == 'ChangeGroup':
            return filter_attr_set(e, {},
                rewrite = {'author': self.rewrite_users},
            )
        elif e.tag == 'ChangeItem' and e.get('field') in ('assignee', 'reporter'):
            return filter_attr_set(e, {},
                rewrite = {'newvalue': self.rewrite_users, 'oldvalue': self.rewrite_users},
            )
        elif e.tag == 'FileAttachment':
            return filter_attr_set(e, {},
                rewrite = {'author': self.rewrite_users},
            )
        elif e.tag == 'Issue':
            return filter_attr_set(e, {},
                rewrite = {'assignee': self.rewrite_users, 'creator': self.rewrite_users, 'reporter': self.rewrite_users},
            )
        elif e.tag == 'Project':
            return filter_attr_set(e, {},
                rewrite = {'lead': self.rewrite_users},
            )
        elif e.tag == 'UserAssociation':
            return filter_attr_set(e, {'sourceName': self.keep_users},
                rewrite = {'sourceName': self.rewrite_users},
            )
        elif e.tag == 'ProjectRoleActor' and e.get('roletype') == 'atlassian-user-role-actor':
            return filter_attr_set(e, {'roletypeparameter': self.keep_users},
                rewrite = {'roletypeparameter': self.rewrite_users},
            )
        elif e.tag == 'PortalPage' and e.get('username'):
            return filter_attr_set(e, {'username': self.keep_users},
                rewrite = {'username': self.rewrite_users},
            )
        elif e.tag == 'ColumnLayout' and e.get('username'):
            return filter_attr_set(e, {'username': self.keep_users},
                rewrite = {'username': self.rewrite_users},
            )
        elif e.tag == 'ExternalEntity':
            # TODO: drop all?
            return filter_attr_set(e, {'name': self.keep_users},
                rewrite = {'name': self.rewrite_users},
            )
        elif e.tag == 'FavouriteAssociations':
            return filter_attr_set(e, {'username': self.keep_users},
                rewrite = {'username': self.rewrite_users},
            )
        elif e.tag == 'Feature' and e.get('featureType') == 'user':
            return filter_attr_set(e, {'userKey': self.keep_users})
        elif e.tag == 'FilterSubscription':
            return filter_attr_set(e, {'username': self.keep_users})
        elif e.tag == 'Notification' and e.get('type') == 'Single_User':
            return filter_attr_set(e, {'parameter': self.keep_users})
        elif e.tag == 'SchemePermissions' and e.get('type') == 'user':
            return filter_attr_set(e, {'parameter': self.keep_users})
        elif e.tag == 'SchemePermissions' and e.get('type') == 'group':
            return filter_attr_set(e, {'parameter': self.keep_groups})
        elif e.tag == 'OSHistoryStep' and e.get('caller'):
            return filter_attr_set(e, {'caller': self.keep_users},
                rewrite = {'caller': self.rewrite_users},
            )
        elif e.tag == 'OSPropertyEntry' and self.drop_osproperty:
            return filter_attr_glob(e, 'propertyKey', self.drop_osproperty)
        elif e.tag == 'Directory':
            return filter_attr_set(e, {'id': self.keep_directories})
        elif e.tag in ('DirectoryAttribute', 'DirectoryOperation'):
            return filter_attr_set(e, {'directoryId': self.keep_directories})
        elif e.tag == 'MailServer':
            return None
        else:
            return e

    def scan(self, file):
        for e in parse_xml(file):
            self.element_count += 1

            if e.tag == 'Directory' and e.get('type') == 'INTERNAL':
                self.internal_directory_id = e.get('id')

            if e.tag == 'User':
                self.all_users.add(e.get('userName'))

            elif e.tag == 'ProjectRoleActor':
                roletype = e.get('roletype')
                roletypeparameter = e.get('roletypeparameter')

                if roletype == 'atlassian-user-role-actor':
                    log.info("project_role_actor_users %s", roletypeparameter)

                    self.project_users.add(roletypeparameter)

    def process(self, input, output):
        process_xml(self.filter, input, output, count_total=self.element_count)

    def verify(self, file):
        """
            Log any tags having attributes with dropped usernames
        """

        reject_users = self.all_users - self.keep_users
        reject_users -= set(self.rewrite_users.values())
        reject_users |= self.rewrite_users.keys()

        total_count = 0
        total_counts = collections.defaultdict(int)
        count = 0
        counts = collections.defaultdict(int)

        for e in parse_xml(file):
            total_count += 1
            total_counts[e.tag] += 1

            attrs = {k: v for k, v in e.attrib.items() if v in reject_users}

            if attrs:
                log.warn("USER %s %s", e.tag, ', '.join(f'{k}<{v}>' for k, v in attrs.items()))

                count += 1
                counts[e.tag] += 1

        log.info("Summary: %d/%d = %.2f%% items", count, total_count, count/total_count*100)

        for tag in counts:
            log.info("\t%-30s: %8d/%8d = %.2f%%", tag, counts[tag], total_counts[tag], counts[tag]/total_counts[tag]*100)

def rewrite_data_rows(e, match, rewrite={}):
    table = e.get('tableName')
    cols = []

    for c in e.iterfind('{http://www.atlassian.com/ao}column'):
        cols.append(c.get('name'))

    log.debug("SCAN %s %r", table, cols)

    for row in e.iterfind('{http://www.atlassian.com/ao}row'):
        elements = {}

        for c, item in zip(cols, row):
            elements[c] = item

        if any(elements[c].text != v for c, v in match.items()):
            log.debug("SKIP %s %s", table, {c: e.text for c, e in elements.items() if c in match})
            continue

        for attr, map in rewrite.items():
            old = elements[attr].text
            new = map.get(old)

            if new:
                log.info("REWRITE %s %s=%s => %s", table, attr, old, new)
                elements[attr].text = new

    return e

class ActiveObjectMangler:
    XMLNS = 'http://www.atlassian.com/ao'
    DATA = '{http://www.atlassian.com/ao}data'

    def __init__(self, clear_tables=None, rewrite_users=None):
        self.clear_tables = []
        self.rewrite_users = None

        if clear_tables:
            self.clear_tables = list(clear_tables)

        if rewrite_users:
            self.rewrite_users = dict(rewrite_users)

    def filter(self, e):
        if e.tag == self.DATA and e.get('tableName') == 'AO_60DB71_BOARDADMINS':
            return rewrite_data_rows(e, {'TYPE': 'USER'}, {'KEY': self.rewrite_users})
        elif e.tag == self.DATA and e.get('tableName') == 'AO_60DB71_AUDITENTRY':
            return rewrite_data_rows(e, {}, {'USER': self.rewrite_users})
        elif e.tag == self.DATA and e.get('tableName') == 'AO_60DB71_RAPIDVIEW':
            return rewrite_data_rows(e, {}, {'OWNER_USER_NAME': self.rewrite_users})
        elif e.tag == self.DATA and e.get('tableName') == 'AO_8BAD1B_STATISTICS':
            return rewrite_data_rows(e, {}, {'C_USERKEY': self.rewrite_users})
        elif e.tag == self.DATA:
            return filter_attr_glob(e, 'tableName', self.clear_tables)
        else:
            return e

    def process(self, input, output):
        ET.register_namespace('', self.XMLNS)

        process_xml(self.filter, input, output, count_interval=10, default_namespace='')

def main():
    parser = argparse.ArgumentParser(
        formatter_class = argparse.ArgumentDefaultsHelpFormatter,
        description     = __doc__,
    )

    parser.set_defaults(log_level=logging.WARN)

    parser.add_argument('-q', '--quiet', action='store_const', dest='log_level', const=logging.ERROR, help="Do not log warnings")
    parser.add_argument('-v', '--verbose', action='store_const', dest='log_level', const=logging.INFO, help="Log info messages")
    parser.add_argument('--debug', action='store_const', dest='log_level', const=logging.DEBUG, help="Log debug messages")

    parser.add_argument('--input-entities') # must be a re-openable path, not a File or sys.stdin
    parser.add_argument('--load-state', metavar='PATH')
    parser.add_argument('--save-state', metavar='PATH')
    parser.add_argument('--keep-users', metavar='PATH', help="YAML list of users to keep")
    parser.add_argument('--drop-users', metavar='PATH', help="YAML list of users to drop")
    parser.add_argument('--rewrite-users', metavar='PATH', help="YAML map of users to rewrite")
    parser.add_argument('--keep-groups', metavar='PATH', help="YAML list of groups to keep")
    parser.add_argument('--rewrite-directories', metavar='PATH', help="YAML map of user/group directories to rewrite")
    parser.add_argument('--drop-osproperty', metavar='PATH', help="YAML list of OSProperty key globs to drop")
    parser.add_argument('--verify', action='store_true', help="Log any tags with dropped usernames")
    parser.add_argument('--output-entities', type=argparse.FileType('wb'), default=sys.stdout)

    parser.add_argument('--input-activeobjects') # must be a re-openable path, not a File or sys.stdin
    parser.add_argument('--output-activeobjects', type=argparse.FileType('wb'), default=sys.stdout)
    parser.add_argument('--clear-activeobject-tables', metavar='PATH', help="YAML list of ActiveObject table globs to drop")


    args = parser.parse_args()

    logging.basicConfig(
        level       = args.log_level,
        stream      = sys.stderr,
        format      = "%(asctime)s %(levelname)5s %(module)s: %(message)s",
    )

    keep_users = None
    drop_users = None
    rewrite_users = None
    rewrite_directories = None
    keep_groups = None
    drop_osproperty = None

    if args.keep_users:
        with open(args.keep_users) as file:
            keep_users = yaml.safe_load(file)

    if args.drop_users:
        with open(args.drop_users) as file:
            drop_users = yaml.safe_load(file)

    if args.rewrite_users:
        with open(args.rewrite_users) as file:
            rewrite_users = yaml.safe_load(file)

    if args.rewrite_directories:
        with open(args.rewrite_directories) as file:
            rewrite_directories = yaml.safe_load(file)

    if args.keep_groups:
        with open(args.keep_groups) as file:
            keep_groups = yaml.safe_load(file)

    if args.drop_osproperty:
        with open(args.drop_osproperty) as file:
            drop_osproperty = yaml.safe_load(file)


    if args.input_entities:

        app = EntityMangler(
            keep_users = keep_users,
            drop_users = drop_users,
            rewrite_users = rewrite_users,
            keep_groups = keep_groups,
            rewrite_directories = rewrite_directories,
            drop_osproperty = drop_osproperty,
        )

        if args.load_state:
            with open(args.load_state, 'r') as file:
                app.load_state(json.load(file))
        else:
            app.scan(args.input_entities)

        if args.save_state:
            with open(args.save_state, 'w') as file:
                json.dump(app.save_state(), file)
        elif args.verify:
            app.verify(args.input_entities)
        else:
            app.process(args.input_entities, args.output_entities)

    if args.input_activeobjects:
        clear_tables = None

        if args.clear_activeobject_tables:
            with open(args.clear_activeobject_tables) as file:
                clear_tables = yaml.safe_load(file)

        app = ActiveObjectMangler(
            clear_tables = clear_tables,
            rewrite_users = rewrite_users,
        )

        if args.output_activeobjects:
            app.process(args.input_activeobjects, args.output_activeobjects)

if __name__ == '__main__':
    main()
