#!/usr/bin/env python3

import argparse
import collections
import json
import logging
import sys
import yaml

import xml.etree.ElementTree as ET

log = logging.getLogger('jira-manglr')

__doc__ = """
Mangle Jira imports
"""

def split_xml_root(e):
    # output root element open
    xml = ET.tostring(e, short_empty_elements=False)
    xml_open, xml_close1, xml_close2 = xml.partition(b'</')

    return xml_open, xml_close1 + xml_close2

class App:
    def __init__(self, keep_users=None, rewrite_directories=None):
        self.element_count = 0
        self.all_users = set()
        self.project_users = set()
        self.internal_directory_id = None
        self.remap_directory_id = None

        self.keep_users = set()
        self.keep_directories = set()
        self.rewrite_directories = {}

        if keep_users:
            self.keep_users = keep_users

        if rewrite_directories:
            self.keep_directories = {str(id) for id in rewrite_directories.values()}
            self.rewrite_directories = {str(k): str(v) for k, v in rewrite_directories.items()}

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

    def parse(self, file, count_interval=10000):
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

    def filter_attr_set(self, e, attrs, rewrite=None):
        """
            attrs   - { attr: set(values) }
        """

        values = {attr: e.get(attr) for attr in attrs}

        if any((e.get(attr) not in attrs[attr]) for attr in attrs):
            log.info("DROP %s %s", e.tag, values)
            return None

        if rewrite:
            for attr, map in rewrite.items():
                old = e.get(attr)
                new = map.get(old)

                if not new:
                    log.info("DROP %s rewrite:%s=%s", e.tag, attr, old)
                    return None
                else:
                    log.info("REWRITE %s %s: %s -> %s", e.tag, attr, old, new)
                    e.set(attr, new)

        log.info("KEEP %s %s", e.tag, values)
        return e

    def filter(self, e):
        if e.tag in ('AuditChangedValue', 'AuditItem', 'AuditLog'):
            return None
        elif e.tag in ('OAuthServiceProviderToken', ):
            return None
        elif e.tag == 'Avatar' and e.get('avatarType') == 'user' and e.get('owner'):
            return self.filter_attr_set(e, {'owner': self.keep_users})
        elif e.tag == 'User':
            return self.filter_attr_set(e, {'userName': self.keep_users},
                rewrite = {'directoryId': self.rewrite_directories},
            )
        elif e.tag == 'ApplicationUser':
            return self.filter_attr_set(e,  {'userKey': self.keep_users})
        elif e.tag == 'Group':
            return self.filter_attr_set(e, {}, # TODO
                rewrite = {'directoryId': self.rewrite_directories},
            )
        elif e.tag == 'Membership' and e.get('membershipType') == 'GROUP_USER':
            return self.filter_attr_set(e, {'childName': self.keep_users},
                rewrite = {'directoryId': self.rewrite_directories},
            )
        elif e.tag == 'UserAttribute':
            return self.filter_attr_set(e, {},
                rewrite = {'directoryId': self.rewrite_directories},
            )
        elif e.tag == 'UserHistoryItem':
            return self.filter_attr_set(e, {'username': self.keep_users})
        elif e.tag == 'SearchRequest':
            return self.filter_attr_set(e, {'author': self.keep_users})
        elif e.tag == 'RememberMeToken':
            return self.filter_attr_set(e, {'username': self.keep_users})
        elif e.tag == 'PortalPage' and e.get('username'):
            return self.filter_attr_set(e, {'username': self.keep_users})
        elif e.tag == 'ColumnLayout' and e.get('username'):
            return self.filter_attr_set(e, {'username': self.keep_users})
        elif e.tag == 'ExternalEntity':
            # TODO: drop all?
            return self.filter_attr_set(e, {'name': self.keep_users})
        elif e.tag == 'FavouriteAssociations':
            return self.filter_attr_set(e, {'username': self.keep_users})
        elif e.tag == 'Feature' and e.get('featureType') == 'user':
            return self.filter_attr_set(e, {'userKey': self.keep_users})
        elif e.tag == 'FilterSubscription':
            return self.filter_attr_set(e, {'username': self.keep_users})
        elif e.tag == 'Notification' and e.get('type') == 'Single_User':
            return self.filter_attr_set(e, {'parameter': self.keep_users})
        elif e.tag == 'SchemePermissions' and e.get('type') == 'user':
            return self.filter_attr_set(e, {'parameter': self.keep_users})
        elif e.tag == 'OSHistoryStep' and e.get('caller'):
            return self.filter_attr_set(e, {'caller': self.keep_users})
        elif e.tag == 'Directory':
            return self.filter_attr_set(e, {'id': self.keep_directories})
        elif e.tag in ('DirectoryAttribute', 'DirectoryOperation'):
            return self.filter_attr_set(e, {'directoryId': self.keep_directories})
        else:
            return e



    def process(self, input, output, count_interval=10000):
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
                root_open, root_close = split_xml_root(root)

                log.debug("ROOT %s => %s + %s", e, root_open, root_close)

                output.write(root_open)

            elif level == 2 and event == 'start':
                input_count += 1
                input_counts[e.tag] += 1

                if input_count % count_interval == 0:
                    log.info("Processing %d/%d elements...", input_count, self.element_count)

            elif level == 1 and event == 'end':
                ref = e # for cleanup

                # process each top-level element
                e = self.filter(e)

                if e is None:
                    pass
                else:
                    output_count += 1
                    output_counts[e.tag] += 1

                    ET.ElementTree(e).write(output, xml_declaration=False)

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

    def scan(self, file):
        for e in self.parse(file):
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

    def verify(self, file):
        """
            Log any tags having attributes with dropped usernames
        """

        reject_users = self.all_users - self.keep_users

        total_count = 0
        total_counts = collections.defaultdict(int)
        count = 0
        counts = collections.defaultdict(int)

        for e in self.parse(file):
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


def main():
    parser = argparse.ArgumentParser(
        formatter_class = argparse.ArgumentDefaultsHelpFormatter,
        description     = __doc__,
    )

    parser.set_defaults(log_level=logging.WARN)

    parser.add_argument('-q', '--quiet', action='store_const', dest='log_level', const=logging.ERROR, help="Do not log warnings")
    parser.add_argument('-v', '--verbose', action='store_const', dest='log_level', const=logging.INFO, help="Log info messages")
    parser.add_argument('--debug', action='store_const', dest='log_level', const=logging.DEBUG, help="Log debug messages")

    parser.add_argument('--input', required=True) # must be a re-openable path, not a File or sys.stdin
    parser.add_argument('--load-state', metavar='PATH')
    parser.add_argument('--save-state', metavar='PATH')
    parser.add_argument('--keep-users', metavar='PATH', help="List of additional users to keep")
    parser.add_argument('--rewrite-directories', metavar='PATH', help="YAML map of user/group directories to rewrite")
    parser.add_argument('--verify', action='store_true', help="Log any tags with dropped usernames")
    parser.add_argument('--output', type=argparse.FileType('wb'), default=sys.stdout)

    args = parser.parse_args()

    logging.basicConfig(
        level       = args.log_level,
        stream      = sys.stderr,
        format      = "%(asctime)s %(levelname)5s %(module)s: %(message)s",
    )

    keep_users = None
    keep_directories = None

    if args.keep_users:
        with open(args.keep_users) as file:
            keep_users = set(l.strip() for l in file if l.strip())

    if args.rewrite_directories:
        with open(args.rewrite_directories) as file:
            rewrite_directories = yaml.safe_load(file)

    app = App(
        keep_users = keep_users,
        rewrite_directories = rewrite_directories,
    )

    if args.load_state:
        with open(args.load_state, 'r') as file:
            app.load_state(json.load(file))
    else:
        app.scan(args.input)

    if args.save_state:
        with open(args.save_state, 'w') as file:
            json.dump(app.save_state(), file)
    elif args.verify:
        app.verify(args.input)
    else:
        app.process(args.input, args.output)

if __name__ == '__main__':
    main()
