#!/usr/bin/env python3

import argparse
import collections
import json
import logging
import sys

import xml.etree.ElementTree as ET

log = logging.getLogger('jira-manglr')

__doc__ = """
Mangle Jira imports
"""

def split_xml_root(e):
    # output root element open
    xml = ET.tostring(e, short_empty_elements=False, encoding='unicode')
    xml_open, xml_close1, xml_close2 = xml.partition('</')

    return xml_open, xml_close1 + xml_close2

class App:
    def __init__(self):
        self.element_count = 0
        self.project_users = set()

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

    def filter(self, e):
        if e.tag in ('AuditChangedValue', 'AuditItem', 'AuditLog'):
            return None
        elif e.tag == 'User':
            if e.get('userName') in self.project_users:
                log.info("KEEP User %s", e.get('userName'))
                return e
            else:
                log.info("DROP User %s", e.get('userName'))
                return None
        elif e.tag == 'ApplicationUser':
            if e.get('userKey') in self.project_users:
                log.info("KEEP ApplicationUser %s", e.get('userKey'))
                return e
            else:
                log.info("DROP ApplicationUser %s", e.get('userKey'))
                return None
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
                # output root element open
                root_open, root_close = split_xml_root(e)

                output.write(root_open + '\n')

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

                    #output.write('\t')
                    ET.ElementTree(e).write(output, xml_declaration=False, encoding='unicode')
                    #output.write('\n')

                ref.clear()

            elif level == 0 and event == 'end':
                # output root element close
                output.write('\n')
                output.write(root_close + '\n')

        log.info("Stats: %d/%d items = %.2f", output_count, input_count, output_count/input_count*100)

        for tag in input_counts:
            i = input_counts[tag]
            o = output_counts[tag]

            log.info("\t%s: %6d/%6d = %.2f", tag, o, i, o/i*100)

    def scan(self, file):
        for e in self.parse(file):
            self.element_count += 1

            if e.tag == 'ProjectRoleActor':
                roletype = e.get('roletype')
                roletypeparameter = e.get('roletypeparameter')

                if roletype == 'atlassian-user-role-actor':
                    log.info("project_role_actor_users %s", roletypeparameter)

                    self.project_users.add(roletypeparameter)

    def save_state(self):
        return {
            'element_count': self.element_count,
            'project_users': list(self.project_users),
        }

    def load_state(self, state):
        self.element_count = state['element_count']

        if 'project_role_actor_users' in state:
            self.project_users = set(state['project_role_actor_users'])
        else:
            self.project_users = set(state['project_users'])


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
    parser.add_argument('--output', type=argparse.FileType('w'), default=sys.stdout)
    parser.add_argument('--load-state', metavar='PATH')
    parser.add_argument('--save-state', metavar='PATH')

    args = parser.parse_args()

    logging.basicConfig(
        level       = args.log_level,
        stream      = sys.stderr,
        format      = "%(asctime)s %(levelname)5s %(module)s: %(message)s",
    )

    app = App()

    if args.load_state:
        with open(args.load_state, 'r') as file:
            app.load_state(json.load(file))
    else:
        app.scan(args.input)

    if args.save_state:
        with open(args.save_state, 'w') as file:
            json.dump(app.save_state(), file)
    else:
        app.process(args.input, args.output)

if __name__ == '__main__':
    main()
