Mangle Jira XML

# Config

```yaml
# for --input/output-entities
entities:
  # edit osproperties
  drop_osproperty:
    - jira.properties/applinks.admin.*
    - jira.properties/applinks.global.application.ids
  rewrite_osproperty:
    jira.properties/jira.title: Jira (Test)

  # move User/Group from directoryId="10100" -> directoryId="1"
  # keeps the internal (1) Directory, and drops any other Directory
  rewrite_directories:
    10100: 1

  # keep users associated with projects (ProjectRoleActor atlassian-user-role-actor)
  keep_project_users: true

  # drops all unlisted users (combined with keep_project_users and rewrite_users)
  keep_users:
    - test@example.com

  # rewrite usernames (including all known references)
  rewrite_users:
    foo@exmple.net: foo@example.com

  # modify internal user attributes, e.g. set admin login password to use after importing
  modify_users:
    admin:
      credential: "{PKCS5S2}xxx" # print(passlib.hash.atlassian_pbkdf2_sha1.hash('...'))
      displayName: Jira Admin
      emailAddress: jira@example.com
      firstName: Jira
      lastName: Admin
      lowerEmailAddress: jira@example.com
      lowerDisplayName: jira admin
      lowerFirstName: jira
      lowerLastName: admin
      lowerUserName: admin

  # drops all unlisted groups
  keep_groups:
    - jira-administrators
    - jira-developers
    - jira-users

# for --input/output-activeobjects
activeobjects:
  clear_tables:
    - AO_2C4E5C_* # MAIL
```

# Example

### Invalid XML tokens

Jira XML exports may contain weird whitespace characters, which xml.etree rejects:

    tr -d $'\f\x03\x1E\x1F\x08\x10\x0B\x1B' < entities.xml > entities-v0.xml

### Pre-process entities

Generate `state.json` for output step

    ~/funidata/jira-manglr/jira-manglr.py --verbose --config ~/funidata/jira-manglr/config.yml --input-entities entities-v0.xml --save-state state-v1.json

### Process entities

Generate processed `entities.xml`

    ~/funidata/jira-manglr/jira-manglr.py --verbose --config ~/funidata/jira-manglr/config.yml --input-entities entities-v0.xml --load-state state-v1.json --output-entities entities-v1.xml

### Verify entities

    ~/funidata/jira-manglr/jira-manglr.py --verbose --config ~/funidata/jira-manglr/config.yml --input-entities entities-v1.xml --load-state state-v1.json --verify

XXX: currently gives false positives

### Process ActiveObjects

    ~/funidata/jira-manglr/jira-manglr.py --verbose --config ~/funidata/jira-manglr/config.yml --input-activeobjects activeobjects.xml --load-state state-v1.json --output-activeobjects activeobjects-v1.xml

### Create importable zip

    v=v1; mkdir -p import-$v && cp entities-$v.xml import-$v/entities.xml && cp activeobjects-$v.xml import-$v/activeobjects.xml && zip -jr import-$v.zip import-$v
