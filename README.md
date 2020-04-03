Mangle Jira XML

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
