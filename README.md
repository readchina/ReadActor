# Wikidata Lookup

This repo contains Python scripts for verifying the authenticity for named entities in ReadAct.

The goal is to automately extract information about person, space, and institutions from ReadAct/data, then locate the named entity in Wikidata and compare features.

## The idea
#### Autenticity about Person
Two approaches are adopted: **lookup by name** or **query with Wikipedia links**.

For the former, names (include alt_name) are used to lookup with SPARQL query statements, and features like name, alt_name, gender or sex, birth year, death year, place of birth are used in a  weighting mechanism to choose the most likely candidate.

For the latter, using MediaWiki API, Q-identifiers are acquired based on Wikipedia links and then be used for SPARQL queyring.

## How to use

#### Person Lookup
- Major syntax:

  ```
  python
  ```

- Parameters



