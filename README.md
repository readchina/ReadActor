# Wikidata Lookup

This repo contains Python scripts for verifying the authenticity for named entities in ReadAct.

The goal is to automately extract information about **person**, **space**, and **institutions** from **ReadAct**/data, then use Wikidata as an authenticity sources and compare features.

## The idea
#### Autenticity about Person
Two approaches are adopted: **lookup by name** or **query with Wikipedia links**.

For the former, names (include alt_name) are used to lookup with SPARQL query statements, and features like name, alt_name, gender or sex, birth year, death year, place of birth are used in a  weighting mechanism to choose the most likely candidate.

For the latter, using MediaWiki API, Q-identifiers are acquired based on Wikipedia links and then be used for SPARQL queyring.

#### Autenticity about Space

Two APIs (OpenStreetMap and Wikidata) are under using.

#### Autenticity about Institution

Wikidata to be the authenticity source as well as the other named entities.



## Working Environment
Python3.8
MacOS

## Requirement on CSV
For pre-defined column names, check the definition in [Data Dictionary](https://github.com/readchina/ReadAct/blob/master/csv/data_dictionary.csv).

## How to use the current command line tool

#### Requirements

- Python3.8 or higher version.

- Required dependencies. Can be installed by:

  ```
  pip install -r requirements.txt		
  ```

#### Person Lookup

- Example:

  ```
  python3.8 command_line_tool.py ../CSV/Person.csv
  ```
  or
  
  ```
  python3 command_line_tool.py ../CSV/Person.csv
  ```
  
  To read a user defined `Person.csv`, check the column names and to update it if necessary. Updated rows will be marked as modified by `SemBot`. 

  The updating can be done based on:

  	- `wikipedia link`
  	-  `Wikidata id`
  	-  `family name` and  `first name` (to be implemented)

  At the end, a `statistic` message will be printed out to tell the user how many entries are updated.



