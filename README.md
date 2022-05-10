# Wikidata Lookup

[![CI](https://github.com/readchina/WikidataLookup/actions/workflows/ci.yml/badge.svg)](https://github.com/readchina/WikidataLookup/actions/workflows/ci.yml)

This repo contains a Python package for verifying named entities in [ReadAct](https://github.com/readchina/ReadAct).

The goal is to automately extract data about **person**, **space**, and **institutions** from ReadAct's data tables, and to use [Wikidata](https://www.wikidata.org/wiki/Wikidata:Main_Page) for verification and to extract missing data points.

## Requierments

- Python: `>=3.8`

The tool is tested on macOS and linux. 

## Installation

You can install the tool using pip:

```bash 
pip install ReadChinaLookup
```

To check if the tool is working:

```bash
python -m src.scripts.command_line_tool --version
```

You should see:

```bash
version 1.0.0
```

## Development

We use [black](https://pypi.org/project/black/) and [isort](https://pypi.org/project/isort/) code formaters to format the code. To install them, run:

```bash
pip install black isort
```

To avoid CI rejecting your code contribution, you should run:

```bash
isort --profile black .
black .
```

before commiting code.

### From Source

From the root directory of this repository, run:

```bash
pip install -r requirements.txt
```

To run the testsuite:

```bash
python -m unittest discover -v
```

<!-- Something about which version of the programm and the first compatible ReadAct version here -->

## Version

Every PR will trigger the release of a new version. [Python semantic release](https://python-semantic-release.readthedocs.io/en/latest/) is used for version control. 

See  [Parsing of commit logs](https://python-semantic-release.readthedocs.io/en/latest/commit-log-parsing.html#commit-log-parsing) for commit conventions.

Here is an example:
<!-- ToDo: an exmaple to show how to tell the CI that this is the time of a new release -->
```
```


## Usage

The tables need to adhere to ReadAct's data model, you can check the definitions in its [Data Dictionary](https://github.com/readchina/ReadAct/blob/master/csv/data_dictionary.csv).

### Person Lookup

To install the package ReadChinaLookup 1.0.0:

```
pip install -i https://test.pypi.org/simple/ ReadChinaLookup==1.0.0
```

To read a user defined `Person.csv`, check the column names and to update it if necessary. Updated rows will be marked as modified by `SemBot`:

```bash
python3.8 -m src.scripts.command_line_tool src/CSV/Person.csv
```

The updating can be  based on:

- `wikipedia link`
- `Wikidata id`
- `family name` and  `first name` (to be implemented)

At the end, a `statistic` message will be printed out to tell the user how many entries are updated.

## What it does

### Agents

There are two approaches for checking person entries:

- **look up with name** or
- **look up with Wikipedia link**.

For the former, names (include alt_name) are used to look up with SPARQL query statements, and features like name, alt_name, gender or sex, birth year, death year, place of birth are used in a weighting mechanism to choose the most likely candidate.

For the latter, using [MediaWiki API](https://www.mediawiki.org/wiki/API:Main_page), Q-identifiers are acquired based on Wikipedia links and then queried via SPARQL.

### Space

Two APIs (OpenStreetMap and MediaWiki) are under using.

### Institution

To look up Institutions we use MediaWiki API.


## The time it takes
To run this tool on your own data, it takes from a few seconds to several hours according to the amount of data.

For example, using the data in [ReadAct](https://github.com/readchina/ReadAct), to run this tool on the [Person.csv](https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Person.csv) (data until 30.04.2022), it takes up to several hours. But if you only add and commit one or two new Person entries, or run this tool on your own CVS table which consists of one or two lines, it should take only a few seconds or one minute.

It is similar if you want to run scripts in this tool by yourselves, like `authenticity_person.py`, `authenticity_space.py`, `authenticity_institution.py`, it takes from a few minutes to several hours depending on the amount of data. 

For example, it takes a few minutes to run `authenticity_space.py` for [Space.csv](https://github.com/readchina/ReadAct/blob/master/csv/data/Space.csv) (data until 30.04.2022).

