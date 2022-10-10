# ReadActor

[![CI](https://github.com/readchina/WikidataLookup/actions/workflows/ci.yml/badge.svg)](https://github.com/readchina/WikidataLookup/actions/workflows/ci.yml)

This repo contains a Python package for verifying the authenticity of named entities based on [ReadAct](https://github.com/readchina/ReadAct).

The goal is to automately extract data about **person**, **space**, and **institutions** from ReadAct database, and to use [Wikidata](https://www.wikidata.org/wiki/Wikidata:Main_Page) to verify existing data and update missing data.

The two ways of using it are either install it locally and run the following scripts, or push your updates to ReadAct to GitHub where this package will be ran in CI automatically.

## Requierments

The tool is tested on macOS and linux. 

Python: `>=3.8`

You might need to upgrade pip by using the command:

`python -m pip install --upgrade pip`


## Installation

You can install the tool using pip:

```bash 
pip install ReadActor pandas
```

To check if the tool is working:

```bash
readactor --version
```

You should see version number similar to:

```bash
version 1.0.1
```

### From Source

From the root directory of this repository, run:

```bash
pip install -r requirements.txt
```

To run the testsuite:

```bash
python -m unittest discover -v
```

ReadActor works with ReadAct version 2.0.0 and later. 

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

## Releases

Currently, the version number is updated manually, in `setup.py`

```python
setup(
    name="ReadActor",
    version="2.0.2-alpha",
)
```

Release are automated on CI triggered by push events to the `master` branch. We follow semantic versioning, so during normal development the version number should be a pre-release number ending in `-alpha`, `-beta` etc. 

To create a release simply commit and push a change that uses a full version number e.g. `2.0.2` to the `master` branch. 

<!--Every PR will trigger the release of a new version. [Python semantic release](https://python-semantic-release.readthedocs.io/en/latest/) is used for version control. 

See  [Parsing of commit logs](https://python-semantic-release.readthedocs.io/en/latest/commit-log-parsing.html#commit-log-parsing) for commit conventions.

Here is an example: -->
<!-- ToDo: an exmaple to show how to tell the CI that this is the time of a new release -->


## Strategy
### Person look up
There are two approaches for checking person entries:

- **look up with name** or
- **look up with Wikipedia link**.

For the former, names (include alt_name) are used to look up with SPARQL query statements, and features like name, alt_name, gender or sex, birth year, death year, place of birth are used in a weighting mechanism to choose the most likely candidate.

For the latter, using [MediaWiki API](https://www.mediawiki.org/wiki/API:Main_page), Q-identifiers are acquired based on Wikipedia links and then queried via SPARQL.

Longest mact are taken as the final result.

### Institution lookup
To look up Institutions we use MediaWiki API.

### Space lookup
Two APIs (OpenStreetMap and MediaWiki) are under using.


## Usage

The tables need to adhere to ReadAct's data model, you can check the definitions in its [Data Dictionary](https://github.com/readchina/ReadAct/blob/master/csv/data_dictionary.csv) and you can check the data schema in the [schema folder](https://github.com/readchina/ReadAct/tree/master/csv/schema).


<!-- ToDo: to update on pypi with the new version -->
To install the package ReadActor 1.0.0:

```
pip install -i https://test.pypi.org/simple/ ReadActor==1.0.0
```

It is suggested to run it in a virtual environment with the following codes:

```bash
# to create the virtuanl environment for the first time
virtualenv venv

# to activate existed virtual environment (for OS X)
. venv/bin/activate

# to deactivate the virtual environment
deactivate
```

Command `readactor --help` or `readactor -h` will show you different options.
For example:

```bash
"Usage: cli [OPTIONS] [PATH]",
            "",
            "Options:",
            "  -v, --version      Package version",
            "  -d, --debug        Print full log output to console",
            "  -i, --interactive  Prompt user for confirmation to continue",
            "  -q, --quiet        Print no log output to console other then completion",
            "                     message and error level events",
            "  -o, --output       Do not update input table, but create a new file at <path>",
            "                     instead",
            "  -s, --summary      Do not update input table, but summarise results in console",
            "  -S, --space        Process only places (places and locations)",
            "  -A, --agents       Process only agents (persons and institutions)",
            "  -h, --help         Show this message and exit.",
```

The basic usage is to use this tool to verify the authenticity of Person/Institution/Space entities by comparing your data with ReadAct and query on Wikidata.
The most command style is:

```bash
# readactor [path]
readactor src/CSV/Person.csv
```

If you are new to this tool, please also read the following relevant details.


## Details
### Basic rules:
1. To process entities like Person/Institution/Space, you are expected to pass one and only one path in your command, for example, `readactor myProject/Person.csv`.
2. In the directory which you stores either one or some of Person/Institution/Space tables, the file names, if the file exists, must be exactly `Person.csv` or `Institution.csv` or `Space.csv` or `Agent.csv` (pay attention to the upper case letter).
3. When there are new Space entities in your Person/Institution table which has no corresponding entry in ReadAct, you are expected to include the new entities in your local `Space.csv` in the same directory as the Person/Institution table.
4. For new Space entities which are introduced by the tool itself, ReadActor will take care of it.
5. Your local `Space.csv` might be overwritten in certain condition (no new Space entity appeared in your Person/Institution table). It is always a good idea to have a backup of the CSV files that you are going to process.


### Agents

<!-- -->

A local Agent table is necessary when you want to process Person/Institution table. Because the `wikidata_id` for Person/Institution will only be stored in the Agent table.

You should at least fill the `agent_id` column in your Agent table. For each Person/Institution entry in the file you want to process, there should be one corresponding line in your `Agent.csv`.

### Person

Please make sure the "Agent.csv" is in the same directory as your "Person.csv".

### Institution

Please make sure the "Agent.csv" is in the same directory as your "Institution.csv".

### Space





## The time it takes
To run this tool on your own data, it takes from a few seconds to several hours according to the amount of data.

For example, using the data in [ReadAct](https://github.com/readchina/ReadAct), to run this tool on the [Person.csv](https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Person.csv) (data until 20.09.2022), it takes up to several hours. But if you only add and commit one or two new Person entries, or run this tool on your own CVS table which consists of a few lines, it should take only a few seconds or several minute.

It is similar if you want to run scripts in this tool by yourselves, like `authenticity_person.py`, `authenticity_space.py`, `authenticity_institution.py`, it takes from a few minutes to several hours depending on the amount of data. 

For example, it takes a few minutes to run `authenticity_space.py` for [Space.csv](https://github.com/readchina/ReadAct/blob/master/csv/data/Space.csv) (data until 30.04.2022).

