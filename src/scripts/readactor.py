import importlib
import logging
import os
import sys
from datetime import date
from importlib.metadata import version

import click
import numpy as np
import pandas as pd

from src.scripts.agent_table_processing import process_agent_tables
from src.scripts.authenticity_space import get_coordinate_from_wikidata, get_QID
from src.scripts.process_Institution import process_Inst
from src.scripts.process_Person import process_Pers
from src.scripts.process_Space import process_Spac

# Creating an object
logger = logging.getLogger()
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s: - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])
SPACE_GITHUB = (
    "https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Space.csv"
)


def combine_space_tables(df_space_user, df_space_gh, space_ids_gh):
    """
    This function aims to use merging the Space tables in ReadAct and in user's directory. If any space_id in the
    user table already exists in ReadAct, delete according line(s) in the user table and then merge.
    :param df_space_user: dataframe of the Space.csv from user
    :param df_space_gh: dataframe of the Space.csv from ReadAct
    :param space_ids_gh: all the space_ids in the Space.csv in ReadAct
    :return: a processed combined agent dataframe
    """
    df_space_user_not_in_gh = df_space_user.loc[
        ~df_space_user["space_id"].isin(space_ids_gh)
    ]  # delete lines which has agent_id in ReadAct: for any agent_id already in ReadAct, use the ReadAct resource
    space_cols = [
        "space_id",
        "old_id",
        "space_type",
        "space_name",
        "language",
        "lat",
        "long",
        "wikidata_id",
        "note",
        "created",
        "created_by",
        "last_modified",
        "last_modified_by",
    ]
    df_processd = pd.merge(
        df_space_gh, df_space_user_not_in_gh, on=space_cols, how="outer"
    )
    return df_processd


def create_new_space_entry(
    df, place_dict_combined, today, place_name, combined_two_space, entity_type, path
):
    # Read Space.csv from ReadAct
    df_space_gh = pd.read_csv(SPACE_GITHUB).fillna("")
    space_ids_gh = df_space_gh["space_id"].tolist()
    space_ids_gh.sort()
    last_space_id = space_ids_gh[-1]

    if (
        combined_two_space is True
    ):  # Already read local Space.csv. Must combine two space table.
        if entity_type == "Person":
            path_space_user = path[:-10] + "Space.csv"
        elif entity_type == "Institution":
            path_space_user = path[:-15] + "Space.csv"
        df_space_user = pd.read_csv(path_space_user)
        df_space_processed = combine_space_tables(
            df_space_user, df_space_gh, space_ids_gh
        )
    else:  # the place in user's P/I table are all in ReadAct, unnecessary to check for potential local Space.csv. Pay
        # attention that if ReadActor find any new space entity then to save a new Space table might overwrite any
        # potential local Space.csv
        df_space_processed = df_space_gh

    flag = False  # Flag to show if any new entries are added
    for index, row in df.iterrows():
        if (
            row[place_name] is None
            or pd.isna(row[place_name])
            or len(str(row[place_name])) == 0
        ):
            continue
        elif row[place_name] in place_dict_combined:
            continue
        else:
            # New space introduced by SPAQRL query
            # Append new entries
            # space_id,old_id,space_type,space_name,language,lat,long,wikidata_id,note,created,created_by,
            # last_modified,last_modified_by
            if int(last_space_id[2:]) > 9999:
                logger.error(
                    "Please inform the maintainer to update the schema of Space and modify scripts accordingly."
                )
                sys.exit()
            new_space_id = last_space_id[0:2] + str(int(last_space_id[2:]) + 1)
            new_space_name = row[place_name]
            new_language = "en"
            new_created = today
            new_created_by = "ReadActor"
            query_space = get_QID(new_space_name)
            if query_space is None:
                new_wikidata_id = ""
                new_lat = ""
                new_long = ""
                new_space_type = "L"  # L for locations (with NULL coordinates)
            else:
                coordinate = get_coordinate_from_wikidata(query_space["id"])[0]
                new_wikidata_id = query_space["id"]
                if len(coordinate) == 0:
                    new_lat = ""
                    new_long = ""
                elif len(coordinate) == 1:
                    new_long = coordinate[0]
                    new_lat = ""
                elif len(coordinate) == 2:
                    new_lat = coordinate[1]
                    new_long = coordinate[0]
                new_space_type = "PL"  # PL for place
            df_space_processed.loc[len(df_space_processed.index)] = [
                new_space_id,
                "",  # space_type
                new_space_type,
                new_space_name,
                new_language,
                new_lat,
                new_long,
                new_wikidata_id,
                "",  # note
                new_created,
                new_created_by,
                "",  # last_modified
                "",  # last_modified_by
            ]
            flag = True
            last_space_id = new_space_id
    return df_space_processed, flag


def space_dict_for_agents(df_space):
    space_dict = {}
    for index, row in df_space.iterrows():
        # consider the case that if there are identical space_names in csv file
        if row["space_name"] not in space_dict.keys():
            # key: 'space_name'
            # value: 'space_id'
            space_dict[row["space_name"]] = row["space_id"]
        else:
            logger.warning(
                "There are reduplicated space_name in ReadAct. Please notice the maintainer."
            )
            continue
    space_dict[""] = ""
    space_dict[None] = ""
    space_dict[float("nan")] = ""
    return space_dict


# eager
def print_version(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    s = "version " + importlib.metadata.version("ReadActor")
    click.echo(s)
    ctx.exit()


# eager
def print_log(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    with open("ReadActor.log", "r") as f:
        data = f.read()
        click.echo(data)
    ctx.exit()


def log(level):
    fh = logging.FileHandler("ReadActor.log")
    ch = logging.StreamHandler()

    logger.setLevel(logging.DEBUG)

    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)

    ch.setLevel(level)
    ch.setFormatter(formatter)

    logger.addHandler(ch)
    logger.addHandler(fh)


# Todo(QG): double check for the support of full URI.
@click.command(context_settings=CONTEXT_SETTINGS)
@click.option(
    "-v",
    "--version",
    is_flag=True,
    callback=print_version,
    expose_value=False,
    is_eager=True,
    help="Package version",
)
@click.option(
    "-d",
    "--debug",
    is_flag=True,
    callback=print_log,
    expose_value=False,
    is_eager=True,
    help="Print full log output to console",
)
@click.option(
    "-i",
    "--interactive",
    is_flag=True,
    default=False,
    is_eager=True,
    help="Prompt user for confirmation to continue",
)
@click.option(
    "-q",
    "--quiet",
    is_flag=True,
    help="Print no log output to console other then completion message and error level events",
)
@click.option(
    "-o",
    "--output",
    is_flag=True,
    help="Do not update input table, but create a new file at <path> instead",
)
@click.option(
    "-s",
    "--summary",
    is_flag=True,
    help="Do not update input table, but summarise results in console",
)
@click.option(
    "-S",
    "--space",
    is_flag=True,
    help="Process only places (places and locations)",
)
@click.option(
    "-A",
    "--agents",
    is_flag=True,
    help="Process only agents (persons and institutions)",
)
@click.argument("path", default=".", type=str)
def cli(path, interactive, quiet, output, summary, space, agents):
    if interactive:
        click.confirm("Do you want to update the table?", default=False, abort=True)

    if quiet:
        level = logging.ERROR
    else:
        level = logging.INFO

    log(level)

    if space:
        if "Space" not in path:
            print(
                "You want to process space/locations, but your input file path doesn't contain this kind of file."
            )
            sys.exit()
    if agents:
        if "Person" not in path and "Institution" not in path:
            print(
                "You want to process person/institution, but your input file path doesn't contain this kind of file."
            )
            sys.exit()

    today = date.today().strftime("%Y-%m-%d")

    # process the dataframe (Person, Space, Institution).
    if "Space" in path:
        entity_type = "Space"
        df = pd.read_csv(path)  # index_col=0
        df = df.fillna("")  # Replace all the nan into empty string
        df = process_Spac(df)

    elif "Person" in path:
        entity_type = "Person"
        place_name = "place_of_birth"
        agent_user_path = path[:-10] + "Agent.csv"
        (
            df,
            agent_processed,
            _,
            _,
            _,
            place_dict_combined,
            combined_two_space,
        ) = process_agent_tables(entity_type, "user", path=[path, agent_user_path])

        agent_processed_sorted = agent_processed.loc[
            agent_processed["agent_id"].str[2:].astype(int).sort_values().index
        ].reset_index(drop=True)

        df = process_Pers(df, entity_type)
        df_sorted = df.loc[
            df["person_id"].str[2:].astype(int).sort_values().index
        ].reset_index(
            drop=True
        )  # Sort Person.csv by person_id
        df = df_sorted

        # Get dictionary of space id and space name, replace the space id in Person table
        df_space_raw = pd.read_csv(SPACE_GITHUB)
        space_dict = space_dict_for_agents(df_space_raw)
        df = df.replace({place_name: space_dict})
        # If SPARQL query introduces any space name which is not in Space table, generate space id
        df_space_processed, flag_space_table = create_new_space_entry(
            df,
            place_dict_combined,
            today,
            place_name,
            combined_two_space,
            entity_type,
            path,
        )

    elif "Institution" in path:
        entity_type = "Institution"
        place_name = "place"
        agent_user_path = path[:-15] + "Agent.csv"
        (
            df,
            agent_processed,
            _,
            _,
            _,
            place_dict_combined,
            combined_two_space,
        ) = process_agent_tables(entity_type, "user", path=[path, agent_user_path])
        agent_processed_sorted = agent_processed.loc[
            agent_processed["agent_id"].str[2:].astype(int).sort_values().index
        ].reset_index(drop=True)
        df = process_Inst(df, entity_type)
        df_sorted = df.loc[
            df["inst_id"].str[2:].astype(int).sort_values().index
        ].reset_index(
            drop=True
        )  # Sort Institution.csv by inst_id
        df = df_sorted

        # Get dictionary of space id and space name, replace the space id in Institution table
        df_space_raw = pd.read_csv(SPACE_GITHUB)
        space_dict = space_dict_for_agents(df_space_raw)
        df = df.replace({place_name: space_dict})
        # If SPARQL query introduces any space name which is not in Space table, generate space id
        df_space_processed, flag_space_table = create_new_space_entry(
            df,
            place_dict_combined,
            today,
            place_name,
            combined_two_space,
            entity_type,
            path,
        )

    if entity_type == "Person":
        a_id = "person_id"
        # After adding new space entry, replace space name with new space ID
        space_dict = space_dict_for_agents(df_space_processed)
        df = df.replace({place_name: space_dict})
        df["narrative_age"] = pd.to_numeric(
            df["narrative_age"], errors="coerce"
        ).astype("Int64")
    elif entity_type == "Institution":
        a_id = "inst_id"
        # after adding new space entry, replace space name with new space ID
        space_dict = space_dict_for_agents(df_space_processed)
        df = df.replace({place_name: space_dict})

    # output to new tables
    if output:
        if entity_type == "Space":
            new_csv_path = path[:-4] + "_updated.csv"
            with open(new_csv_path, "w+") as f:
                f.write(df.to_csv(index=False))
        else:
            # write two or three updated tables to new files: Person/Institution, Agent (, Space)
            df_person_or_inst = df.copy(deep=True)  # a deep copy
            df_person_or_inst.drop("wikidata_id", inplace=True, axis=1)
            new_csv_path = path[:-4] + "_updated.csv"
            with open(new_csv_path, "w") as f3:
                f3.write(df_person_or_inst.to_csv(index=False))

            df_agent = agent_processed_sorted
            for i in range(len(df_agent["agent_id"])):
                for j in range(len(df[a_id])):
                    if df_agent["agent_id"][i] == df[a_id][j]:
                        if df_agent["wikidata_id"][i] != df["wikidata_id"][j]:
                            df_agent.loc[i, "wikidata_id"] = df["wikidata_id"][j]
                            df_agent.loc[i, "last_modified"] = today
                            df_agent.loc[i, "last_modified_by"] = "ReadActor"
                            logger.info("Wikidata id is updated. ")
            new_agent_user_path = agent_user_path[:-4] + "_updated.csv"
            with open(new_agent_user_path, "w") as f:
                f.write(df_agent.to_csv(index=False))

            if flag_space_table:
                if entity_type == "Person":
                    path_space = path[:-10] + "Space_updated.csv"
                elif entity_type == "Institution":
                    path_space = path[:-15] + "Space_updated.csv"
                with open(path_space, "w") as f:
                    f.write(df_space_processed.to_csv(index=False))

    # Print summary
    elif summary:
        if entity_type == "Space":
            print("\nSummary:\n", df.to_csv(index=False))
        elif entity_type == "Person" or entity_type == "Institution":
            # print two tables on screen: agent and the other
            df_person_or_inst = df.copy(deep=True)  # a deep copy
            df_person_or_inst.drop("wikidata_id", inplace=True, axis=1)

            print("\nSummary of Person/Institution:")
            print(df_person_or_inst.to_csv(index=False))

            df_agent = agent_processed_sorted
            for i in range(len(df_agent["agent_id"])):
                for j in range(len(df[a_id])):
                    if df_agent["agent_id"][i] == df[a_id][j]:
                        if df_agent["wikidata_id"][i] != df["wikidata_id"][j]:
                            df_agent.loc[i, "wikidata_id"] = df["wikidata_id"][j]
                            df_agent.loc[i, "last_modified"] = today
                            df_agent.loc[i, "last_modified_by"] = "ReadActor"
                            logger.info("Wikidata id is updated. ")
            print("\nSummary of Agent:")
            print(df_agent.to_csv(index=False))

            if flag_space_table:
                print("\nSummary of Space")
                print(df_space_processed.to_csv(index=False))

    else:
        if entity_type == "Space":
            with open(path, "w") as f1:
                f1.write(df.to_csv(index=False))
        else:
            # updated two tables: agent and the other
            df_person_or_inst = df.copy(deep=True)  # a deep copy
            df_person_or_inst.drop("wikidata_id", inplace=True, axis=1)
            with open(path, "w") as f3:
                f3.write(df_person_or_inst.to_csv(index=False))

            df_agent = agent_processed_sorted
            for i in range(len(df_agent["agent_id"])):
                for j in range(len(df[a_id])):
                    if df_agent["agent_id"][i] == df[a_id][j]:
                        if df_agent["wikidata_id"][i] != df["wikidata_id"][j]:
                            df_agent.loc[i, "wikidata_id"] = df["wikidata_id"][j]
                            df_agent.loc[i, "last_modified"] = today
                            df_agent.loc[i, "last_modified_by"] = "ReadActor"
                            logger.info("Wikidata id is updated. ")
            with open(agent_user_path, "w") as f:
                f.write(df_agent.to_csv(index=False))

            if flag_space_table:
                if entity_type == "Person":
                    path_space = path[:-10] + "Space.csv"
                elif entity_type == "Institution":
                    path_space = path[:-15] + "Space.csv"
                if os.path.isfile(path_space):
                    logger.warning("Your Space.csv at %s is overwritten. " % path_space)
                with open(path_space, "w") as f:
                    f.write(df_space_processed.to_csv(index=False))


if __name__ == "__main__":
    cli()
