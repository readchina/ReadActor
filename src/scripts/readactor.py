import importlib
import logging
import sys
from datetime import date
from importlib.metadata import version

import click
import pandas as pd

from src.scripts.agent_table_processing import (
    process_agent_tables,
    space_dict_for_agents,
)
from src.scripts.process_Institution import process_Inst
from src.scripts.process_Person import process_Pers
from src.scripts.process_Space import process_Spac

# Creating an object
logger = logging.getLogger()
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s: - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


# Todo(QG): this is the file on a branch which is not master. Should be replaced after 2.0.


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

    # process the dataframe (Person, Space, Institution).

    if "Space" in path:
        entity_type = "Space"
        df = pd.read_csv(path)  # index_col=0
        df = df.fillna("")  # Replace all the nan into empty string
        df = process_Spac(df)

    elif "Person" in path:
        entity_type = "Person"
        agent_user_path = path[:-10] + "Agent.csv"
        df, agent_processed, _, _, _ = process_agent_tables(
            entity_type, "user", path=[path, agent_user_path]
        )

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

        space_dict = space_dict_for_agents()
        df = df.replace({"place_of_birth": space_dict})

        for index, row in df.iterrows():
            if (
                row["place_of_birth"] is None
                or pd.isna(row["place_of_birth"])
                or len(str(row["place_of_birth"])) == 0
            ):
                continue
            elif row["place_of_birth"] not in space_dict.values():
                logger.error(
                    "The place %s is not in Space.csv. Please update Space.csv first."
                    % row["place_of_birth"]
                )
                sys.exit()

    elif "Institution" in path:
        entity_type = "Institution"
        agent_user_path = path[:-15] + "Agent.csv"
        df, agent_processed, _, _, _ = process_agent_tables(
            entity_type, "user", path=[path, agent_user_path]
        )
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

        space_dict = space_dict_for_agents()
        df = df.replace({"place": space_dict})
        for index, row in df.iterrows():
            if (
                row["place"] is None
                or pd.isna(row["place"])
                or len(str(row["place"])) == 0
            ):
                continue
            elif row["place"] not in space_dict.values():
                logger.error(
                    "The place %s is not in Space.csv. Please update Space.csv first."
                    % row["place"]
                )
                sys.exit()

    if entity_type == "Person":
        a_id = "person_id"
    elif entity_type == "Institution":
        a_id = "inst_id"
    today = date.today().strftime("%Y-%m-%d")

    # output to new tables
    if output:
        if entity_type == "Space":
            new_csv_path = path[:-4] + "_updated.csv"
            with open(new_csv_path, "w+") as f:
                f.write(df.to_csv(index=False))
        else:
            # write two updated tables to new files: agent and the other
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
            # print("\nSummary of Agent:")
            # print(df_agent.to_csv(index=False))

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


if __name__ == "__main__":
    cli()
