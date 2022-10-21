import logging
import os.path
import sys

import pandas as pd

from src.scripts.authenticity_space import read_space_csv

PERSON_GITHUB = (
    "https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Person.csv"
)
SPACE_GITHUB = (
    "https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Space.csv"
)
INST_GITHUB = "https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Institution.csv"
AGENT_GITHUB = (
    "https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Agent.csv"
)

logger = logging.getLogger(__name__)


def search_wikidataID_in_table(row, agent_processed, agent_id):
    df_tmp = agent_processed[agent_processed["agent_id"] == row[agent_id]]
    # ToDo(QG): here should raise an error if the length > 1
    if len(df_tmp.index) == 1:
        if df_tmp.iloc[0]["wikidata_id"].startswith("Q"):
            row["wikidata_id"] = df_tmp.iloc[0]["wikidata_id"]
    return row


def addWikidataID_and_replaceSpace(
    df_PI_gh, agent_processed, agent_id, place_dict, entity_type
):
    for index, row in df_PI_gh.iterrows():
        if entity_type == "Institution":
            if row["place"] in place_dict:
                row["place"] = place_dict[row["place"]][0]
        elif entity_type == "Person":
            if row["place_of_birth"] in place_dict:
                row["place_of_birth"] = place_dict[row["place_of_birth"]][0]
        result = search_wikidataID_in_table(row, agent_processed, agent_id)
        df_PI_gh.loc[index] = result
    return df_PI_gh


def combine_agent_tables(df_agent_user, df_agent_gh, all_agents_ids_gh):
    """
    This function aims to use merging the Agent tables in ReadAct and in user's directory. If any agent_id in the
    user table already exists in ReadAct, delete according line(s) in the user table and then merge.
    :param df_agent_user: dataframe of the Agent.csv from user
    :param df_agent_gh: dataframe of the Agent.csv from ReadAct
    :param all_agents_ids_gh: all the agent_ids in the Agent.csv in ReadAct
    :return: a processed combined agent dataframe
    """
    df_agent_user_not_in_gh = df_agent_user.loc[
        ~df_agent_user["agent_id"].isin(all_agents_ids_gh)
    ]  # delete lines which
    # has agent_id in ReadAct, which is to say, for any agent_id already in ReadAct, use the ReadAct resource
    agent_cols = [
        "agent_id",
        "old_id",
        "agent_type",
        "wikidata_id",
        "fictionality",
        "language",
        "commentary",
        "note",
        "created",
        "created_by",
        "last_modified",
        "last_modified_by",
    ]
    df_processd = pd.merge(
        df_agent_gh, df_agent_user_not_in_gh, on=agent_cols, how="outer"
    )
    return df_processd


def preparation():
    place_dict = read_space_csv()
    df_agent_gh = pd.read_csv(AGENT_GITHUB).fillna("")  # Get Agent table from ReadAct
    all_agents_ids_gh = list(
        set(df_agent_gh["agent_id"].tolist())
    )  # Get all the unique agent_ids
    last_item_id_gh = all_agents_ids_gh[-1]  # Get the last agent_id in ReadAct
    return place_dict, df_agent_gh, all_agents_ids_gh, last_item_id_gh


def process_agent_tables(entity_type, user_or_ReadAct, path):
    place_dict, df_agent_gh, all_agents_ids_gh, last_item_id_gh = preparation()
    if entity_type == "Person":
        agent_id = "person_id"  # Set variables for later
        place_name = "place_of_birth"
    elif entity_type == "Institution":
        agent_id = "inst_id"
        place_name = "place"

    if user_or_ReadAct == "ReadAct":
        if entity_type == "Person":
            which_agent = PERSON_GITHUB
        elif entity_type == "Institution":
            which_agent = INST_GITHUB
        agent_processed = df_agent_gh
        dtype_dict = {}

    elif user_or_ReadAct == "user":
        if entity_type == "Person":
            which_agent = path[0]  # path of Person.csv in user's computer
            dtype_dict = {"birthyear": str, "deathyear": str}
        elif entity_type == "Institution":
            which_agent = path[0]  # path of Institution.csv in user's computer
            dtype_dict = {
                "start": str,
                "end": str,
                "alt_start": str,
                "alt_end": str,
            }
        df_agent_user = pd.read_csv(path[1]).fillna("")  # Get agent table

        # Check if agent_id are unique in user file
        if not pd.Series(df_agent_user["agent_id"]).is_unique:
            logger.error("Error: agent IDs in your Agent table are not unique.")
            sys.exit()

        agent_processed = combine_agent_tables(
            df_agent_user, df_agent_gh, all_agents_ids_gh
        )

    all_wikidata_ids = [x for x in agent_processed["wikidata_id"].tolist() if x]
    df_P_or_I_gh = pd.read_csv(which_agent, dtype=dtype_dict).fillna("")

    # Check if the place in user's Person/Institution table are all in ReadAct.
    # So that to get a processed space table to convert space IDs in P/I into space names.
    if all(place in place_dict for place in df_P_or_I_gh[place_name].tolist()):
        place_dict_combined = place_dict

        combined_two_space = False  # Unnecessary to check for potential local Space.csv
    else:  # place in user's table is not in ReadAct, check if any local space table
        space_path_user = path[1][:-9] + "Space.csv"
        if not os.path.isfile(space_path_user):
            logger.error(
                "There's place in your Institution/Person table without providing according space table. Please check."
            )
            sys.exit()
        else:
            combined_two_space = True  # Read local Space.csv
            place_dict_user = read_space_csv(space_path_user)
            # update place_dict by combining two Space table
            place_dict_combined = {
                **place_dict_user,
                **place_dict,
            }  # If any space_id in user's space table is also in ReadAct, take the value from ReadAct data

    df_P_or_I_gh["wikidata_id"] = ""  # add an empty wikidata_id column
    df_P_or_I_gh = addWikidataID_and_replaceSpace(
        df_P_or_I_gh, agent_processed, agent_id, place_dict_combined, entity_type
    )  # add wikidata_id information basing on ReadAct's Agent table

    return (
        df_P_or_I_gh,
        agent_processed,
        all_agents_ids_gh,
        last_item_id_gh,
        all_wikidata_ids,
        place_dict_combined,
        combined_two_space,
    )


if __name__ == "__main__":
    pass
