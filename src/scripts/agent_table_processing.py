import pandas as pd

from src.scripts.authenticity_space import read_space_csv

PERSON_GITHUB = "https://raw.githubusercontent.com/readchina/ReadAct/2.0-RC-patch/csv/data/Person.csv"
SPACE_GITHUB = "https://raw.githubusercontent.com/readchina/ReadAct/2.0-RC-patch/csv/data/Space.csv"
INST_GITHUB = "https://raw.githubusercontent.com/readchina/ReadAct/2.0-RC-patch/csv/data/Institution.csv"
AGENT_GITHUB = "https://raw.githubusercontent.com/readchina/ReadAct/2.0-RC-patch/csv/data/Agent.csv"


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


def combine_agent_tables(df_agent_user, df_agent_gh, agent_ids_gh):
    """
    This function aims to use merging the Agent tables in ReadAct and in user's directory. If any agent_id in the
    user table already exists in ReadAct, delete according line(s) in the user table and then merge.
    :param df_agent_user: dataframe of the Agent.csv from user
    :param df_agent_gh: dataframe of the Agent.csv from ReadAct
    :param agent_ids_gh: all the agent_ids in the Agent.csv in ReadAct
    :return: a processed combined agent dataframe
    """
    df_agent_user_not_in_gh = df_agent_user.loc[
        ~df_agent_user["agent_id"].isin(agent_ids_gh)
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


def process_agent_tables(entity_type, user_or_ReadAct, path):
    place_dict = read_space_csv()

    df_agent_gh = pd.read_csv(AGENT_GITHUB).fillna("")  # Get Agent table from ReadAct
    agent_ids_gh = list(
        set(df_agent_gh["agent_id"].tolist())
    )  # Get all the unique agent_ids
    last_item_id_gh = agent_ids_gh[-1]  # Get the last agent_id in ReadAct
    if entity_type == "Person":
        agent_id = "person_id"  # Set variables for later
    elif entity_type == "Institution":
        agent_id = "inst_id"

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
            }  # here we assume all the years in Institution are type int
        df_agent_user = pd.read_csv(path[1]).fillna("")  # Get agent table
        agent_processed = combine_agent_tables(df_agent_user, df_agent_gh, agent_ids_gh)
        print("\n@@@@@@@@@@@agent_processed@@@@@@@@@@@@@@@")
        print(agent_processed)

    all_wikidata_ids = [x for x in agent_processed["wikidata_id"].tolist() if x]
    df_PI = pd.read_csv(which_agent, dtype=dtype_dict).fillna("")
    df_PI["wikidata_id"] = ""  # add an empty wikidata_id column
    df_PI = addWikidataID_and_replaceSpace(
        df_PI, agent_processed, agent_id, place_dict, entity_type
    )  # add wikidata_id information from ReadAct's Agent table

    return df_PI, agent_processed, agent_ids_gh, last_item_id_gh, all_wikidata_ids


def space_dict_for_agents(
    space_url=SPACE_GITHUB,
):
    df = pd.read_csv(space_url)
    space_dict = {}
    for index, row in df.iterrows():
        # consider the case that if there are identical space_names in csv file
        if row["space_name"] not in space_dict.keys():
            # key: 'space_name'
            # value: 'space_id'
            space_dict[row["space_name"]] = row["space_id"]
        else:
            # ToDo(QG): Is it on purpose that we have reduplicated space_name ?
            continue
    space_dict[""] = ""
    space_dict[None] = ""
    space_dict[float("nan")] = ""
    return space_dict


if __name__ == "__main__":
    pass
