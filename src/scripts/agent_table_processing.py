import pandas as pd
from src.scripts.authenticity_space import read_space_csv

PERSON_GITHUB = "https://raw.githubusercontent.com/readchina/ReadAct/add-wikidata_id/csv/data/Person.csv"
SPACE_GITHUB = "https://raw.githubusercontent.com/readchina/ReadAct/add-wikidata_id/csv/data/Space.csv"
INST_GITHUB = "https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Institution.csv"
AGENT_GITHUB = "https://raw.githubusercontent.com/readchina/ReadAct/add-wikidata_id/csv/data/Agent.csv"


def search_wikidataID_into_specific_table(row, df_agent_inst, agent_id):
    for i, r in df_agent_inst.iterrows():
        if row[agent_id] == r["agent_id"]:
            if r["wikidata_id"].startswith("Q"):
                row["wikidata_id"] = r["wikidata_id"]
            return row


def add_wikidataID__replaceSpaceForInst_to_specific_table(
    df_agent_gh, df_agent_inst, agent_id, place_dict, entity_type
):
    df_inst_new = df_agent_gh
    for index, row in df_agent_gh.iterrows():
        if entity_type == "Institution":
            if row["place"] in place_dict:
                row["place"] = place_dict[row["place"]][0]
        result = search_wikidataID_into_specific_table(row, df_agent_inst, agent_id)
        if result is not None:
            row = result
        df_inst_new.loc[index] = row
    return df_inst_new


def process_agent_tables(entity_type, user_or_ReadAct, path=[]):
    if entity_type == "Person":
        if user_or_ReadAct == "ReadAct":
            which_agent = PERSON_GITHUB
        else:
            which_agent = path[0]
        agent_id = "person_id"
        cols = [
            "person_id",
            "family_name",
            "first_name",
            "name_lang",
            "sex",
            "rustication",
            "birthyear",
            "deathyear",
            "alt_name",
            "place_of_birth",
            "social_position",
            "neibu_access",
            "wikidata_id",
            "source_1",
            "page_1",
            "source_2",
            "page_2",
            "note",
            "created",
            "created_by",
            "last_modified",
            "last_modified_by",
        ]
    elif entity_type == "Institution":
        place_dict = read_space_csv()
        if user_or_ReadAct == "ReadAct":
            which_agent = INST_GITHUB
        else:
            which_agent = path[0]
        agent_id = "inst_id"
        cols = [
            "inst_id",
            "inst_name",
            "inst_name_lang",
            "place",
            "start",
            "end",
            "alt_start",
            "alt_end",
            "inst_alt_name",
            "wikidata_id",
            "note",
            "source",
            "page",
            "created",
            "created_by",
            "last_modified",
            "last_modified_by",
        ]

    df_agent_gh = pd.read_csv(which_agent).fillna("")  # Get Institution/Person table
    agent_ids_gh = df_agent_gh[
        agent_id
    ].tolist()  # Get all the institution/person's agent_id
    agent_ids_gh.sort()  # Order them
    last_item_id = agent_ids_gh[-1]  # Get the last institution/person's agent_id

    if user_or_ReadAct == "ReadAct":
        df_agent = pd.read_csv(AGENT_GITHUB).fillna("")  # Get agent table
    else:
        df_agent = pd.read_csv(path[1]).fillna("")  # Get agent table
    df_agent_part = df_agent.loc[
        df_agent["agent_id"].isin(agent_ids_gh)
    ]  # Get part of agent table
    df_agent_gh[
        "wikidata_id"
    ] = ""  # Add an empty wikidata_id column to institution/person table
    df_agent_new = add_wikidataID__replaceSpaceForInst_to_specific_table(
        df_agent_gh, df_agent_part, agent_id, place_dict, entity_type
    )  # combine wikidata_id infomation from agent
    # table with institution/person table to get a new df
    df_agent_new = df_agent_new[cols]  # Reorder the table
    wikidata_ids_GH = df_agent_new["wikidata_id"].tolist()
    wikidata_ids_GH = [x for x in wikidata_ids_GH if x]
    return df_agent_new, agent_ids_gh, last_item_id, wikidata_ids_GH


if __name__ == "__main__":
    pass
    # entity_type = "Institution"
    # df_inst_new, inst_ids_gh, last_inst_id, wikidata_ids_GH = process_agent_tables(entity_type)
    # print(df_inst_new.head(5).to_string())
