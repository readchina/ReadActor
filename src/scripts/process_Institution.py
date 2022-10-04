import logging
import re
import sys
from datetime import date

import pandas as pd

from src.scripts.agent_table_processing import process_agent_tables
from src.scripts.authenticity_institution import get_QID_inst, sparql_inst

logger = logging.getLogger(__name__)


def modify_note_lastModified_lastModifiedBy(row, message, today):
    row["note"] += " " + message
    row["last_modified"] = today
    row["last_modified_by"] = "ReadActor"
    return row


def process_Inst(df, entity_type):
    # Check if (inst_id, inst_name) pairs are unique in user file
    id_name_pairs = []
    for pair in zip(df["inst_id"], df["inst_name"]):
        pair_str = "({})".format(",".join(pair))
        id_name_pairs.append(pair_str)
    if len(id_name_pairs) > len(set(id_name_pairs)):
        logger.error(
            "Error: (inst_id, inst_name) in your Institution table are not unique."
        )
        sys.exit()

    # Process the local Agent table
    (
        df_P_or_I_gh,
        agent_processed,
        all_agents_ids_gh,
        last_inst_id,
        all_wikidata_ids,
        _,
        _,
    ) = process_agent_tables(entity_type, "ReadAct", path=[])
    # Process local table row by row
    print("~~~~~~~~~~~~~")
    print("df:", df)
    print("~~~~~~~~~~~~~")
    print("df_P_or_I_gh:", df_P_or_I_gh)
    print("~~~~~~~~~~~~~")
    for index, row in df.iterrows():
        print("-------------\nFor row ", index + 2, " :")
        print(row.tolist())
        row, last_inst_id = check_each_row_Inst(
            index, row, df_P_or_I_gh, all_agents_ids_gh, last_inst_id, all_wikidata_ids
        )
        # make the format of start and end (year) valid
        row = format_year_Inst(row)
        df.loc[index] = row
    return df


def check_each_row_Inst(
    index, row, df_inst_gh, all_agents_ids_gh, last_inst_id, all_wikidata_ids
):
    today = date.today().strftime("%Y-%m-%d")
    if row["note"] == "skip" or row["note"] == "Skip":
        return row, last_inst_id
    else:
        if isinstance(row["inst_id"], str) and len(row["inst_id"]) > 0:
            if row["inst_id"] in all_agents_ids_gh:  # inst_id is in ReadAct
                return (
                    __compare_wikidata_ids_Inst(index, row, df_inst_gh, today),
                    last_inst_id,
                )
            else:  # inst_id not in ReadAct
                if (isinstance(row["wikidata_id"], str) is True) and (
                    len(row["wikidata_id"]) > 0
                ):  # user did input wikidata_id
                    if (
                        row["wikidata_id"] in all_wikidata_ids
                    ):  # the input wikidata_id in ReadAct
                        logger.error(
                            "For row %s , wikidata_id in ReadAct but inst_id not. Please check. "
                            % index
                        )
                        sys.exit()
                    else:  # the input wikidata_id not in ReadAct
                        inst_wiki = sparql_inst(
                            [row["wikidata_id"]]
                        )  # query by wikidata_id to get other properties

                        print("===============")
                        print("inst_wiki:")
                        print(inst_wiki)
                        print("===============")

                        l = [
                            inst_wiki["headquarters"],
                            inst_wiki["administrativeTerritorialEntity"],
                            inst_wiki["locationOfFormation"],
                            inst_wiki["inception"],
                        ]
                        l = [i for x in l for i in x]
                        print("===============")
                        print("l:")
                        print(l)
                        print("===============")
                        if not any(l):  # all items in above list are empty strings
                            message = (
                                "For row %s, the user input wikidata_id does not have relevant info. "
                                "Couldn't check. "
                            ) % index
                            logger.warning(message)
                            row = modify_note_lastModified_lastModifiedBy(
                                row, message, today
                            )
                        else:
                            row = __compare_place_and_start_for_Inst(
                                index, row, inst_wiki, today, ""
                            )
                else:  # user did NOT input wikidata_id
                    # TODO(QG): maybe it is better to modify get_QID_inst()
                    if row["inst_name"] is None or len(row["inst_name"]) == 0:
                        wikidata_id_from_query_Inst = None
                    else:
                        wikidata_id_from_query_Inst = get_QID_inst(
                            row["inst_name"]
                        )  # only return one value
                    if (
                        wikidata_id_from_query_Inst is None
                    ):  # query by name and return None
                        logger.info(
                            "For row %s : Query by name didn't find any wikidata item."
                            % index
                        )
                    else:  # query by name and find a wikidata_id
                        if (
                            wikidata_id_from_query_Inst[0]["id"] in all_wikidata_ids
                        ):  # found wikidata_id in ReadAct
                            logger.error(
                                "For row %s, The wikidata_id found by querying with institution name exists in "
                                'ReadAct already. If you are 100% sure that your input is correct, you can put "skip" '
                                'in "note". ' % index
                            )
                            sys.exit()
                        # The found wikidata_id is not in ReadAct, the next step is to compare the info given by
                        # Wikidata with start/end/place in Institution
                        else:
                            inst_wiki = sparql_inst(
                                [wikidata_id_from_query_Inst[0]["id"]]
                            )  # query by wikidata_id to get other properties
                            l = [
                                inst_wiki["headquarters"],
                                inst_wiki["administrativeTerritorialEntity"],
                                inst_wiki["locationOfFormation"],
                                inst_wiki["inception"],
                            ]
                            l = [i for x in l for i in x]
                            if not any(l):  # all items in above list are empty strings
                                message = (
                                    "For row %s, the user input wikidata_id does not have relevant info. "
                                    "Couldn't check. "
                                ) % index
                                logger.warning(message)
                                row = modify_note_lastModified_lastModifiedBy(
                                    row, message, today
                                )
                            else:
                                row = __compare_place_and_start_for_Inst(
                                    index,
                                    row,
                                    inst_wiki,
                                    today,
                                    wikidata_id_from_query_Inst[0]["id"],
                                )
        else:  # user did not input inst_id
            logger.error("Please input inst_id for row %s ." % index)
            sys.exit()
    return row, last_inst_id


def __compare_wikidata_ids_Inst(index, row, df_inst_gh, today):
    wikidata_id_usr = row["wikidata_id"]
    row_gh_index = df_inst_gh.index[
        (df_inst_gh["inst_id"] == row["inst_id"])
        & (df_inst_gh["language"] == row["language"])
    ].tolist()[0]
    row_GH = df_inst_gh.iloc[row_gh_index]
    wikidata_id_gh = row_GH["wikidata_id"]
    if wikidata_id_gh == wikidata_id_usr:  # two wikidata_id are the same
        res = __compare_two_rows_Inst(row, row_GH)
        if not res:  # one or multiple fields don't match
            return __overwrite_Inst(row, row_GH, index, today)
        logger.info("Row %s is checked. Pass " % index)  # all the fields match
        return row
    else:  # two wikidata_id are not the same
        error_msg = (
            "For row %s : `wikidata_id` does not match GitHub data. Please check. "
            % index
        )
        logger.error(error_msg)
        sys.exit()


def __compare_two_rows_Inst(row, row_gh):
    fields_to_be_compared = [
        "inst_name",
        "language",
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
    for i in fields_to_be_compared:
        if row_gh[i] == "" or row_gh[i] is None:
            continue
        elif (
            i in ["start", "end", "alt_start", "alt_end"]
            and re.search("[a-zA-Z]", str(row[i])) is None
        ):
            if "." in str(row[i]):
                row[i] = int(
                    float(row[i])
                )  # to remove ".0" part of potential floating-point number
            if int(row[i]) != int(row_gh[i]):
                return False
        else:
            if row[i] != row_gh[i]:
                return False
    return True


def __overwrite_Inst(row, row_gh, index, today):
    fields_to_be_overwritten = [
        "inst_name",
        "language",
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
    modified_fields = []
    for i in fields_to_be_overwritten:
        if row[i] != row_gh[i]:
            row[i] = row_gh[i]
            modified_fields.append(i)
    message = "In row %s , the following fields are overwritten: %s " % (
        index,
        ", ".join(map(str, modified_fields)),
    )
    row = modify_note_lastModified_lastModifiedBy(row, message, today)
    logger.info(message)
    return row


def __compare_place_and_start_for_Inst(
    index, row, inst_wiki, today, wikidata_id_by_query
):
    potential_place = (
        inst_wiki["headquarters"]
        + inst_wiki["administrativeTerritorialEntity"]
        + inst_wiki["locationOfFormation"]
    )
    inception = inst_wiki["inception"][0][0:4]
    print("====")
    print(potential_place)
    print(inception)
    if row["place"] in potential_place:
        if wikidata_id_by_query:
            row["wikidata_id"] = wikidata_id_by_query
            message1 = "Wikidata_id is acquired by querying with name. "
        else:
            message1 = ""
        if row["start"] != inception:
            row["start"] = inception
            message2 = (
                "Row %s has fields 'start' changed according to the wikidata_id given by user. "
                % index
            )
            logger.info(message2)
            row = modify_note_lastModified_lastModifiedBy(
                row, message1 + message2, today
            )
        else:
            message2 = "Row %s is checked. Pass. " % index
            logger.info(message1 + message2)
    elif row["start"] in inception:
        if wikidata_id_by_query:
            row["wikidata_id"] = wikidata_id_by_query
            message1 = "Wikidata_id is acquired by querying with name. "
        else:
            message1 = ""
        if row["place"] != max(set(potential_place)):
            row["place"] = max(set(potential_place))
            message2 = (
                "Row %s has fields 'place' changed according to the wikidata_id given by user. "
                % index
            )
            logger.info(message2)
            row = modify_note_lastModified_lastModifiedBy(
                row, message1 + message2, today
            )
        else:
            message2 = "Row %s is checked. Pass. " % index
            logger.info(message1 + message2)
    else:
        message = "Fields in row %s does not match wikidata properties. " % index
        logger.warning(message)
        row = modify_note_lastModified_lastModifiedBy(row, message, today)
    print("&&&&&&&&&&&&&&")
    print("row to be returned: ", row)
    return row


def format_year_Inst(row):
    for x in ["start", "end"]:
        if isinstance(row[x], float):
            row[x] = int(row[x])
        if "-" in str(row[x]):
            if isinstance(row[x][1:], float):
                row[x] = "-" + str(int(row[x][1:]))
            if isinstance(row[x][1:], int):
                year = str(int(row[x][1:]))
                if len(year) < 3:
                    pad = 3 - len(year)  # pad = 2,1
                    if pad == 1:
                        row["start"] = "-" + "0" + year
                    elif pad == 2:
                        row["start"] = "-" + "00" + year
        elif isinstance(row[x], int):
            year = str(int(row[x]))
            if len(year) < 4:
                pad = 4 - len(year)  # pad = 3,2,1
                if pad == 1:
                    row[x] = "0" + year
                elif pad == 2:
                    row[x] = "00" + year
                elif pad == 3:
                    row[x] = "000" + year
    return row


if __name__ == "__main__":
    pass
