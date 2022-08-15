import logging
import sys
from datetime import date

import pandas as pd

from src.scripts.agent_table_processing import process_agent_tables
from src.scripts.authenticity_institution import compare_inst, get_QID_inst, sparql_inst

logger = logging.getLogger(__name__)


def modify_note_lastModified_lastModifiedBy(row, message, today):
    row["note"] += " " + message
    row["last_modified"] = today
    row["last_modified_by"] = "ReadActor"
    return row


def process_Inst(df, entity_type):
    df_inst_new, inst_ids_gh, last_inst_id, wikidata_ids_GH = process_agent_tables(
        entity_type, "ReadAct", path=[]
    )
    for index, row in df.iterrows():
        print(
            "-------------\nFor row ", index, " :"
        )  # Todo(QG): adjust row index output
        print(row.tolist())
        row, last_person_id = check_each_row_Inst(
            index, row, df_inst_new, inst_ids_gh, last_inst_id, wikidata_ids_GH
        )
        df.loc[index] = row
    return df


def check_each_row_Inst(
    index, row, df_inst_new, inst_ids_gh, last_inst_id, wikidata_ids_GH
):
    today = date.today().strftime("%Y-%m-%d")
    if row["note"] == "skip" or row["note"] == "Skip":
        return row, last_inst_id
    else:
        if isinstance(row["inst_id"], str) and len(row["inst_id"]) > 0:
            if row["inst_id"] in inst_ids_gh:  # inst_id is in ReadAct
                return (
                    __compare_wikidata_ids_Inst(index, row, df_inst_new, today),
                    last_inst_id,
                )
            else:  # inst_id not in ReadAct
                if (isinstance(row["wikidata_id"], str) is True) and (
                    len(row["wikidata_id"]) > 0
                ):  # user did input wikidata_id
                    if (
                        row["wikidata_id"] in wikidata_ids_GH
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
                        l = [
                            inst_wiki["headquarters"],
                            inst_wiki["administrativeTerritorialEntity"],
                            inst_wiki["locationOfFormation"],
                            inst_wiki["locationOfFormation"],
                        ]
                        if not any(l):  # all items in above list are empty strings
                            message = (
                                "For row %s, the user input wikidata_id does not have relevant info. Couldn't check. "
                            ) % index
                            logger.warning(message)
                            row = modify_note_lastModified_lastModifiedBy(
                                row, message, today
                            )
                        else:
                            row = __compare_place_and_start_for_Inst(
                                index, row, inst_wiki, l, today, row["wikidata_id"]
                            )
                else:  # user did NOT input wikidata_id
                    wikidata_id_from_query_Inst = get_QID_inst(
                        row["name"]
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
                            wikidata_id_from_query_Inst in wikidata_ids_GH
                        ):  # found wikidata_id in ReadAct
                            logger.error(
                                'For row %s, The wikidata_id found by querying with institution name exists in ReadAct already. If you are 100% sure that your input is correct, you can put "skip" in "note". '
                                % index
                            )
                            sys.exit()
                        # The found wikidata_id is not in ReadAct, the next step is to check its coordinate
                        else:
                            inst_wiki = sparql_inst(
                                [wikidata_id_from_query_Inst]
                            )  # query by wikidata_id to get other properties
                            l = [
                                inst_wiki["headquarters"],
                                inst_wiki["administrativeTerritorialEntity"],
                                inst_wiki["locationOfFormation"],
                                inst_wiki["locationOfFormation"],
                            ]
                            if not any(l):  # all items in above list are empty strings
                                message = (
                                    "For row %s, the user input wikidata_id does not have relevant info. Couldn't check. "
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
                                    l,
                                    today,
                                    wikidata_id_from_query_Inst,
                                )
        else:  # user did not input inst_id
            logger.error("Please input inst_id for row %s ." % index)
            sys.exit()
    return row, last_inst_id


def __compare_wikidata_ids_Inst(index, row, df_inst_new, today):
    wikidata_id_usr = row["wikidata_id"]
    row_gh_index = df_inst_new.index[
        (df_inst_new["inst_id"] == row["inst_id"])
        & (df_inst_new["inst_name_lang"] == row["inst_name_lang"])
    ].tolist()[0]
    row_GH = df_inst_new.iloc[row_gh_index]
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
    for i in fields_to_be_compared[3:7]:
        if row_gh[i] == "":
            pass
        else:
            length = len(str(int((row_gh[i]))))
            if str(int(row[i]))[0 : length + 1] != str(
                int(row_gh[i])
            ):  # Only check maximally the first 4 digits if the
                # column is
                # about
                # year/time
                return False
    else:
        if row[i] != row_gh[i]:
            return False
    return True


def __overwrite_Inst(row, row_gh, index, today):
    fields_to_be_overwritten = [
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


def __compare_place_and_start_for_Inst(index, row, inst_wiki, l, today, wikidata_id):
    if (row["place"] in l) or (
        str(row["start"])[0:4] in str(inst_wiki["inception"])[0:4]
    ):
        if wikidata_id != row["wikidata_id"]:
            row["wikidata_id"] = wikidata_id
            message = (
                "For row %s, the wikidata_id can be queried. You should look institution up in Wikidata and write the wikidata_id in your table in the future. "
                % index
            )
            logger.warning(message)
            row = modify_note_lastModified_lastModifiedBy(row, message, today)
        else:
            logger.info("Row %s is checked. Pass. " % index)
    else:
        message = (
            "Row %s has no field which is matching any according wikidata properties. Please check. "
            % index
        )
        logger.warning(message)
        row = modify_note_lastModified_lastModifiedBy(row, message, today)
    return row


if __name__ == "__main__":
    pass
