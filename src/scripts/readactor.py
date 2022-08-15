import importlib
import logging
import sys
from datetime import date
from importlib.metadata import version

import click
import pandas as pd

from src.scripts.agent_table_processing import process_agent_tables
from src.scripts.authenticity_person import (
    order_name_by_language,
    sparql_by_name,
    sparql_with_Qid,
)
from src.scripts.authenticity_space import (
    compare_coordinates_with_threhold,
    get_coordinate_from_wikidata,
    get_QID,
    query_with_OSM,
)
from src.scripts.process_Institution import process_Inst

# Creating an object
logger = logging.getLogger()
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s: - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])

# Todo(QG): this is the file on a branch which is not master. Should be replaced after 2.0.
SPACE_GITHUB = "https://raw.githubusercontent.com/readchina/ReadAct/add-wikidata_id/csv/data/Space.csv"


def check_each_row_Space(
    index, row, df_space_gh, space_ids_gh, last_space_id, wikidata_ids_GH
):
    today = date.today().strftime("%Y-%m-%d")
    if (
        row["note"].strip() == "skip" or row["note"].strip() == "Skip"
    ):  # user wants to skip this line
        logger.info("User chooses to skip row %s ." % index)
    elif row["note"].strip() != "skip" or row["note"].strip() != "Skip":
        if (
            isinstance(row["space_id"], str) and len(row["space_id"]) > 0
        ):  # user did input space_id
            if row["space_id"] in space_ids_gh:  # space_id in ReadAct
                return (
                    __compare_wikidata_ids_Space(index, row, df_space_gh, today),
                    last_space_id,
                )
            else:  # space_id not in ReadAct
                if (isinstance(row["wikidata_id"], str) is True) and (
                    len(row["wikidata_id"]) > 0
                ):  # user did input wikidata_id
                    if (
                        row["wikidata_id"] in wikidata_ids_GH
                    ):  # the input wikidata_id in ReadAct
                        logger.error(
                            "For row %s , wikidata_id in ReadAct but space_id not. Please check."
                            % index
                        )
                        sys.exit()
                    else:  # the input wikidata_id not in ReadAct
                        coordinate_from_wikidata = get_coordinate_from_wikidata(
                            row["wikidata_id"]
                        )
                        if (
                            len(coordinate_from_wikidata) == 0
                        ):  # input wikidata_id has no P625
                            logger.info(
                                "In row %s, the user input wikidata_id does not have coordinate location property ("
                                "P625). Could not check 'lat' and 'long'. Pass " % index
                            )
                        else:  # input wikidata_id has P625 (coordinate location)
                            # TODO(QG): the threshold here is +/- 0.1, should discuss if it makes sense.
                            if compare_coordinates_with_threhold(
                                coordinate_from_wikidata, row["lat"], row["long"], 0.1
                            ):
                                logger.info("Row %s is checked. Pass." % index)
                            else:  # if the difference between coordinates bigger than threshold
                                warning_msg = (
                                    "In row %s ,you'd better compare the coordinate you entered and the one on "
                                    "Wikidata manually." % index
                                )
                                logger.warning("warning_msg")
                                row = modify_note_lastModified_lastModifiedBy(
                                    row, warning_msg, today
                                )
                else:  # user input space_id not in ReadAct and user did not input wikidata_id
                    res_OSM = query_with_OSM(
                        row["space_id"],
                        [row["space_name"], row["space_type"], row["lat"], row["long"]],
                    )
                    # TODO(QG): here, this subsection should be changed in the future to obtain the potential
                    #  wikidata id.
                    if (
                        res_OSM is None
                    ):  # space_name matches with geo coordinates basing on OSM query
                        logger.info(
                            "In row %s , space_name matches with geo coordinates according to OSM query."
                            % index
                        )
                    else:  # res_OSM: space_name, space_type, lat, lang, space_id
                        # ToDo(QG): no_match_list[0][0] should be equal to row['space_name]. Should check.
                        wikidata_id_from_query = get_QID(
                            res_OSM[0]
                        )  # only return one value
                        if wikidata_id_from_query is None:  # query returns None
                            logger.info(
                                "For row %s : check lat+long on OSM found the address which doesn't contain "
                                "space_name. Query by name didn't find any wikidata item."
                                % index
                            )
                        else:  # query by name and find a wikidata_id
                            if (
                                wikidata_id_from_query in wikidata_ids_GH
                            ):  # found wikidata_id in ReadAct
                                logger.error(
                                    "For row %s, found wikidata_id already in ReadAct while the space_id is not. If "
                                    "you are certain about your input, you can put the word 'skip' in 'note' to avoid "
                                    "this error message. " % index
                                )
                                sys.exit()
                            # The found wikidata_id is not in ReadAct, the next step is to check its coordinate
                            else:
                                # ToDo(QG): for computing efficiency, here the `no_match_list` should be replaced
                                #  with `wikidata_id_query` but the code should be modified accordingly
                                coordinate_from_query = get_coordinate_from_wikidata(
                                    wikidata_id_from_query
                                )
                                if (
                                    len(coordinate_from_query) == 0
                                ):  # has no geo location property
                                    logger.info(
                                        "In row %s, the user input wikidata_id does not have coordinate location "
                                        "property (P625). Could not check 'lat' and 'long'. "
                                        % index
                                    )
                                else:  # compare user input geo coordinate with the one from query
                                    # TODO(QG): the threshold here is +/- 0.1, should discuss if it makes sense.
                                    if compare_coordinates_with_threhold(
                                        coordinate_from_query,
                                        row["lat"],
                                        row["long"],
                                        0.1,
                                    ):  # consider a match
                                        row[
                                            "wikidata_id"
                                        ] = wikidata_id_from_query  # to update wikidata
                                        warning_msg = (
                                            "For cases like row %s , you'd better look the Space up in wikidata "
                                            "and input wikidata_id in your table."
                                            % index
                                        )
                                        row = modify_note_lastModified_lastModifiedBy(
                                            row, warning_msg, today
                                        )
                                        logger.warning(warning_msg)
                                    else:  # geo coordinates conflict. Probably the wikidata query is not accurate.
                                        # Or the query returns a geo location in the same area but different from the
                                        # geo location given by user since a geo area has a certain size.
                                        warning_msg = (
                                            "For row %s, by querying with space_name, the found wikidata item "
                                            "has a different geo location from your input. You are suggested to "
                                            "check "
                                            "it and put the word 'skip' in 'note' if you are confident about "
                                            "your input." % index
                                        )
                                        row = modify_note_lastModified_lastModifiedBy(
                                            row, warning_msg, today
                                        )
                                        logger.warning(warning_msg)
        else:  # user did not input space_id
            logger.error("Please input space_id for row %s ." % index)
            sys.exit()
    return row, last_space_id


def modify_note_lastModified_lastModifiedBy(row, message, today):
    row["note"] += " " + message
    row["last_modified"] = today
    row["last_modified_by"] = "ReadActor"
    return row


def __compare_wikidata_ids_Space(index, row, df_space_gh, today):
    """
    When user input wikidata_id and this wikidata_id already exists in ReadAct,
    compare the input row with the ReadAct row which has the same "sapce_id":
    1. compare the wikidata ids.
    2. if both wikidata ids are identical, then compare the rest fields. Otherwise, use ReadAct data to rewrite the
    user input.
    3. if two wikidata ids are not identical, report error for mismatch.
    """
    wikidata_id_usr = row["wikidata_id"]
    row_gh_index = df_space_gh.index[
        (df_space_gh["space_id"] == row["space_id"])
        & (df_space_gh["name_lang"] == row["name_lang"])
    ].tolist()[0]
    row_GH = df_space_gh.iloc[row_gh_index]
    wikidata_id_gh = row_GH["wikidata_id"]
    if (
        wikidata_id_gh is None or len(wikidata_id_gh) == 0
    ):  # if there is no Wikidata_id in ReadAct, pass.
        return row
    if wikidata_id_gh == wikidata_id_usr:
        res = __compare_two_rows_Space(row, row_GH)
        if not res:
            return __overwrite_Space(row, row_GH, index, today)
        logger.info("Row %s is checked. Pass." % index)
        return row
    else:
        logger.error(
            "In row %s , compare with the same space_id in ReadAct, you input a conflicting wikidata_id. "
            "Please check." % index
        )
        sys.exit()


def __compare_two_rows_Space(row, row_GH):
    fields_to_be_compared = ["space_name", "space_type", "lat", "long"]
    # print("-------------------")
    # print(type(row["lat"]))
    # print(type(row_GH["lat"]))
    # print("-------------------")
    for i in fields_to_be_compared:
        if row[i] != row_GH[i]:
            return False
    return True


def __overwrite_Space(row, row_gh, index, today):
    fields_to_be_overwritten = [
        "space_name",
        "space_type",
        "language",
        "lat",
        "long" "created",
        "created_by",
        "last_modified",
        "last_modified_by",
        "note",
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


def __compare_two_rows_Person(row, row_gh):
    """
    This function will be triggered when `person_id` and `wikidata_id` are the same.
    It will compare the rest fields of two rows from two dataframes seperately.

    :param row: one row from the user-uploaded Person.csv
    :param row_gh: the row in Person.csv on GitHub which has the same `person_id` and `wikidata_id` with the other
    parameter
    :return: True if all are matching, otherwise False
    """
    fields_to_be_compared = [
        "family_name",
        "first_name",
        "name_lang",
        "sex",
        "birthyear",
        "deathyear",
        "place_of_birth",
        "created",
        "created_by",
        "last_modified",
        "last_modified_by",
        "note",
    ]
    for i in fields_to_be_compared:
        if row[i] != row_gh[i]:
            return False
    return True


def __overwrite_Person(row, row_gh):
    """
    This function will overwrite all the fields except `person_id` and `wikidata_id`.
    :param row: one row from the user-uploaded Person.csv
    :param row_gh: the row in Person.csv on GitHub which has the same `person_id` and `wikidata_id` with the other
    parameter
    :return: row which is modified
    """
    fields_to_be_overwritten = [
        "family_name",
        "first_name",
        "name_lang",
        "sex",
        "birthyear",
        "deathyear",
        "place_of_birth",
        "created",
        "created_by",
        "last_modified",
        "last_modified_by",
        "note",
    ]
    note_flag = False
    modified_fields = []
    for i in fields_to_be_overwritten:
        if row[i] != row_gh[i]:
            row[i] = row_gh[i]
            modified_fields.append(i)
            note_flag = True
    if note_flag:
        message = (
            "Fields -"
            + ", ".join(modified_fields)
            + "- is/are overwritten.  By ReadActor."
        )
        if isinstance(row["note"], str):
            row["note"] = row["note"] + " " + message
        else:
            row["note"] = message
    logger.info(message)
    return row


def __compare_wikidata_ids_Person(index, row, df_person_GH):
    wikidata_id_usr = row["wikidata_id"]
    row_gh_index = df_person_GH.index[
        (df_person_GH["person_id"] == row["person_id"])
        & (df_person_GH["name_lang"] == row["name_lang"])
    ].tolist()[0]
    row_GH = df_person_GH.iloc[row_gh_index]
    wikidata_id_gh = row_GH["wikidata_id"]
    if wikidata_id_gh == wikidata_id_usr:
        res = __compare_two_rows_Person(row, row_GH)
        if not res:
            return __overwrite_Person(row, row_GH)
        logger.info("Row %s is checked. Pass " % index)
        return row
    else:
        row[
            "note"
        ] = "Error: `wikidata_id` is not matching with GitHub data. Please check. By ReadActor."
        error_msg = (
            "For row "
            + str(int(index))
            + " : `wikidata_id` does not match GitHub data. Please check. By "
            "ReadActor."
        )
        logger.error(error_msg)
        sys.exit()


def __check_person_id_size(row, last_id_in_gh):
    if int(last_id_in_gh[2:]) >= 9999:
        logger.warning(
            "It is better to update all person_id in the database. By ReadActor."
        )
        if isinstance(row["note"], str):
            row["note"] = (
                row["note"]
                + " Warning: It is better to update all person_id in the database. By ReadActor."
            )
        else:
            row[
                "note"
            ] = "Warning: It is better to update all person_id in the database. By ReadActor."


def check_gh(
    df,
):  # a function to check if Person.csv on GitHub has `wikidata_id` column
    if "wikidata_id" not in df.columns:
        error_msg = (
            "There is no `wikidata_id` column in the accordingly CSV table on GitHub. Please inform someone to check "
            "it. "
            ""
            "By ReadActor."
        )
        logger.error(error_msg)
        exit()


def get_last_id(df, named_entity_type):
    if named_entity_type == "Space":
        ids_GH = df["space_id"].tolist()
    elif named_entity_type == "Person":
        ids_GH = df["person_id"].tolist()
    elif named_entity_type == "Institution":
        ids_GH = df["inst_id"].tolist()
    ids_GH.sort()
    return ids_GH[-1], ids_GH


def check_each_row_Person(
    index, row, df_person_gh, person_ids_gh, last_person_id, wikidata_ids_GH
):
    today = date.today().strftime("%Y-%m-%d")
    if row["note"] == "skip" or row["note"] == "Skip":
        return row, last_person_id
    else:
        if isinstance(row["person_id"], str) and len(row["person_id"]) > 0:
            if row["person_id"] in person_ids_gh:
                return (
                    __compare_wikidata_ids_Person(index, row, df_person_gh),
                    last_person_id,
                )
            else:
                if (isinstance(row["wikidata_id"], str) is True) and (
                    len(row["wikidata_id"]) > 0
                ):
                    if row["wikidata_id"] in wikidata_ids_GH:
                        row["note"] = (
                            "Error: `wikidata_id` already exists in GitHub data but the person_id does not match. "
                            "Please check. By ReadActor."
                        )
                        error_msg = (
                            "For row "
                            + str(int(index))
                            + " :"
                            + "`wikidata_id` already exists in "
                            "GitHub data "
                            "but the `person_id` does not match. Please check."
                        )
                        logger.error(error_msg)
                        sys.exit()
                    else:
                        wikidata_id_usr = row["wikidata_id"]
                        person_dict = sparql_with_Qid(wikidata_id_usr)
                        note_flag = False
                        modified_fields = []
                        if (
                            "gender" in person_dict
                            and row["sex"] != person_dict["gender"]
                        ):
                            row["sex"] = person_dict["gender"]
                            modified_fields.append("sex")
                            note_flag = True
                        if (
                            "birthyear" in person_dict
                            and row["birthyear"] != person_dict["birthyear"]
                        ):
                            row["birthyear"] = person_dict["birthyear"]
                            modified_fields.append("birthyear")
                            note_flag = True
                        if (
                            "deathyear" in person_dict
                            and row["deathyear"] != person_dict["deathyear"]
                        ):
                            row["deathyear"] = person_dict["deathyear"]
                            modified_fields.append("deathyear")
                            note_flag = True
                        if (
                            "birthplace" in person_dict
                            and row["place_of_birth"] != person_dict["birthplace"]
                        ):
                            row["place_of_birth"] = person_dict["birthplace"]
                            modified_fields.append("place_of_birth")
                            note_flag = True
                        if note_flag:
                            message = (
                                "Fields -"
                                + ", ".join(modified_fields)
                                + "- is/are overwritten.  By ReadActor."
                            )
                            if isinstance(row["note"], str):
                                row["note"] = row["note"] + " " + message
                            else:
                                row["note"] = message
                            logger.info(message)
                            row["last_modified"] = today
                            row["last_modified_by"] = "ReadActor"
                            return row, last_person_id
                        else:
                            logger.info("Row %s is checked. Pass " % index)
                            return row, last_person_id
                else:  # user provided "person_id" but not "wikidata_id"
                    names = order_name_by_language(row)
                    person = sparql_by_name(names, row["name_lang"], 2)
                    if len(person) > 0:
                        wikidata_id_usr = next(iter(person))
                        if wikidata_id_usr in wikidata_ids_GH:
                            row["note"] = (
                                "Error: `wikidata_id` queried by family_name, first_name, name_lang already exists in "
                                ""
                                "ReadAct data, but your provided person_id does not match. Please check your data "
                                "carefully. If you are 100% sure that your input is correct, then it is likely that "
                                "this person has an identical name with a person in Wikidata database. Please put "
                                '"skip" in "note" column for this row and run this program again. By ReadActor.'
                            )
                            error_msg = (
                                "For row "
                                + str(int(index))
                                + " :"
                                + " `wikidata_id` queried by "
                                "family_name, "
                                "first_name, name_lang already exists in ReadAct data, but your provided "
                                "person_id does not match. Please check your data carefully. If you are 100% "
                                "sure that your input is correct, then it is likely that this person has an "
                                'identical name with a person in Wikidata database. Please put "skip" in "note" '
                                "column for this row and run this program again. By ReadActor."
                            )
                            logger.error(error_msg)
                            sys.exit()
                        else:
                            row["wikidata_id"] = wikidata_id_usr
                            person_dict = sparql_with_Qid(wikidata_id_usr)
                            note_flag = False
                            modified_fields = ["wikidata_id"]
                            if (
                                "gender" in person_dict
                                and row["sex"] != person_dict["gender"]
                            ):
                                row["sex"] = person_dict["gender"]
                                modified_fields.append("sex")
                                note_flag = True
                            if (
                                "birthyear" in person_dict
                                and row["birthyear"] != person_dict["birthyear"]
                            ):
                                row["birthyear"] = person_dict["birthyear"]
                                modified_fields.append("birthyear")
                                note_flag = True
                            if (
                                "deathyear" in person_dict
                                and row["deathyear"] != person_dict["deathyear"]
                            ):
                                row["deathyear"] = person_dict["deathyear"]
                                modified_fields.append("deathyear")
                                note_flag = True
                            if (
                                "birthplace" in person_dict
                                and row["place_of_birth"] != person_dict["birthplace"]
                            ):
                                row["place_of_birth"] = person_dict["birthplace"]
                                modified_fields.append("place_of_birth")
                                note_flag = True
                            if note_flag:
                                if isinstance(row["note"], str):
                                    row["note"] = (
                                        row["note"]
                                        + " Fields "
                                        + ", ".join(modified_fields)
                                        + " in this table is/are updated.  By ReadActor."
                                    )
                                else:
                                    row["note"] = (
                                        "Fields "
                                        + ", ".join(modified_fields)
                                        + " is/are updated.  By ReadActor."
                                    )
                                logger.warning(
                                    "You should look row %s up in Wikidata again. If it does not "
                                    "match with this modification, you should retrieve the old data for "
                                    "this row and put 'skip' in 'note'." % index
                                )
                                row["last_modified"] = today
                                row["last_modified_by"] = "ReadActor"
                                return row, last_person_id
                            else:
                                logger.warning(
                                    "You should look the person in row %s up in Wikidata and input the "
                                    "`wikidata_id` in your table in the future." % index
                                )
                                if isinstance(row["note"], str):
                                    row["note"] = (
                                        row["note"]
                                        + " Field `wikidata_id` in this table is updated.  By ReadActor."
                                    )
                                else:
                                    row[
                                        "note"
                                    ] = "Field `wikidata_id` in this table is updated.  By ReadActor."
                                logger.info(
                                    "'Field `wikidata_id` in row %s of this table is updated.  By ReadActor.'"
                                    % index
                                )
                                row["last_modified"] = today
                                row["last_modified_by"] = "ReadActor"
                                return row, last_person_id
                    else:
                        if isinstance(row["note"], str):
                            row["note"] = (
                                row["note"] + " No match in Wikidata.  By ReadActor."
                            )
                        else:
                            row["note"] = "No match in Wikidata.  By ReadActor."
                        # Todo: "note" is changed, does it count as modified?
                        logger.info("Row %s in this table is checked. Pass." % index)
                        return row, last_person_id
        else:  # No user provided `person_id`
            __check_person_id_size(row, last_person_id)
            row["person_id"] = last_person_id[0:2] + str(int(last_person_id[2:]) + 1)
            if (isinstance(row["wikidata_id"], str) is True) and (
                len(row["wikidata_id"]) > 0
            ):  # no person_id, but has wikidata_id
                if row["wikidata_id"] in wikidata_ids_GH:
                    row["note"] = (
                        "Error: this `wikidata_id` already exists in ReadAct. Please check carefully. If you are 100% "
                        ""
                        "sure that your input is correct, then it is likely that this person has an identical name "
                        'with a person in Wikidata database. Please put "skip" in "note" column for this row and run '
                        "this program again.  By ReadActor."
                    )
                    error_msg = (
                        "For row "
                        + str(index)
                        + " : this `wikidata_id` already exists in ReadAct. "
                        "Please check carefully. If you are 100% sure that your input is correct, then it is "
                        "likely that this person has an identical name with a person in Wikidata database. "
                        'Please put "skip" in "note" column for this row and run this program again.  By '
                        "ReadActor."
                    )
                    logger.error(error_msg)
                    sys.exit()
                else:
                    last_person_id = row["person_id"]
                    person_dict = sparql_with_Qid(row["wikidata_id"])
                    note_flag = False
                    modified_fields = ["person_id"]
                    if "gender" in person_dict and row["sex"] != person_dict["gender"]:
                        row["sex"] = person_dict["gender"]
                        modified_fields.append("sex")
                        note_flag = True
                    if (
                        "birthyear" in person_dict
                        and row["birthyear"] != person_dict["birthyear"]
                    ):
                        row["birthyear"] = person_dict["birthyear"]
                        modified_fields.append("birthyear")
                        note_flag = True
                    if (
                        "deathyear" in person_dict
                        and row["deathyear"] != person_dict["deathyear"]
                    ):
                        row["deathyear"] = person_dict["deathyear"]
                        modified_fields.append("deathyear")
                        note_flag = True
                    if (
                        "birthplace" in person_dict
                        and row["place_of_birth"] != person_dict["birthplace"]
                    ):
                        row["place_of_birth"] = person_dict["birthplace"]
                        modified_fields.append("place_of_birth")
                        note_flag = True
                    if note_flag:
                        message = (
                            "Fields "
                            + ", ".join(modified_fields)
                            + " is/are updated.  By ReadActor."
                        )
                        if isinstance(row["note"], str):
                            row["note"] = row["note"] + " " + message
                        else:
                            row["note"] = message
                        row["last_modified"] = today
                        row["last_modified_by"] = "ReadActor"
                        logger.info(message)
                        return row, last_person_id
                    else:
                        if isinstance(row["note"], str):
                            row["note"] = (
                                row["note"]
                                + " Field `person_id` in this table is updated.  By ReadActor."
                            )
                        else:
                            row[
                                "note"
                            ] = "Field `person_id` in this table is updated.  By ReadActor."
                        row["last_modified"] = today
                        row["last_modified_by"] = "ReadActor"
                        logger.info(
                            "Field `person_id` in row %s of this table is updated.  By ReadActor."
                            % index
                        )
                        return row, last_person_id
            else:  # no person_id, no wikidata_id
                names = order_name_by_language(row)
                person = sparql_by_name(names, row["name_lang"], 2)
                if len(person) > 0:
                    wikidata_id_usr = next(iter(person))
                    if wikidata_id_usr in wikidata_ids_GH:
                        row["note"] = (
                            "Error: `wikidata_id` queried by family_name, first_name, name_lang already exists in "
                            "ReadAct data, but your provided person_id does not match. Please check your data "
                            "carefully. If you are 100% sure that your input is correct, then it is likely that this "
                            'person has an identical name with a person in Wikidata database. Please put "skip" in '
                            '"note" column for this row and run this program again. By ReadActor.'
                        )
                        error_msg = (
                            "For row "
                            + str(int(index))
                            + " : `wikidata_id` queried by family_name, "
                            "first_name, name_lang already exists in ReadAct data, but your provided person_id "
                            "does not match. Please check your data carefully. If you are 100% sure that your "
                            "input is correct, then it is likely that this person has an identical name with a "
                            'person in Wikidata database. Please put "skip" in "note" column for this row and '
                            "run this program again. By ReadActor."
                        )
                        logger.error(error_msg)
                        sys.exit()
                    else:
                        last_person_id = row["person_id"]
                        row["wikidata_id"] = wikidata_id_usr
                        person_dict = sparql_with_Qid(wikidata_id_usr)
                        note_flag = False
                        modified_fields = ["person_id"]
                        if (
                            "gender" in person_dict
                            and row["sex"] != person_dict["gender"]
                        ):
                            modified_fields.append("sex")
                            note_flag = True
                        if (
                            "birthyear" in person_dict
                            and row["birthyear"] != person_dict["birthyear"]
                        ):
                            modified_fields.append("birthyear")
                            note_flag = True
                        if (
                            "deathyear" in person_dict
                            and row["deathyear"] != person_dict["deathyear"]
                        ):
                            modified_fields.append("deathyear")
                            note_flag = True
                        if (
                            "birthplace" in person_dict
                            and row["place_of_birth"] != person_dict["birthplace"]
                        ):
                            modified_fields.append("place_of_birth")
                            note_flag = True
                        if note_flag:
                            if isinstance(row["note"], str):
                                row["note"] = (
                                    row["note"]
                                    + " Fields "
                                    + ", ".join(modified_fields)
                                    + " in this table is/are updated.  By ReadActor."
                                )
                            else:
                                row["note"] = (
                                    "Fields "
                                    + ", ".join(modified_fields)
                                    + " is/are updated.  By ReadActor."
                                )
                            logger.warning(
                                "For row %s, you should input at least a person_id even if "
                                "there is no matched wikidata_id. By ReadActor." % index
                            )
                            row["last_modified"] = today
                            row["last_modified_by"] = "ReadActor"
                            return row, last_person_id
                        else:
                            if isinstance(row["note"], str):
                                row["note"] = (
                                    row["note"]
                                    + " Field `person_id` in this table is updated.  By ReadActor."
                                )
                            else:
                                row[
                                    "note"
                                ] = "Field `person_id` in this table is updated.  By ReadActor."
                            logger.warning(
                                "For row %s, you should look the person up in Wikidata and input the "
                                "`wikidata_id` in your table in the future." % index
                            )
                            row["last_modified"] = today
                            row["last_modified_by"] = "ReadActor"
                            return row, last_person_id
                else:
                    last_person_id = row["person_id"]
                    message = "Field `person_id` is updated. No match in Wikidata.  By SemReadActorBot."
                    if isinstance(row["note"], str):
                        row["note"] = row["note"] + " " + message
                    else:
                        row["note"] = message
                    row["last_modified"] = today
                    row["last_modified_by"] = "ReadActor"
                    logger.info(message)
                    return row, last_person_id


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

    # Check which named entity should we process (Person, Space, Institution).
    if "Person" in path:
        entity_type = "Person"
        agent_user_path = path[-10] + "/Agent.csv"
        df = process_agent_tables(entity_type, "user", path=[path, agent_user_path])[0]
        df = df.fillna("")  # Replace all the nan into empty string
        (
            df_person_new,
            person_ids_gh,
            last_person_id,
            wikidata_ids_GH,
        ) = process_agent_tables(entity_type)

        #  to be consistent
        for index, row in df.iterrows():
            print(
                "-------------\nFor row ", index, " :"
            )  # Because the header line in Person.csv is already row 1
            # Todo(QG): adjust other row index output
            print(row.tolist())
            row, last_person_id = check_each_row_Person(
                index,
                row,
                df_person_new,
                person_ids_gh,
                last_person_id,
                wikidata_ids_GH,
            )
            df.loc[index] = row

    elif "Space" in path:
        df = pd.read_csv(path)  # index_col=0
        df = df.fillna("")  # Replace all the nan into empty string
        entity_type = "Space"
        df_space_gh = pd.read_csv(SPACE_GITHUB)
        # Replace all the nan into empty string
        df_space_gh = df_space_gh.fillna("")
        check_gh(df_space_gh)
        space_ids_gh = df_space_gh["space_id"].tolist()
        space_ids_gh.sort()
        wikidata_ids_GH = df_space_gh["wikidata_id"].tolist()
        last_space_id = space_ids_gh[-1]
        for index, row in df.iterrows():
            print(
                "-------------\nFor row ", index, " :"
            )  # Because the header line in Person.csv is already row 1
            # Todo(QG): adjust other row index output
            print(row.tolist())
            row, last_space_id = check_each_row_Space(
                index, row, df_space_gh, space_ids_gh, last_space_id, wikidata_ids_GH
            )
            df.loc[index] = row

    elif "Institution" in path:
        entity_type = "Institution"
        agent_user_path = path[:-15] + "Agent.csv"
        df = process_agent_tables(entity_type, "user", path=[path, agent_user_path])[0]
        df = df.fillna("")  # Replace all the nan into empty string
        df = process_Inst(df, entity_type)

    print(
        "\n\n\n\ndf before check output/summary/else, but should already reflect if ReadActor did something: ",
        df,
    )

    if output:
        if entity_type == "space":
            new_csv_path = path[:-4] + "_updated.csv"
            with open(new_csv_path, "w+") as f:
                f.write(df.to_csv(index=False))
        else:
            # write two tables: agent and the other
            # ToDo(QG): here we should update two tables for any agents
            pass
    elif summary:
        print("\nelif summary:\n", df.to_csv(index=False))
    else:
        if entity_type == "space":
            with open(path, "w") as f:
                f.write(df.to_csv(index=False))
        else:
            # write two tables: agent and the other
            # ToDo(QG): here we should update two tables for any agents
            pass


if __name__ == "__main__":
    cli()
