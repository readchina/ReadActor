import datetime
import importlib
import logging
import sys
from importlib.metadata import version

import click
import pandas as pd

from src.scripts.authenticity_person import (
    order_name_by_language,
    sparql_by_name,
    sparql_with_Qid,
)
from src.scripts.authenticity_space import (
    compare_to_openstreetmap,
    geo_code_compare,
    get_coordinate_from_wikidata,
    get_QID,
)

# Creating an object
logger = logging.getLogger()
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s: - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])

# Todo(QG): this is the file on a branch which is not master. Should be replaced after 2.0.
PERSON_GITHUB = "https://raw.githubusercontent.com/readchina/ReadAct/add-wikidata_id/csv/data/Person.csv"
SPACE_GITHUB = "https://raw.githubusercontent.com/readchina/ReadAct/add-wikidata_id/csv/data/Space.csv"
Institution_GITHUB = (
    "https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Agent.csv"
)


# Todo(QG): to write a function to process table about space.
def check_each_row_Space(
    index, row, df_space_gh, space_ids_gh, last_space_id, wikidata_ids_GH
):
    today = datetime.date.today().strftime("%Y-%m-%d")
    if row["note"] == "skip" or row["note"] == "Skip":
        return row, last_space_id
    else:
        # Check if user has input space_id for this row
        if isinstance(row["space_id"], str) and len(row["space_id"]) > 0:
            # Check if the found space_id already in ReadAct
            if row["space_id"] in space_ids_gh:
                # Find the row with the same "sapce_id" in ReadAct
                # To compare two wikidata_id for two Space
                # Comtains two cases: both space_id and wikidata_id match
                # Or wikidata_id doesn't match
                return __compare_wikidata_ids(index, row, df_space_gh), last_space_id
            else:
                # Check if user input wikidata_id, if yes:
                if (isinstance(row["wikidata_id"], str) is True) and (
                    len(row["wikidata_id"]) > 0
                ):
                    # Check if this input wikidata_id in ReadAct, if yes then it is an error, because the
                    # input space_id is not in ReadAct
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
                        coordinate_pair_wikidata = get_coordinate_from_wikidata(
                            [row["wikidata_id"]]
                        )[
                            0
                        ]  # because we
                        # only queried one qname, the returned list should only have maximally one value
                        if len(coordinate_pair_wikidata) == 0:
                            logger.info(
                                "Row %s is checked. The user input wikidata_id does not have the coordinate location "
                                "property (P625). Could not check `lat` and `long`. Pass ",
                                index,
                            )
                            return row, last_space_id
                        else:
                            coordinate_pair_usr = [row["lat"], row["long"]]
                            # TODO(QG): the threshold here is +/- 0.1 between the user input coordinate and the coordinate returned by Wikidata query, should discuss if it makes sense.
                            if (
                                abs(
                                    float(coordinate_pair_wikidata[0])
                                    - float(coordinate_pair_usr[0])
                                )
                                <= 0.1
                            ) and (
                                abs(
                                    float(coordinate_pair_wikidata[1])
                                    - float(coordinate_pair_usr[1])
                                )
                                <= 0.1
                            ):
                                logger.info("Row %s is checked. Pass ", index)
                                return row, last_space_id
                            # If the coordinates entered by user and the coordinates conveyed by wikidata item are quite
                            # different (above the threhold)
                            else:
                                # Cannot set any thresholds here because the scope of the coordinate of a Space location
                                # can be as big as a country.
                                warning_msg = (
                                    "For row "
                                    + str(int(index))
                                    + " :"
                                    + "You'd better compare the coordinate you entered and the coordinate "
                                    "conveyed by the wikidata id you entered. By ReadActor "
                                )
                                logger.warning("warning_msg")
                                row["note"] = (
                                    "You'd better compare the coordinate you entered and the coordinate conveyed by the "
                                    "wikidata id you entered. By ReadActor "
                                )
                                return row, last_space_id
                # user input space_id not in ReadAct and user did not input wikidata_id
                else:
                    geo_code_dict = {}
                    # key: space_id
                    # value: space_name, space_type, lat, lang
                    geo_code_dict[row["space_id"]] = [
                        row["space_name"],
                        row["space_type"],
                        row["lat"],
                        row["long"],
                    ]
                    no_match_list = compare_to_openstreetmap(geo_code_dict)
                    geo_code_dict = {}
                    # TODO(QG): here, this subsection should be changed in the future to obtain the potential wikidata id.
                    if len(no_match_list) == 0:
                        # This space is authentic.
                        logger.info(
                            "Row %s is checked. The space is authentic according to the OpenStreetMap API query. "
                            "Pass ",
                            index,
                        )
                        return row, last_space_id
                    else:
                        # ToDo(QG): no_match_list[0][0] should be equal to row['space_name]. Should check.
                        wikidata_id_query = get_QID(
                            no_match_list[0][0]
                        )  # only return one value
                        if wikidata_id_query is None:
                            # Cannot find any related wikidata_id
                            # TODO(QG): should be united: for INFO level, do we write anything into the `note` field?
                            # And if modification only appears in `note`, should we modify `last_modified` and
                            # `last_modified_by`? These two questions should be gloablly checked.
                            logger.info(
                                "Row %s is checked. The name and coordinate didn't match according to OSM. And, "
                                "cannot find any wikidata_id by querying with the name. Pass ",
                                index,
                            )
                            return row, last_space_id
                        else:
                            # Query by name and find a wikidata_id
                            if wikidata_id_query in wikidata_ids_GH:
                                row["note"] = (
                                    "Error: Wikidata_id should be unique. `wikidata_id` already exists in GitHub data, "
                                    "but the space_id does not match. "
                                    "Please check. By ReadActor."
                                )
                                error_msg = (
                                    "For row "
                                    + str(int(index))
                                    + " :"
                                    + "`wikidata_id` already exists in "
                                    "GitHub data, but the `space_id` does not match. Please check."
                                )
                                logger.error(error_msg)
                                sys.exit()
                            # The found wikidata_id is not in ReadAct, the next step is to check its coordinate
                            else:
                                # ToDo(QG): for computing efficiency, here the `no_match_list` should be replaced
                                # with `wikidata_id_query` but the code should be modified accordingly
                                coordinate_from_query = get_coordinate_from_wikidata(
                                    wikidata_id_query
                                )
                                if (
                                    len(coordinate_from_query) == 0
                                ):  # found the wikidata item has no P625 feature
                                    logger.info(
                                        "Row %s is checked. The user input wikidata_id does not have the coordinate location "
                                        "property (P625). Could not check `lat` and `long`. Pass ",
                                        index,
                                    )
                                    return row, last_space_id
                                    # check if the coordinate entered by user gets a match with
                                    # the wikidata item which was queried by name
                                else:
                                    if (
                                        float(abs(float(coordinate_from_query[0][0])))
                                        - 0.9
                                        <= float(row["lat"])
                                        <= float(
                                            abs(float(coordinate_from_query[0][0]))
                                        )
                                        + 0.9
                                    ) and (
                                        float(abs(float(coordinate_from_query[0][1])))
                                        - 0.9
                                        <= float(row["long"])
                                        <= float(
                                            abs(float(coordinate_from_query[0][1]))
                                        )
                                        + 0.9
                                    ):
                                        # Update Wikidata_id
                                        row["wikidata_id"] = wikidata_id_query
                                        warning_msg = (
                                            "For row "
                                            + str(int(index))
                                            + " :"
                                            + "You should look space up in Wikidata and input the wikidata_id in your "
                                            "table in the future. By ReadActor "
                                        )
                                        logger.warning("warning_msg")
                                        row["note"] = (
                                            "You should look space up in Wikidata and input the wikidata_id in your table "
                                            "in the future. By ReadActor "
                                        )
                                        row["last_modified"] = today
                                        row["last_modified_by"] = "ReadActor"
                                        return row, last_space_id
                                    else:
                                        row["wikidata_id"] = wikidata_id_query
                                        row["lat"] = None
                                        row["long"] = None
                                        warning_msg = "For row "
                                        +str(int(index))
                                        +" :"
                                        "You should look this space up in Wikidata again. If it does not match this " "modification, you should retrieve the old data for this row and put `skip` in " "the column 'note'. By ReadActor "
                                        row["note"] = (
                                            "You should look this space up in Wikidata again. If it does not match this "
                                            "modification, you should retrieve the old data for this row and put `skip1 "
                                            "in the column 'note'. By ReadActor "
                                        )
                                        row["last_modified"] = today
                                        row["last_modified_by"] = "ReadActor"
                                        return row, last_space_id


def __compare_wikidata_ids(index, row, df_space_gh):
    wikidata_id_usr = row["wikidata_id"]
    row_gh_index = df_space_gh.index[
        (df_space_gh["person_id"] == row["person_id"])
        & (
            df_space_gh["language"]
            == row["space_type"]
            # & (df_space_gh["space_type"] == row["space_type"]
            # & (df_space_gh["lat"] == row["lat"]
            # & (df_space_gh["long"] == row["long"]
        )
    ].tolist()[0]
    row_GH = df_space_gh.iloc[row_gh_index]
    wikidata_id_gh = row_GH["wikidata_id"]
    if wikidata_id_gh == wikidata_id_usr:
        res = __compare_two_rows_Space(row, row_GH)
        if not res:
            return __overwrite_Space(row, row_GH, index)
        logger.info("Row %s is checked. Pass ", index)
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


def __compare_two_rows_Space(row, row_GH):
    fields_to_be_compared = ["space_name", "space_type", "lat", "long"]
    print("-------------------")
    print(type(row["lat"]))
    print(type(row_GH["lat"]))
    print("-------------------")
    for i in fields_to_be_compared:
        if row[i] != row_GH[i]:
            return False
    return True


def __overwrite_Space(row, row_gh, index):
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
    note_flag = False
    modified_fields = []
    for i in fields_to_be_overwritten:
        if row[i] != row_gh[i]:
            row[i] = row_gh[i]
            modified_fields.append(i)
            note_flag = True
    if note_flag:
        message = (
            "For row "
            + str(int(index))
            + "Fields -"
            + ", ".join(modified_fields)
            + "- is/are overwritten.  By ReadActor."
        )
        if isinstance(row["note"], str):
            row["note"] = row["note"] + " " + message
        else:
            row["note"] = message
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


def __compare_wikidata_ids_Space(index, row, df_person_GH):
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
        logger.info("Row %s is checked. Pass ", index)
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
    if named_entity_type == "Person":
        ids_GH = df["person_id"].tolist()
    elif named_entity_type == "Space":
        ids_GH = df["space_id"].tolist()
    elif named_entity_type == "Institution":
        ids_GH = df["institution_id"].tolist()
    ids_GH.sort()
    wikidata_ids_GH = df["wikidata_id"].tolist()
    return ids_GH[-1], ids_GH, wikidata_ids_GH


def check_each_row_Person(
    index, row, df_person_gh, person_ids_gh, last_person_id, wikidata_ids_GH
):
    today = datetime.date.today().strftime("%Y-%m-%d")
    if row["note"] == "skip" or row["note"] == "Skip":
        return row, last_person_id
    else:
        if isinstance(row["person_id"], str) and len(row["person_id"]) > 0:
            if row["person_id"] in person_ids_gh:
                return (
                    __compare_wikidata_ids_Space(index, row, df_person_gh),
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
                            logger.info("Row %s is checked. Pass ", index)
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
                                    "this row and put 'skip' in 'note'.",
                                    index,
                                )
                                row["last_modified"] = today
                                row["last_modified_by"] = "ReadActor"
                                return row, last_person_id
                            else:
                                logger.warning(
                                    "You should look the person in row %s up in Wikidata and input the "
                                    "`wikidata_id` in your table in the future.",
                                    index,
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
                                    "'Field `wikidata_id` in row %s of this table is updated.  By ReadActor.'",
                                    index,
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
                        logger.info("Row %s in this table is checked. Pass.", index)
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
                            "Field `person_id` in row %s of this table is updated.  By ReadActor.",
                            index,
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
                                "there is no matched wikidata_id. By ReadActor.",
                                index,
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
                                "`wikidata_id` in your table in the future.",
                                index,
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
@click.argument("path", default=".", type=str)
def cli(path, interactive, quiet, output, summary):
    if interactive:
        click.confirm("Do you want to update the table?", default=False, abort=True)

    if quiet:
        level = logging.ERROR
    else:
        level = logging.INFO

    log(level)

    df = pd.read_csv(path)  # index_col=0
    df = df.fillna("")  # Replace all the nan into empty string

    # Check which named entity should we process (Person, Space, Institution).
    if "Person" in path:
        named_entity_type = "Person"
        df_person_gh = pd.read_csv(PERSON_GITHUB)
        # Replace all the nan into empty string
        df_person_gh = df_person_gh.fillna("")
        check_gh(df_person_gh)
        last_person_id, person_ids_gh, wikidata_ids_GH = get_last_id(
            df_person_gh, named_entity_type
        )
        #  to be consistent
        for index, row in df.iterrows():
            print(
                "-------------\nFor row ", index + 2, " :"
            )  # Because the header line in Person.csv is already row 1
            # Todo(QG): adjust other row index output
            print(row.tolist())
            row, last_person_id = check_each_row_Person(
                index, row, df_person_gh, person_ids_gh, last_person_id, wikidata_ids_GH
            )
            df.loc[index] = row
    elif "Space" in path:
        named_entity_type = "Space"
        df_space_gh = pd.read_csv(SPACE_GITHUB)
        # Replace all the nan into empty string
        df_space_gh = df_space_gh.fillna("")
        check_gh(df_space_gh)
        last_space_id, space_ids_gh, wikidata_ids_GH = get_last_id(
            df_space_gh, named_entity_type
        )
        for index, row in df.iterrows():
            print(
                "-------------\nFor row ", index + 2, " :"
            )  # Because the header line in Person.csv is already row 1
            # Todo(QG): adjust other row index output
            print(row.tolist())
            row, last_space_id = check_each_row_Space(
                index, row, df_space_gh, space_ids_gh, last_space_id, wikidata_ids_GH
            )
            df.loc[index] = row

    elif "Institution" in path:
        named_entity_type = "Institution"
        pass

    if output:
        new_csv_path = path[:-4] + "_updated.csv"
        with open(new_csv_path, "w+") as f:
            f.write(df.to_csv(index=False))
    elif summary:
        print(df.to_csv(index=False))
    else:
        with open(path, "w") as f:
            f.write(df.to_csv(index=False))


if __name__ == "__main__":
    cli()
