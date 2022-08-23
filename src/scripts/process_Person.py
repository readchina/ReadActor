import logging
import sys
from datetime import date

import pandas as pd

from src.scripts.agent_table_processing import process_agent_tables
from src.scripts.authenticity_person import (
    order_name_by_language,
    sparql_by_name,
    sparql_with_Qid,
)

logger = logging.getLogger(__name__)


# def modify_note_lastModified_lastModifiedBy(row, message, today):
#     row["note"] += " " + message
#     row["last_modified"] = today
#     row["last_modified_by"] = "ReadActor"
#     return row


def process_Pers(df, entity_type):
    (
        df_person_new,
        person_ids_gh,
        last_person_id,
        wikidata_ids_GH,
    ) = process_agent_tables(entity_type, "ReadAct", path=[])
    for index, row in df.iterrows():
        print(
            "-------------\nFor row ", index, " :"
        )  # Todo(QG): adjust row index output
        print(row.tolist())
        row, last_person_id = check_each_row_Person(
            index, row, df_person_new, person_ids_gh, last_person_id, wikidata_ids_GH
        )
        # validate the format of start and end (year)
        row = validate_year_Person(row)
        df.loc[index] = row
    return df


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


def validate_year_Person(row):
    for x in ["birthyear", "deathyear"]:
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
