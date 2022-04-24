import argparse
import datetime
import logging
import sys

import pandas as pd
from src.scripts.authenticity_person import (
    get_Qid_from_wikipedia_url,
    order_name_by_language,
    sparql_by_name,
    sparql_with_Qid,
)

# Create and configure logger
logging.basicConfig(
    filename="sembot.log", format="%(asctime)s %(message)s", filemode="w"
)

# Creating an object
logger = logging.getLogger()

DATA_DICTIONARY_GITHUB = (
    "https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data_dictionary.csv"
)
PERSON_CSV_GITHUB = (
    "https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Person.csv"
)
EXPECTED_COL = [
    "person_id",
    "family_name",
    "first_name",
    "name_lang",
    "sex",
    "birthyear",
    "deathyear",
    "place_of_birth",
    "wikidata_id",
    "created",
    "created_by",
    "last_modified",
    "last_modified_by",
    "note",
]
MINIMAL_COL = ["family_name", "first_name", "name_lang"]
FIELDS_OF_WIKIDATA = [
    "sex",
    "birthyear",
    "deathyear",
    "place_of_birth",
]  # gender, birthplace

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s: - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)


def validate(path):
    valid = False
    df = pd.read_csv(path)  # index_col=0
    df = df.fillna("")  # Replace all the nan into empty string

    if not set(MINIMAL_COL).issubset(df.columns.tolist()):
        print(set(MINIMAL_COL) - set(df.columns.tolist()))
        logger.error("Your file is missing the following mandatory column(s):")
    elif not set(EXPECTED_COL).issubset(set(df.columns.tolist())):
        missing_columns = set(EXPECTED_COL) - set(df.columns.tolist())
        valid = True
        print(
            "There are 15 expected columns in Person.csv.\nYour file has missing column(s):"
        )
        print(list(missing_columns))
        for i in missing_columns:
            df[i] = ""
        df = df[
            [
                "person_id",
                "family_name",
                "first_name",
                "name_lang",
                "sex",
                "birthyear",
                "deathyear",
                "place_of_birth",
                "wikidata_id",
                "created",
                "created_by",
                "last_modified",
                "last_modified_by",
                "note",
            ]
        ]
        print(
            "All missing columns are inserted to your csv table now.\nNote that columns outside the 15 expected "
            "columns are dropped."
        )
        # To Do: rewrite to make sure that each column has a fixed position in any Person.csv
    else:
        df = df[
            [
                "person_id",
                "family_name",
                "first_name",
                "name_lang",
                "sex",
                "birthyear",
                "deathyear",
                "place_of_birth",
                "wikidata_id",
                "created",
                "created_by",
                "last_modified",
                "last_modified_by",
                "note",
            ]
        ]
        valid = True
        print(
            "--> Validate 2/2 \nAll 15 expected columns are included.\nPlease note that any irrelevant column will "
            "be "
            "dropped.\n"
        )
    return valid, df


def __compare_two_rows(row, row_gh):
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


def __overwrite(row, row_gh):
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
            'Fields "'
            + ", ".join(modified_fields)
            + '" is/are overwritten.  By SemBot.'
        )
        if isinstance(row["note"], str):
            row["note"] = row["note"] + " " + message
        else:
            row["note"] = message
    logger.info(message)
    return row


def __compare_wikidata_ids(index, row, df_person_GH):
    wikidata_id_usr = row["wikidata_id"]
    row_gh_index = df_person_GH.index[
        (df_person_GH["person_id"] == row["person_id"])
        & (df_person_GH["name_lang"] == row["name_lang"])
    ].tolist()[0]
    row_GH = df_person_GH.iloc[row_gh_index]
    wikidata_id_gh = row_GH["wikidata_id"]
    if wikidata_id_gh == wikidata_id_usr:
        res = __compare_two_rows(row, row_GH)
        if not res:
            return __overwrite(row, row_GH)
        logger.info("Row %s is checked. Pass ", index)
        return row
    else:
        row[
            "note"
        ] = "Error: `wikidata_id` is not matching with GitHub data. Please check. By SemBot."
        error_msg = (
            "For row "
            + str(int(index))
            + " : `wikidata_id` does not match GitHub data. Please check. By "
            "SemBot."
        )
        logger.error(error_msg)
        sys.exit()


def __check_person_id_size(last_id_in_gh):
    if int(last_id_in_gh[2:]) >= 9999:
        logger.warning(
            "It is better to update all person_id in the database. By SemBot."
        )
        if isinstance(row["note"], str):
            row["note"] = (
                row["note"]
                + " Warning: It is better to update all person_id in the database. By SemBot."
            )
        else:
            row[
                "note"
            ] = "Warning: It is better to update all person_id in the database. By SemBot."


def check_gh(
    df,
):  # a function to check if Person.csv on GitHub has `wikidata_id` column
    if "wikidata_id" not in df.columns:
        error_msg = "There is no `wikidata_id` column in the Person.csv on GitHub. Please inform someone to check it. By SemBot."
        logger.error(error_msg)
        exit()


def get_last_id(df):
    person_ids_GH = df["person_id"].tolist()
    person_ids_GH.sort()
    wikidata_ids_GH = df["wikidata_id"].tolist()
    return person_ids_GH[-1], person_ids_GH, wikidata_ids_GH


def check_each_row(
    index, row, df_person_gh, person_ids_gh, last_person_id, wikidata_ids_GH
):
    today = datetime.date.today().strftime("%Y-%m-%d")
    if row["note"] == "skip" or row["note"] == "Skip":
        return row, last_person_id
    else:
        if isinstance(row["person_id"], str) and len(row["person_id"]) > 0:
            if row["person_id"] in person_ids_gh:
                return __compare_wikidata_ids(index, row, df_person_gh), last_person_id
            else:
                if (isinstance(row["wikidata_id"], str) is True) and (
                    len(row["wikidata_id"]) > 0
                ):
                    if row["wikidata_id"] in wikidata_ids_GH:
                        row[
                            "note"
                        ] = "Error: `wikidata_id` already exists in GitHub data but the person_id does not match. Please check. By SemBot."
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
                                'Fields "'
                                + ", ".join(modified_fields)
                                + '" is/are overwritten.  By SemBot.'
                            )
                            if isinstance(row["note"], str):
                                row["note"] = row["note"] + " " + message
                            else:
                                row["note"] = message
                            logger.info(message)
                            row["last_modified"] = today
                            row["last_modified_by"] = "SemBot"
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
                            row[
                                "note"
                            ] = 'Error: `wikidata_id` queried by family_name, first_name, name_lang already exists in ReadAct data, but your provided person_id does not match. Please check your data carefully. If you are 100% sure that your input is correct, then it is likely that this person has an identical name with a person in Wikidata database. Please put "skip" in "note" column for this row and run this program again. By SemBot.'
                            error_msg = (
                                "For row "
                                + str(int(index))
                                + " :"
                                + " `wikidata_id` queried by "
                                "family_name, "
                                'first_name, name_lang already exists in ReadAct data, but your provided person_id does not match. Please check your data carefully. If you are 100% sure that your input is correct, then it is likely that this person has an identical name with a person in Wikidata database. Please put "skip" in "note" column for this row and run this program again. By SemBot.'
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
                                        + " in this table is/are updated.  By SemBot."
                                    )
                                else:
                                    row["note"] = (
                                        "Fields "
                                        + ", ".join(modified_fields)
                                        + " is/are updated.  By SemBot."
                                    )
                                logger.warning(
                                    "You should look row %s up in Wikidata again. If it does not "
                                    "match with this modification, you should retrieve the old data for "
                                    "this row and put 'skip' in 'note'.",
                                    index,
                                )
                                row["last_modified"] = today
                                row["last_modified_by"] = "SemBot"
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
                                        + " Field `wikidata_id` in this table is updated.  By SemBot."
                                    )
                                else:
                                    row[
                                        "note"
                                    ] = "Field `wikidata_id` in this table is updated.  By SemBot."
                                logger.info(
                                    "'Field `wikidata_id` in row %s of this table is updated.  By SemBot.'",
                                    index,
                                )
                                row["last_modified"] = today
                                row["last_modified_by"] = "SemBot"
                                return row, last_person_id
                    else:
                        if isinstance(row["note"], str):
                            row["note"] = (
                                row["note"] + " No match in Wikidata.  By SemBot."
                            )
                        else:
                            row["note"] = "No match in Wikidata.  By SemBot."
                        # Todo: "note" is changed, does it count as modified?
                        logger.info("Row %s in this table is checked. Pass.", index)
                        return row, last_person_id
        else:  # No user provided `person_id`
            __check_person_id_size(last_person_id)
            row["person_id"] = last_person_id[0:2] + str(int(last_person_id[2:]) + 1)
            if (isinstance(row["wikidata_id"], str) is True) and (
                len(row["wikidata_id"]) > 0
            ):  # no person_id, but has wikidata_id
                if row["wikidata_id"] in wikidata_ids_GH:
                    row[
                        "note"
                    ] = 'Error: this `wikidata_id` already exists in ReadAct. Please check carefully. If you are 100% sure that your input is correct, then it is likely that this person has an identical name with a person in Wikidata database. Please put "skip" in "note" column for this row and run this program again.  By SemBot.'
                    error_msg = (
                        "For row "
                        + str(index)
                        + " : this `wikidata_id` already exists in ReadAct. "
                        'Please check carefully. If you are 100% sure that your input is correct, then it is likely that this person has an identical name with a person in Wikidata database. Please put "skip" in "note" column for this row and run this program again.  By SemBot.'
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
                            + " is/are updated.  By SemBot."
                        )
                        if isinstance(row["note"], str):
                            row["note"] = row["note"] + " " + message
                        else:
                            row["note"] = message
                        row["last_modified"] = today
                        row["last_modified_by"] = "SemBot"
                        logger.info(message)
                        return row, last_person_id
                    else:
                        if isinstance(row["note"], str):
                            row["note"] = (
                                row["note"]
                                + " Field `person_id` in this table is updated.  By SemBot."
                            )
                        else:
                            row[
                                "note"
                            ] = "Field `person_id` in this table is updated.  By SemBot."
                        row["last_modified"] = today
                        row["last_modified_by"] = "SemBot"
                        logger.info(
                            "Field `person_id` in row %s of this table is updated.  By SemBot.",
                            index,
                        )
                        return row, last_person_id
            else:  # no person_id, no wikidata_id
                names = order_name_by_language(row)
                person = sparql_by_name(names, row["name_lang"], 2)
                if len(person) > 0:
                    wikidata_id_usr = next(iter(person))
                    if wikidata_id_usr in wikidata_ids_GH:
                        row[
                            "note"
                        ] = 'Error: `wikidata_id` queried by family_name, first_name, name_lang already exists in ReadAct data, but your provided person_id does not match. Please check your data carefully. If you are 100% sure that your input is correct, then it is likely that this person has an identical name with a person in Wikidata database. Please put "skip" in "note" column for this row and run this program again. By SemBot.'
                        error_msg = (
                            "For row "
                            + str(int(index))
                            + " : `wikidata_id` queried by family_name, "
                            'first_name, name_lang already exists in ReadAct data, but your provided person_id does not match. Please check your data carefully. If you are 100% sure that your input is correct, then it is likely that this person has an identical name with a person in Wikidata database. Please put "skip" in "note" column for this row and run this program again. By SemBot.'
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
                                    + " in this table is/are updated.  By SemBot."
                                )
                            else:
                                row["note"] = (
                                    "Fields "
                                    + ", ".join(modified_fields)
                                    + " is/are updated.  By SemBot."
                                )
                            logger.warning(
                                "For row %s, you should input at least a person_id even if "
                                "there is no matched wikidata_id. By SemBot.",
                                index,
                            )
                            row["last_modified"] = today
                            row["last_modified_by"] = "SemBot"
                            return row, last_person_id
                        else:
                            if isinstance(row["note"], str):
                                row["note"] = (
                                    row["note"]
                                    + " Field `person_id` in this table is updated.  By SemBot."
                                )
                            else:
                                row[
                                    "note"
                                ] = "Field `person_id` in this table is updated.  By SemBot."
                            logger.warning(
                                "For row %s, you should look the person up in Wikidata and input the "
                                "`wikidata_id` in your table in the future.",
                                index,
                            )
                            row["last_modified"] = today
                            row["last_modified_by"] = "SemBot"
                            return row, last_person_id
                else:
                    last_person_id = row["person_id"]
                    message = "Field `person_id` is updated. No match in Wikidata.  By SemBot."
                    if isinstance(row["note"], str):
                        row["note"] = row["note"] + " " + message
                    else:
                        row["note"] = message
                    row["last_modified"] = today
                    row["last_modified_by"] = "SemBot"
                    logger.info(message)
                    return row, last_person_id


if __name__ == "__main__":

    # TODO(DP): This should be a .log file, e.g. "sembot.log" see L10-13 above
    fh = logging.FileHandler("../log.txt")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)

    logger.addHandler(ch)
    logger.addHandler(fh)

    parser = argparse.ArgumentParser(
        description="Validate CSV columns and update information on ReadAct's person.csv"
    )
    parser.add_argument(
        "person_csv", type=str, help="Path to the loal CSV file to be updated"
    )
    parser.add_argument("--update", help="Iterate through CSV rows and update entries")
    parser.add_argument(
        "--version", action="version", version="version 1.0.0", help="print version"
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        dest="verbosity",
        default=0,
        help="verbose output (repeat " "for increased verbosity)",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_const",
        const=-1,
        default=0,
        dest="verbosity",
        help="quiet " "output (show errors only)",
    )
    args = parser.parse_args()

    # #################################################################
    # # 1. Check the input Person.csv
    # #################################################################
    if not args.person_csv.endswith("Person.csv"):
        print("File invalid. You should use only Person.csv as the first argument\n")
    else:
        print("--> Validate 1/2 \nPerson.csv is going to be checked.\n")

    validate_result, df = validate(
        args.person_csv
    )  # TODO: should be replaced with "args.person_csv"
    if not validate_result:
        print("Error: Please check your Person.csv and re-run this tool. By SemBot.")
        quit()
    print("\n======= Finished Checking ========")

    #################################################################
    # 2. Update Person.csv
    #################################################################
    # TODO: The following 3 lines should be activaed once this script is done
    # df_person_Github = pd.read_csv(PERSON_CSV_GITHUB)
    # with open('../CSV/df_person_Github.csv', 'w') as f:
    #     f.write(df_person_Github.to_csv())
    df_person_gh = pd.read_csv("src/CSV/df_person_Github_fake.csv")
    # Replace all the nan into empty string
    df_person_gh = df_person_gh.fillna("")
    check_gh(df_person_gh)
    last_person_id, person_ids_gh, wikidata_ids_GH = get_last_id(df_person_gh)
    for index, row in df.iterrows():
        print("-------------\nFor row ", index, " :")
        print(row.tolist())
        row, last_person_id = check_each_row(
            index, row, df_person_gh, person_ids_gh, last_person_id, wikidata_ids_GH
        )
    with open("src/CSV/Person_updated.csv", "w") as f:
        f.write(df.to_csv())
