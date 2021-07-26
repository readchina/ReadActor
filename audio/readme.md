# Instructions for Audio Files
This is the repository for audio files of interview recordings. To protect the privacy of interviewee each interviewee can only be identified via an ID construct containing of the abbreviation of your work-package and a three digit number. You should keep a copy that helps you dereference the ID of interviewees with real names, but under no circumstances should this information be uploaded to Github.

## Format
Use an uncompressed format, i.e. `.wav` for interview recordings. Unless you are recording musical performances 16bit at 44.1 khz is usually sufficient.
A 45min interview comes down to roughly 500MB in uncompressed format. To upload such files you need to use git large file storage `git-lfs`, you can find out more about it [here](https://git-lfs.github.com).
This repo is already configured to track `.wav` files using lfs.

## Directories and Files
The directory with the initial letter of your surname contains a folder of the format `YYYY`. You should group your recordings by year of recording. So the full path so far would be: `materials/audio/H/2019`. If the folder is out of date simple create a new one for, e.g. `2020` and sent a PR to the template repo.

### Subfolders
Usually there should not be a need for additional subfolders, but in cases where exceptionally many interviews are conducted on the same date and in the same location, create folders based on the Subject ID of the interviewee.

### File Name
Filenames should be unique across the project, and help you identify what a file contains when just looking at its name. Each File name consists of 4 elements separated by underscores `_`:
-   the date of the recording in full format `YYYY-MM-DD`, e.g. `2019-05-01`
-   the activity (usually `interview`) in no cap letters,
-   the abbreviation of your workpackage, `WP1`, followed by `-` and a the three digit numerical ID of your interviewee, e.g. `WP1-003`
-   two digits for your first, second, etc. interview with the interviewee, e.g. `01`, `02`, etc.


the full file name would look like this (see the example file): `2019-05-01_interwiew_WP1-003_02.wav`

## Readme
Each folder should contain a readme with basic information about the interview, the topics, covered its date and location in written language. Imaging coming back to the folder in a few years time trying to see whats in it. Use full sentences on the readme, not just keywords and abbreviations (i.e. `Shanghai`, not `~~SH~~`). Tools for adding keywords to your files are covered elsewhere, the readme has to be able to stand on its own.
A basic readme has already been included here, feel free to copy and paste it as many times as you need.
