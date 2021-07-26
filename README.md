
# Material Filing Template
This repo contains a folder template and readme instructions for how to go about storing the primary source materials collected during field work.

The goal is to use and document a uniform filing system to maintain long-term usability of our data. The easiest way to use this repo is to simply fork it and start filling out the provided templates. Of course you are free to start creating them from scratch too, as long as you follow the conventions defined here.


## Important
Before backing-up files via GitHub you must decide if the data you are about to share is in the public domain, or if it contains sensitive information and should remain private for either legal or privacy reasons.

Sensitive data **must** be encrypted before uploading, it should be stored in private repos only! You have received detailed instructions for how to encrypt files before uploading. This information will not be repeated here.

Once you have determined if your files are either private or public you can simply follow the instructions outlined below. Lastly, work releated files should be uploaded into the `readchina` organization. 

## How To
This repo contains two top-level folders `audio` and `image`, since these are the most frequent data types you are likely to collect during field work. Should you wish to collect a significant number of other file types simple create a new top level folder and name if after the data-type of the files collected there, e.g. `text`. Each folder contains a `readme.md` file that outlines the naming and folder conventions and contains some boilerplate for you to fill in when you are collecting data in the field. They also contain a sample file for demonstration purposes, make sure to delete the sample file before publishing your data.

## Use as Template
Since creating this repo, a new feature has been added by GitHub. To use this repo as a template simply use the `Use Template` button on the right above. Should this not work as you expect it to, the step-by-step instructions should still work. 

### Step-by-Step
1.  [Create](https://help.github.com/articles/creating-a-new-repository/) either a public or private repository for your data, e.g. `readchina/dp-interviews` on GitHub. You'll need the URL of this new repo in step 3 below.

2.  On your computer go to a folder where you want to collect the data, e.g. `my-data`. Make sure not to use whitespaces for folder names, so not ~~`My Data`~~.
    ```bash
    cd my-data
    ```

3.  Clone this repo, and then push the mirrored clone to your new repo (make sure to adjust the URL in the last line according to the name of your repo)
    ```bash
    git clone --bare https://github.com/readchina/materials.git
    cd materials.git
    git lfs fetch --all
    git push --mirror https://github.com/readchina/dp-interview.git
    ```

    1.  clean up
        ```bash
        cd ..
        rm -rf materials.git
        ```

4.  Congratulations your new repository is now set up and ready to receive files. It should be inside the default GitHub folder on your computer alongside your other repos. Go to a folder with the first letter of your surname (**H**enningsen, **L**in, **M**andzunowski, **P**aterson,**Y**ang) inside either `audio` or `image` as required, e.g.:
    ```bash
    cd Documents/GitHub/dp_interviews
    ```
    The above is the default macOS path, adjust as necessary.

5.  Check the `readme.md` for either [image](image/readme.md) or [audio](audio/readme.md) files as necessary and fill out the template.

6.  Add you files, commit and push as usual.
