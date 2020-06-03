## Branch Development Process for dbt using Github Desktop and Atom

Note that these are the standard steps and tools used by the team at Rittman Analytics to develop new client feature branches for the RA Data Warehouse dbt package. Your own choice of tools, laptop OS and git development strategy may vary.

1. Install Github Desktop for Mac [https://central.github.com/deployments/desktop/desktop/latest/darwin](https://central.github.com/deployments/desktop/desktop/latest/darwin)
    1. If you do not already have a github account, create a free one
    2. If you do already have an account, ask Mark for access to the "rittmananalytics" account
        1. You will also need a login, via github, for dbtCloud. If you are setup on Github and your account is associated with rittmananalytics, you should be able to login to dbtCloud without the need for any further credentials
    3. Sign-in, and if you've just installed Github Desktop then type in your full name when prompted so that commits to our repo are properly labelled
    4. Select **Github** > **Install Command-Line Tool** to install the git command-line utilities, if you've not installed git before
2. Install Atom [https://atom.io/download/mac](https://atom.io/download/mac)
    1. Install Atom by dragging and dropping the Atom executable to your Applications folder
    2. Start Atom, and then press **Install a Package** > **Open Installer**, then search for the package atom-dbt by Fishtown Analytics, then press **Install**. Then search for and install the language-sql-bigquery 
    3. Then go back to Github Desktop, select **Preferences** > **Advanced** and select Atom as your **External Editor**.
3. Clone the RA Warehouse repo (or your forked client-specific copy) locally
    1. Using Github Desktop, select rittmananalytics/ra_data_warehouse (or your client-specific forked repo) and press **Clone rittmananalytics/ra_data_warehouse** 
    2. Press the **Fetch Origin** button to fetch the latest set of branches and commits.
    3. Click **Show In Finder** to see where Github Desktop has cloned your files to
4. Create a new development branch
    1. Click on the **Branches** drop-down within Github Desktop and press **Create New Branch**
    2. Name the branch and press the **Create Branch** button
    3. Click on **Publish Branch** to push this new branch to the remote Github repo
    4. Select **Repository** > **View in External Editor** to open the repo files in Atom
    5. Do your dbt development work in Atom, and at the end, save your work. 
5. Commit your changes
    1. Within Github Desktop, click on the Changes tab and then type in a summary describing the changes you have made, then press **Commit to <your branch name>**
    2. Now press the **Push Origin** button to push this commit to the remote origin repo
6. Create a Pull Request to incorporate branch into main master branch
    1. When you are ready to request your branch be incorporate into the master branch, press the **Create Pull Request** button in Github Desktop
    2. Within the **Open a Pull Request** page on the Github website that was automatically opened for you, type in a summary of what the pull request will change or implement and press the **Open Pull Request** button
    3. Github will then, via dbt, run an automatic build test of branch which will need to pass before the pull request can be merged into the master branch of the dbt repo 
    4. If the automated build test reveals errors in the models or other dbt code you are looking to merge with this branch, click on the **Details** link to go into dbtCloud to review and then fix these errors.
    5. When all tests are completed successfully, press the **Squash Merge** button within Github to merge your branch into the master dbt branch, so that your model code will then compile and execute next time a scheduled dbt run happens.
