# Rittman Analytics Collaborative Development, Deployment and QA Process

This SOP documents the standard approach adopted by Rittman Analytics to work collaboratively on dbt projects in a clean and structured way.

# Setting Up Your Development Environment

## Install Github

1. For Mac: [https://central.github.com/deployments/desktop/desktop/latest/darwin](https://central.github.com/deployments/desktop/desktop/latest/darwin)
2. If you do not already have a github account, create a free one
3. If you do already have an account, ask Mark for access to the "rittmananalytics" account
    - You will also need a login, via github, for dbtCloud. If you are setup on Github and your account is associated with rittmananalytics, you should be able to login to dbtCloud without the need for any further credentials
4. Sign-in, and if you've just installed Github Desktop then type in your full name when prompted so that commits to our repo are properly labelled
5. Select **Github** > **Install Command-Line Tool** to install the git command-line utilities, if you've not installed git before

## Install Atom

1. For Mac: [https://atom.io/download/mac](https://atom.io/download/mac)
2. Install Atom by dragging and dropping the Atom executable to your Applications folder
3. Then go back to Github Desktop, select **Preferences** > **Advanced** and select Atom as your **External Editor**.
4. Start Atom, and then press **Install a Package** > **Open Installer**, then search for the package atom-dbt by Fishtown Analytics, then press **Install**. Then search for and install the language-sql-bigquery 

## Set Up A Project

- If you're creating a new project
    1. Create a private copy of the [Rittman Analytics Data Warehouse framework](https://github.com/rittmananalytics/ra_data_warehouse)
        1. Create a bare clone of the `ra-data-warehouse` repository
            - `git clone --bare https://github.com/rittmananalytics/ra_data_warehouse.git`
        2. Create a new private repository in the client's account
        3. Mirror-push your bare clone to the new client repository
            - `cd ra_data_warehouse.git`
            - `git push --mirror https://github.com/clientaccount/repository.git`
        4. Remove the temporary ra-data-warehouse local repository
            - `rm -rf ra_data_warehouse.git`
    2. Add upstream remotes
        1. Clone the client’s repository
        2. Add the ra-data-warehouse repository as the a remote to fetch future changes
            - `git remote add upstream https://github.com/rittmananalytics/ra_data_warehouse.git`
        3. List remotes
            - `git remote -v`
    3. To update client’s repository with upstream changes (**Still needs to be validated)**
        1. Fetch and merge changes
            - `git pull upstream master`
- If you're added as a collaborator to an existing project
    1. Clone the repository to your local environment
    2. Ask the project's technical lead for instructions to specific configurations required for that project
- Ask your project's technical lead for the following:
    - Account and permissions to data warehouse
    - dbt profile to be added to your local `~/.dbt/profiles.yml` file
        - It should looking something like...

        ```yaml
        clientA:
            target: dev
            outputs:
                dev:
                    type: bigquery
                    method: service-account
                    project: clientA-data-project
                    dataset: analytics_olivier
                    location: EU
                    threads: 1
                    keyfile: /Path/to/json/keyfile.json
                    timeout_seconds: 300
                    priority: interactive
                    retries: 1
        ```

    - To set up schema / datasets that are to be dedicated to your development work

## Install dbt and its virtual environment

Each project has its own version of dbt and packages that it depends on. To not run into dependancy issues, virtual environments are used to development under the same environment as the one in production. Talk to your project's technical lead to learn about this project's dbt version and packages used.

1. Setup your virtual environment: pyenv virtualenvs
    - If you require a new python environment
        - `pyenv versions`
        - `pyenv install 3.7.5`
    - If you require a new virtual environment
        - `cd ~`
        - `pyenv virtualenv venv_clientA`
- Use appropriate virtual environment within git project
    - `cd ~/git_project`
    - `nano .python-version`
    - Write name of virtual environment to use: `venv_clientA`
    - Save and exit
    - Exclude `.python-version` from `.gitignore`

# Development Process

- What's the scope of a development branch
    - Usually associated to a story (either a feature, bug, technical debt, etc.) that is assigned to you
    - **Resist the urge to add more than what's scoped by the story and push back on anyone asking to integrate more than is required**
- Development steps
    - Create a development branch: `git checkout -b my_development_branch`
    - dbt development, testing and documentation
    - Regularly compile your under-development models: `dbt run --model my_cool_model` (implicitly, this will run against the `dev` profile as we have made it the default)
    - View results of your changes using your favorite SQL IDE. For example:

    ```sql
    select * from analytics_olivier.transactions_fact
    ```

    - Compile the whole dbt project: `dbt run`
    - Test the whole project: `dbt test`
    - Commit your changes
        - `git add -A`
        - `git commit -m "Those are the changes I'm making with this commit"`
        - `git push origin my_development_branch`

# Deployment Process

- Only when the following conditions are met should you submit a PR
    - [ ]  The proposed changes accomplish the story's requirements
    - [ ]  The branch compiles and the tests run successfully
    - [ ]  You ran `dbt docs generate & serve` and all proposed changes are reflected in the new documentations
- Steps
    - [ ]  Commit your latest revision of the branch to the repository
    - [ ]  Press the **Create Pull Request** button in Github
    - [ ]  Fill out the PR's submission template that is documented below
    - [ ]  Github will then, via dbt, run an automatic build test of branch which will need to pass before the pull request can be merged into the master branch of the dbt repo (talk to the project's technical lead if not automated build tests are being ran)
    - [ ]  Add reviewers (at least including the technical lead) once you've created your PR, so that other team members get familiarized with your work and validate that the proposed changes will indeed accomplish the story's requirements, follow coding standards, be fully tested and documented
    - [ ]  Inform the project team that a PR has been submitted for the story you'r working on and that anyone is welcomed to review and submit questions/changes if they so desire
    - [ ]  Once the review process is completed, it is the responsibility of the technical lead to merge the PR with `master`
    - [ ]  It is also the technical lead's responsibility to announce to the project team that a new version of `master` has been created and that everyone should refresh their local `master` branch.
- PR's submission template
    - Description & motivation
    - Changes to existing models
    - Screenshots of DAG's changes
    - Feature Release Checklist
    - Checklist
        - [ ]  My pull request represents one logical piece of work.
        - [ ]  My commits are related to the pull request and look clean.
        - [ ]  My SQL follows the Fishtown Analytics style guide ([https://github.com/fishtown-analytics/corp/blob/master/dbt_coding_conventions.md](https://github.com/fishtown-analytics/corp/blob/master/dbt_coding_conventions.md))
        - [ ]  I have materialized my models appropriately.
        - [ ]  I have added appropriate tests and documentation to any new models.
        - [ ]  I ran the dbt package in my development environment without error
        - [ ]  My dbt package has no residual models that are no longer used
        - [ ]  Any data issues remaining are client's responsibility
