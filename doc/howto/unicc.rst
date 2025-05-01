Run :program:`mix-models` on UnICC
**********************************

This is a guide on how to get set up on the Unified IIASA Computing Cluster (UnICC) and how to run MESSAGEix scenarios on the cluster.

.. attention::

   - The steps in this guide will only be actionable for IIASA staff and collaborators who have access to the UnICC.
     It *may* be of use to others with access to similar systems, but is not (yet) intended as a general guide.
   - The information contained is up-to-date as of 2025-01-16.
     Changes to the cluster configuration may change the required steps.

.. contents::
   :local:
   :backlinks: none

Prerequisites and good-to-knows
===============================

Access to the UnICC
-------------------

To access the UnICC, an IIASA account is required.
With your IIASA account, create a ticket with Information and Communication Technologies (ICT) to request access to the UnICC.
The intranet page on the UnICC can be found `here <https://iiasahub.sharepoint.com/sites/ict/SitePages/Scientific-Computing.aspx>`__.
On the intranet webpage, the Slurm User Guide file has a section on how to request access to the UnICC, including what information needs to be provided to ICT in your request:

1. Are there any existing shared projects folder inside the cluster that you need access to?
2. Do you need a new shared project folder inside the cluster?
   In this case, please specify the project name (default size 1 TB), also the name of the users who need access to the folder.
3. Please note that existing home folders will be automatically attached.
4. Please describe which already existing P: drive folder(s) you need access to from inside the cluster.
5. Please note, a 5GB home folder will be automatically created for you in the cluster.

Storage space
~~~~~~~~~~~~~

When requesting access to UnICC, 5GB of space on your home directory will likely be given by default.
While setting up the MESSAGE environment, it is easy to hit the maximum (the repositories like `message_data` are big and GAMS installation itself is almost 2GB on its own).
So, request more space upfront or ask for an increase later (it is possible to request 50GB of storage space, and increase that even further later).

Network drive access
~~~~~~~~~~~~~~~~~~~~

As part of the questionnaire above for the ticket, specify which P: drive folders you need access to.
Additionally, access to your H: drive on the cluster will be automatically granted.
Every user's H: drive is located on the cluster in `/hdrive/all_users/[username]`.

If a shared project folder was requested, it will be located in ``/projects/[project name]``.

Using MESSAGE environments on H: drive vs setting up new MESSAGE Environments
-----------------------------------------------------------------------------

This guide walks through the process of installing a MESSAGEix environment from source on the cluster (in your home directory).
Theoretically, because the H: drive can be accessed on the cluster, repositories and MESSAGEix environments could possibly be in your H: drive folder.
Then, potentially, just activate your MESSAGE environment(s) from the H: drive, saving the trouble of creating new MESSAGE environments.

Working in terminal
-------------------

The rest of this document assumes you're in a terminal window on the UnICC cluster and not in a notebook.

Also, throughout this guide :program:`nano` is used to edit files.
If :program:`nano` is not familiar, use :program:`vim`, :program:`emacs` or any other text editor you're comfortable with.

Git-related setup
=================

Generate SSH Key
----------------

This was needed to clone GitHub repositories.

Follow GitHub's instructions to `generate a new SSH key and add it to the ssh-agent <https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent>`_, then `add the new SSH key to your GitHub account <https://docs.github.com/en/authentication/connecting-to-github-with-ssh/adding-a-new-ssh-key-to-your-github-account>`_

Run:

.. code:: bash

   ssh-keygen -t ed25519 -C "you@email.com" # replace with your own keygen info and email

Received prompt:

.. code:: bash

   Generating public/private ed25519 key pair.
   Enter file in which to save the key (/h/u142/username/.ssh/id_ed25519):
   Enter passphrase (empty for no passphrase):

(Save your passphrase somewhere safe.)

Add SSH Key to SSH-Agent
------------------------

Start ssh-agent in the background:

.. code:: bash

   eval "$(ssh-agent -s)"

Add SSH private key to ssh-agent:

.. code:: bash

   ssh-add ~/.ssh/id_ed25519

Add SSH Key to GitHub Account
-----------------------------

Run:

.. code:: bash

   cat ~/.ssh/id_ed25519.pub

Copy the content.

On GitHub, go to Settings > SSH and GPG keys.

Click on “New SSH key”.

Name new SSH key and paste the key.

Creating Personal Access Tokens
-------------------------------

This was needed to clone message_data for some reason.

Refer to
`creating a personal access token <https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens>`_ for instructions.

In Settings > Developer settings > Personal access tokens > Fine-grained tokens

1. Click “Tokens (classic)”
2. Select Generate new token > Generate new token (classic)
3. Enter token name “IIASA UnICC”
4. Select “No expiration”.

Add Email and Username to Global Git Config
-------------------------------------------

.. code:: bash

   git config --global user.email "you@email.com" # replace with your GitHub email
   git config --global user.username "username" # replace with your GitHub username
   git config --global user.name "Firstname Lastname" # replace with your name

Auto Load Python and Java on Startup
------------------------------------

Add the following to :file:`$HOME/.bash_profile` (by entering :code:`nano ~/.bash_profile`):

.. code:: bash

   module purge
   module load Python/3.11.5-GCCcore-13.2.0
   module load Java
   module load git-lfs

This ensures that the correct Python version is loaded (and added to
$PATH) and that Java is loaded (and added to $PATH) each time the terminal is loaded.

Create Virtual Environment
--------------------------

A lot of people on the team use ``conda`` but Python’s ``venv`` is used to create the virtual environment.

.. important::

   When initially trying to create a virtual environment by just running :code:`python -m venv my_env`, it caused issues when trying to activate the environment in a Slurm job.
   It works just fine interactively on the node, but when using within a job, it would fail to activate.

   The reason is because the default :program:`python` command on the interactive node creates an environment using the default Python instance, inherited from Jupyter, which is not accessible from the compute nodes where the Slurm job will run.
   So it’s necessary to create an environment the following way.

In the home directory (:file:`~` or :file:`$HOME`), run the following to create and activate the virtual environment (note that if the instructions earlier to run :code:`module purge`` or :code:`module load` in your :file:`~/.bash_profile` were followed, these steps probably don’t have to be done again):

.. code:: bash

   module purge
   module load Python/3.11.5-GCCcore-13.2.0
   python3 -m venv env/env_name
   source ~/env/env_name/bin/activate

Install MESSAGEix Ecosystem by Source
=====================================

Get ``message_ix`` Repository
-----------------------------

Run:

.. code:: bash

   git clone https://github.com/username/message_ix.git # replace with your own fork or the IIASA repo
   cd message_ix
   git remote add upstream https://github.com/iiasa/message_ix
   git pull upstream main
   git fetch --all --tags

Install ``message_ix``
----------------------

1. Navigate to the local ``message_ix`` repo root directory.

2. Ensure you’re on the ``main`` branch:

   .. code:: bash

      git checkout main

3. Ensure branch is up-to-date:

   .. code:: bash

      git pull upstream main

4. Fetch the version tags:

   .. code:: bash

      git fetch --all --tags

5. Install from source:

   .. code:: bash

      pip install --editable .[docs,reporting,tests,tutorial]

6. Check ``message_ix`` is installed correctly:

   .. code:: bash

      message-ix show-versions

Get ``ixmp`` Repository
-----------------------

.. code:: bash

   git clone https://github.com/username/ixmp.git # replace with your own fork or the IIASA repo
   cd ixmp
   git remote add upstream https://github.com/iiasa/ixmp
   git pull upstream main
   git fetch --all --tags

Install ``ixmp``
----------------

1. Navigate to the local ``ixmp`` repo root directory.

2. Ensure you’re on the ``main`` branch.

   .. code:: bash

      git checkout main

3. Ensure branch is up-to-date:

   .. code:: bash

      git pull upstream main

4. Fetch the version tags:

   .. code:: bash

      git fetch --all --tags

5. Install from source:

   .. code:: bash

      pip install --editable .[docs,tests,tutorial]

Get ``message-ix-models`` Repository
------------------------------------

.. code:: bash

   git clone https://github.com/username/message-ix-models.git # replace with your own fork or the IIASA repo
   cd message-ix-models
   git remote add upstream https://github.com/iiasa/message-ix-models
   git fetch --all --tags
   git pull upstream main

Install ``message-ix-models``
-----------------------------

1. Navigate to the local ``message-ix-models`` root directory.

2. Ensure you’re on the ``main`` branch:

   .. code:: bash

      git checkout main

3. Ensure branch is up-to-date:

   .. code:: bash

      git pull upstream main

4. Fetch the version tags:

   .. code:: bash

      git fetch --all --tags

5. Install from source:

   .. code:: bash

      pip install --editable .

Install :program:`git-lfs`
--------------------------

UnICC already has :program:`git lfs` installed on the system, but you may still need install large file storage for ``message_data`` or ``message-ix-models``.
Note that you may not have to, as perhaps you don't need to access the large files in these repositories for your work.
The benefit of not installing is that you don't end up using all the needed storage space.
But if you do need access to those files, then follow the instructions below.
The same instructions can be followed from the root directory of ``message_data`` or ``message_ix_models``.

Load ``git lfs`` (if included in your ``~/.bash_profile`` like written earlier, this line doesn’t have to be run):

.. code:: bash

   module load git-lfs

Then, within the root directory of ``message-ix-models`` or ``message_data`` run the following:

.. code:: bash

   git lfs install

Then fetch and pull the lfs files (this might take a while):

.. code:: bash

   git lfs fetch --all
   git lfs pull

Get ``message_data`` Repository
-------------------------------

.. code:: bash

   git clone git clone git@github.com:username/message_data.git # replace with your own fork or the IIASA repo
   cd message_data
   git remote add upstream https://github.com/iiasa/message_data
   git fetch --all --tags

Install ``message_data``
------------------------

1. Navigate to the local ``message_data`` root directory.

2. Ensure you're on the branch you want to be on:

   .. code:: bash

      git checkout branch # replace "branch" with the branch you want to be on

3. Ensure branch is up-to-date:

   .. code:: bash

      git pull upstream branch

4. Fetch the version tags:

   .. code:: bash

      git fetch --all --tags

5. Install from source with all options:

   .. code:: zsh

      pip install --no-build-isolation --editable .[ci,dl,scgen,tests]

   If the above doesn’t work, remove the ``--no-build-isolation``:

   .. code:: zsh

      pip install --editable .[ci,dl,scgen,tests]

Also grab lfs:

.. code:: bash

   git lfs fetch --all
   git lfs pull

GAMS
----

From module
~~~~~~~~~~~

GAMS is provided as a module.
Load the module:

.. code:: bash

   module load gams

Install manually
~~~~~~~~~~~~~~~~

Go to the following website to get the download of GAMS: https://www.gams.com/download/

Click on the Linux download link, and then when the download popup
window shows up, right click and copy the link instead.
Use the link to put in the terminal to download the file:

.. code:: bash

   cd downloads
   wget https://d37drm4t2jghv5.cloudfront.net/distributions/46.5.0/linux/linux_x64_64_sfx.exe

The Linux installation instructions are here:
https://www.gams.com/46/docs/UG_UNIX_INSTALL.html

Create a location/directory where GAMS will be installed and navigate to it (in this case, it is in a folder called ``~/opt/gams``)

.. code:: bash

   cd ~
   mkdir opt
   cd opt/
   mkdir gams
   cd gams/

Run the installation file by simply inputting the filename (complete with path) into the command line:

.. code:: bash

   ~/downloads/linux_x64_64_sfx.exe # replace with your own path

However, a permissions error was received:

.. code:: bash

   bash: /home/username/downloads/linux_x64_64_sfx.exe: Permission denied

If so, run the following:

.. code:: bash

   chmod 754 /home/username/downloads/linux_x64_64_sfx.exe # replace path with your own path to the .exe file

Then try to run the executable file again:

.. code:: bash

   ~/downloads/linux_x64_64_sfx.exe

This should start the installation of GAMS and create a folder in ``~/opt/gams`` (or wherever GAMS is being installed) called ``gams46.5_linux_x64_64_sfx``.
Navigate into this folder:

.. code:: bash

   cd gams46.5_linux_x64_64_sfx

When within the ``/home/username/opt/gams/gams46.5_linux_x64_64_sfx``, run the ``gams`` command to see if it works (but at this moment the full path of the ``gams`` command has to be referenced, which is ``/home/username/opt/gams/gams46.5_linux_x64_64_sfx/gams``):

.. code:: bash

   → /home/username/opt/gams/gams46.5_linux_x64_64_sfx/gams
   --- Job ? Start 06/11/24 14:18:48 46.5.0 a671108d LEX-LEG x86 64bit/Linux
   ***
   *** GAMS Base Module 46.5.0 a671108d May 8, 2024           LEG x86 64bit/Linux
   ***
   *** GAMS Development Corporation
   *** 2751 Prosperity Ave, Suite 210
   *** Fairfax, VA 22031, USA
   *** +1 202-342-0180, +1 202-342-0181 fax
   *** support@gams.com, www.gams.com
   ***
   *** GAMS Release     : 46.5.0 a671108d LEX-LEG x86 64bit/Linux
   *** Release Date     : May 8, 2024
   *** To use this release, you must have a valid license file for
   *** this platform with maintenance expiration date later than
   *** Feb 17, 2024
   *** System Directory : /home/username/opt/gams/gams46.5_linux_x64_64_sfx/
   ***
   *** License          : /home/username/opt/gams/gams46.5_linux_x64_64_sfx/gamslice.txt
   *** GAMS Demo, for EULA and demo limitations see   G240131/0001CB-GEN
   *** https://www.gams.com/latest/docs/UG%5FLicense.html
   *** DC0000  00
   ***
   *** Licensed platform                             : Generic platforms
   *** The installed license is valid.
   *** Evaluation expiration date (GAMS base module) : Jun 29, 2024
   *** Note: For solvers, other expiration dates may apply.
   *** Status: Normal completion
   --- Job ? Stop 06/11/24 14:18:48 elapsed 0:00:00.001

Based on the output, there already is a gamslice (located in ``~/opt/gams/gams46.5_linux_x64_64_sfx``), which the contents can be checked:

.. code:: bash

   → cat gamslice.txt
   GAMS_Demo,_for_EULA_and_demo_limitations_see_________________ […]
   https://www.gams.com/latest/docs/UG%5FLicense.html_______________
   […]

This seems to be a demo gamslice license, so rename it to ``gamslice_demo.txt`` so it can be replaced with a proper license.

.. code:: bash

   mv gamslice.txt gamslice_demo.txt

Copy one of the GAMS licenses in the ECE program folder and put it into the H: drive in a folder called ``gams``.
Within UnICC, the H: drive can be accessed via: ``/hdrive/all_users/username/``.

So, copy the GAMS license from the H: drive to the GAMS installation location (the paths will be different depending on where the file is saved on your own H: drive):

.. code:: bash

   cp /hdrive/all_users/username/gams/gamslice_wCPLEX_2024-12-20.txt /home/username/opt/gams/gams46.5_linux_x64_64_sfx/

Then, within the ``/home/username/opt/gams/gams46.5_linux_x64_64_sfx/`` folder, rename the ``gamslice_wCPLEX_2024-12-20.txt`` file to just ``gamslice.txt``:

.. code:: bash

   mv gamslice_wCPLEX_2024-12-20.txt gamslice.txt

Now, when the ``gams`` command is called, the output looks like this:

.. code:: bash

   → /home/username/opt/gams/gams46.5_linux_x64_64_sfx/gams
   --- Job ? Start 06/11/24 14:24:43 46.5.0 a671108d LEX-LEG x86 64bit/Linux
   ***
   *** GAMS Base Module 46.5.0 a671108d May 8, 2024           LEG x86 64bit/Linux
   ***
   *** GAMS Development Corporation
   *** 2751 Prosperity Ave, Suite 210
   *** Fairfax, VA 22031, USA
   *** +1 202-342-0180, +1 202-342-0181 fax
   *** support@gams.com, www.gams.com
   ***
   *** GAMS Release     : 46.5.0 a671108d LEX-LEG x86 64bit/Linux
   *** Release Date     : May 8, 2024
   *** To use this release, you must have a valid license file for
   *** this platform with maintenance expiration date later than
   *** Feb 17, 2024
   *** System Directory : /home/username/opt/gams/gams46.5_linux_x64_64_sfx/
   ***
   *** License          : /home/username/opt/gams/gams46.5_linux_x64_64_sfx/gamslice.txt
   *** Small MUD - 5 User License                     S230927|0002AP-GEN
   *** IIASA, Information and Communication Technologies Dep.
   *** DC216   01M5CODICLPTMB
   *** License Admin: Melanie Weed-Wenighofer, wenighof@iiasa.ac.at
   ***
   *** Licensed platform                             : Generic platforms
   *** The installed license is valid.
   *** Maintenance expiration date (GAMS base module): Dec 20, 2024
   *** Note: For solvers, other expiration dates may apply.
   *** Status: Normal completion
   --- Job ? Stop 06/11/24 14:24:43 elapsed 0:00:00.000

I then add the GAMS path to my ``~/.bash_profile``:

.. code:: bash

   # add GAMS to path
   export PATH=$PATH:/home/username/opt/gams/gams46.5_linux_x64_64_sfx

I also add the GAMS aliases:

.. code:: bash

   # add GAMS to aliases
   alias gams=/home/username/opt/gams/gams46.5_linux_x64_64_sfx/gams
   alias gamslib=/home/username/opt/gams/gams46.5_linux_x64_64_sfx/gamslib

Now, running just ``gams`` anywhere in the terminal gives the following output:

.. code:: bash

   → gams
   --- Job ? Start 06/11/24 15:14:28 46.5.0 a671108d LEX-LEG x86 64bit/Linux
   ***
   *** GAMS Base Module 46.5.0 a671108d May 8, 2024           LEG x86 64bit/Linux
   ***
   *** GAMS Development Corporation
   *** 2751 Prosperity Ave, Suite 210
   *** Fairfax, VA 22031, USA
   *** +1 202-342-0180, +1 202-342-0181 fax
   *** support@gams.com, www.gams.com
   ***
   *** GAMS Release     : 46.5.0 a671108d LEX-LEG x86 64bit/Linux
   *** Release Date     : May 8, 2024
   *** To use this release, you must have a valid license file for
   *** this platform with maintenance expiration date later than
   *** Feb 17, 2024
   *** System Directory : /home/username/opt/gams/gams46.5_linux_x64_64_sfx/
   ***
   *** License          : /home/username/opt/gams/gams46.5_linux_x64_64_sfx/gamslice.txt
   *** Small MUD - 5 User License                     S230927|0002AP-GEN
   *** IIASA, Information and Communication Technologies Dep.
   *** DC216   01M5CODICLPTMB
   *** License Admin: Melanie Weed-Wenighofer, wenighof@iiasa.ac.at
   ***
   *** Licensed platform                             : Generic platforms
   *** The installed license is valid.
   *** Maintenance expiration date (GAMS base module): Dec 20, 2024
   *** Note: For solvers, other expiration dates may apply.
   *** Status: Normal completion
   --- Job ? Stop 06/11/24 15:14:28 elapsed 0:00:00.000

I can also test if GAMS is working properly by running ``gams trnsport``:

.. code:: bash

   →  gams trnsport
   --- Job trnsport Start 06/11/24 15:15:00 46.5.0 a671108d LEX-LEG x86 64bit/Linux
   --- Applying:
       /home/username/opt/gams/gams46.5_linux_x64_64_sfx/gmsprmun.txt
   --- GAMS Parameters defined
       Input /home/username/opt/gams/gams46.5_linux_x64_64_sfx/trnsport.gms
       ScrDir /home/username/opt/gams/gams46.5_linux_x64_64_sfx/225a/
       SysDir /home/username/opt/gams/gams46.5_linux_x64_64_sfx/
   Licensee: Small MUD - 5 User License                     S230927|0002AP-GEN
             IIASA, Information and Communication Technologies Dep.      DC216
             /home/username/opt/gams/gams46.5_linux_x64_64_sfx/gamslice.txt
             License Admin: Melanie Weed-Wenighofer, wenighof@iiasa.ac.at
             The maintenance period of the license will expire on Dec 20, 2024
   Processor information: 2 socket(s), 128 core(s), and 256 thread(s) available
   GAMS 46.5.0   Copyright (C) 1987-2024 GAMS Development. All rights reserved
   --- Starting compilation
   --- trnsport.gms(66) 3 Mb
   --- Starting execution: elapsed 0:00:00.022
   --- trnsport.gms(43) 4 Mb
   --- Generating LP model transport
   --- trnsport.gms(64) 4 Mb
   ---   6 rows  7 columns  19 non-zeroes
   --- Range statistics (absolute non-zero finite values)
   --- RHS       [min, max] : [ 2.750E+02, 6.000E+02] - Zero values observed as well
   --- Bound     [min, max] : [        NA,        NA] - Zero values observed as well
   --- Matrix    [min, max] : [ 1.260E-01, 1.000E+00]
   --- Executing CPLEX (Solvelink=2): elapsed 0:00:00.053

   IBM ILOG CPLEX   46.5.0 a671108d May 8, 2024           LEG x86 64bit/Linux

   --- GAMS/CPLEX Link licensed for continuous and discrete problems.
   --- GMO setup time: 0.00s
   --- GMO memory 0.50 Mb (peak 0.50 Mb)
   --- Dictionary memory 0.00 Mb
   --- Cplex 22.1.1.0 link memory 0.00 Mb (peak 0.00 Mb)
   --- Starting Cplex

   Version identifier: 22.1.1.0 | 2022-11-28 | 9160aff4d
   CPXPARAM_Advance                                 0
   CPXPARAM_Simplex_Display                         2
   CPXPARAM_MIP_Display                             4
   CPXPARAM_MIP_Pool_Capacity                       0
   CPXPARAM_MIP_Tolerances_AbsMIPGap                0
   Tried aggregator 1 time.
   LP Presolve eliminated 0 rows and 1 columns.
   Reduced LP has 5 rows, 6 columns, and 12 nonzeros.
   Presolve time = 0.00 sec. (0.00 ticks)

   Iteration      Dual Objective            In Variable           Out Variable
        1              73.125000    x(seattle,new-york) demand(new-york) slack
        2             119.025000     x(seattle,chicago)  demand(chicago) slack
        3             153.675000    x(san-diego,topeka)   demand(topeka) slack
        4             153.675000  x(san-diego,new-york)  supply(seattle) slack

   --- LP status (1): optimal.
   --- Cplex Time: 0.00sec (det. 0.01 ticks)


   Optimal solution found
   Objective:          153.675000

   --- Reading solution for model transport
   --- Executing after solve: elapsed 0:00:00.482
   --- trnsport.gms(66) 4 Mb
   *** Status: Normal completion
   --- Job trnsport.gms Stop 06/11/24 15:15:01 elapsed 0:00:00.483

Set Up ``ixmp_dev``
-------------------

If you are a MESSAGEix developer with access to the `ixmp_dev` database, set up your access to the `ixmp_dev` database.

Running MESSAGEix on the cluster
================================

Example script
--------------
Here is a simple Python script to simply grab, clone, and solve a MESSAGE.
Create it by calling `nano ~/job/message/solve.py`, then pasting the following:

.. code:: python

    import message_ix

    # select scenario
    model_orig = "model" # replace with name of real model
    scen_orig = "scenario" # replace with name of real scenario

    # target scenario
    model_tgt = "unicc_test"
    scen_tgt = scen_orig + "_cloned"
    comment = "Cloned " + model_orig + "/" + scen_orig

    # load scenario
    print("Loading scenario...")
    s, mp = message_ix.Scenario.from_url("ixmp://ixmp_dev/" + model_orig + "/" + scen_orig)

    # clone scenario
    print("Cloning scenario...")
    s_new = s.clone(model_tgt, scen_tgt, comment, keep_solution=False)

    # solve the cloned scenario
    print("Solving scenario...")
    s_new.set_as_default()
    s_new.solve(
        "MESSAGE",
    )

    # close db
    print("Closing database...")
    mp.close_db()


Submitting Jobs
---------------

To submit a job, create a new file called ``job.do``, but it doesn’t have to be called that and it can have any file extension.
For example, it can be called ``submit.job`` or even ``hi.jpeg``, and those would all work.
So, run:

.. code:: bash

   nano ~/job/message/job.do

In the editor, write/paste:

.. code:: bash

    #!/bin/bash
    #SBATCH --time=3:00:00
    #SBATCH --mem=40G
    #SBATCH --mail-type=BEGIN,END,FAIL
    #SBATCH --mail-user=username@iiasa.ac.at
    #SBATCH -o ~/out/solve_%J.out
    #SBATCH -e ~/err/solve_%J.err

    module purge
    source /opt/apps/lmod/8.7/init/bash
    module load Python/3.11.5-GCCcore-13.2.0
    module load Java

    echo "Activating environment..."
    source ~/env/env-name/bin/activate

    echo "Running python script..."
    python ~/job/message/solve.py

This script requests the following:

- 3 hours of time
- 40 GB of memory
- Send an email when the job begins and ends (or fails)
- Send email to the address provided
- Save the outputs of the job (not the solved scenario, just any print statements in the Python script or anything like that) in ``/home/username/out/message/``, and the file would be called ``solve_%J.out`` where the “%J” is the job number
- Same as above, but saves the errors in an ``err`` folder. This is helpful when the script outputs a lot of warnings or errors and now there is a separate file for errors/warnings and a separate file for just the output.

You can choose to forego saving the outputs and errors to files, but it is helpful to have them saved somewhere in case you need to refer back to them or to see what happened during the job.
If using the exact same script as above, you will have to manually create the ``out`` and ``err`` folders in the home directory first, if they don't already exist.
You can do this by running:

.. code:: bash

    mkdir ~/out
    mkdir ~/err

It is important (I think) to load the Python and Java modules.
I’m not sure why the ``source /opt/apps/lmod/8.7/init/bash`` line is there, but ICT included that in an email to me when I was asking for help.

To submit the job, run the following (assuming you are in the folder
where ``job.do`` is located):

.. code:: bash

   sbatch job.do

The ``sbatch`` command is what submits the job, and whatever argument that comes after it is your job file.

Checking queue
--------------

To check the status of the job(s) by the user:

.. code:: bash

   squeue -u username


While the job is waiting/pending, your queue may look like this:

.. code:: bash

   JOBID PARTITION     NAME     USER ST       TIME  NODES NODELIST(REASON)
   1234567     batch     job1 username PD       0:00      1 (Resources)

The ``ST`` column shows the status of the job.
``PD`` means pending.

When the job is running, the queue may look like this:

.. code:: bash

   JOBID PARTITION     NAME     USER ST       TIME  NODES NODELIST(REASON)
   1234567     batch     job1 username  R       0:01      1 node1


Usually my jobs run right away or within a few minutes of being submitted, but sometimes they can sit in the queue for a while.
This is usually because there are a lot of jobs in the queue, and the cluster is busy.

To check where all jobs submitted by all users are in the queue:

.. code:: bash

   squeue


Checking job run information
----------------------------

To check information about a specific job, a helpful command is (replace ``1234567`` with the actual job ID):

.. code:: bash

   scontrol show jobid 1234567

Your output will look something like this:

.. code:: bash

   JobId=404543 JobName=job.do
   UserId=mengm(32712) GroupId=mengm(60100) MCS_label=N/A
   Priority=10000 Nice=0 Account=default QOS=normal
   JobState=FAILED Reason=NonZeroExitCode Dependency=(null)
   Requeue=1 Restarts=0 BatchFlag=1 Reboot=0 ExitCode=1:0
   DerivedExitCode=0:0
   RunTime=00:00:11 TimeLimit=03:00:00 TimeMin=N/A
   SubmitTime=2025-01-22T05:56:31 EligibleTime=2025-01-22T05:56:31
   AccrueTime=2025-01-22T05:56:31
   StartTime=2025-01-22T05:56:35 EndTime=2025-01-22T05:56:46 Deadline=N/A
   PreemptEligibleTime=2025-01-22T05:56:35 PreemptTime=None
   SuspendTime=None SecsPreSuspend=0 LastSchedEval=2025-01-22T05:56:35 Scheduler=Backfill
   Partition=generic AllocNode:Sid=10.42.153.116:248
   ReqNodeList=(null) ExcNodeList=(null)
   NodeList=compute2
   BatchHost=compute2
   NumNodes=1 NumCPUs=1 NumTasks=1 CPUs/Task=1 ReqB:S:C:T=0:0:*:*
   ReqTRES=cpu=1,mem=40G,node=1,billing=1
   AllocTRES=cpu=1,mem=40G,node=1,billing=1
   Socks/Node=* NtasksPerN:B:S:C=0:0:*:* CoreSpec=*
   JOB_GRES=(null)
     Nodes=compute2 CPU_IDs=2 Mem=40960 GRES=
   MinCPUsNode=1 MinMemoryNode=40G MinTmpDiskNode=0
   Features=(null) DelayBoot=00:00:00
   OverSubscribe=OK Contiguous=0 Licenses=(null) Network=(null)
   Command=/home/mengm/job/message/job.do
   WorkDir=/home/mengm
   StdErr=/home/mengm/~/err/solve_%J.err
   StdIn=/dev/null
   StdOut=/home/mengm/~/out/solve_%J.out
   Power=
   MailUser=username@iiasa.ac.at MailType=BEGIN,END,FAIL

Here you see the job information, including submit time, the associated commands/files, and the output files.
Additionally, here you can see the resources requested and allocated for the job, such as number of nodes, CPUs, memory, etc.

The ``JobState`` will show the status of the job.
If it is ``FAILED``, the ``Reason`` will show why it failed.
The ``ExitCode`` will show the exit code of the job.
If it is ``0:0``, then the job ran successfully.
If it is ``1:0``, then the job failed.

When my job fails, I usually go ahead and check both the ``err`` and
``out`` files to see what happened.
The ``err`` file will show any errors or warnings that occurred during the job, and the ``out`` file will show any print statements or output from the Python script.

Another useful command to check recent jobs and their information is:

.. code:: bash

   sacct -l

However, this will show a lot of information, so it might be better to run a more specific command like:

.. code:: bash

   sacct --format=jobid,MaxRSS,MaxVMSize,start,end,CPUTimeRAW,NodeList

Resources to request for reducing MESSAGEix run time
----------------------------------------------------

The following information is based on non-scientific "testing" (goofing around), so take it with a grain of salt.
I have found that requesting more CPUs per task can help reduce the run time of a MESSAGEix solve.

For example, a MESSAGE job with ``#SBATCH --cpus-per-task=4`` took over 30 minutes to finish, whereas the same job with ``#SBATCH --cpus-per-task=16`` took about 20 minutes to finish.
I also tried changing ``#SBATCH --ntasks=1`` to ``#SBATCH --ntasks=4``, but that didn't seem to make a difference in run time.

So usually my ``SBATCH`` job request settings look like this:

.. code:: bash

    #SBATCH --time=20:00:00
    #SBATCH --mem=100G
    #SBATCH --nodes=1
    #SBATCH --ntasks=1
    #SBATCH --cpus-per-task=16

I usually request lots of run time (20 hours) and lots of memory (100 GB) because I don't want my job to fail for those reasons.

.. caution::
   Many users making such requests simultaneously is likely to worsen congestion on UnICC and make it less usable for all users.
   A better approach is to use one's own best estimates of the actual resource use, multiplied by a safety factor.

I keep ``--nodes=1`` because I don't know enough about running on multiple nodes, and I don't really do any parallel computing, so I don't think I need to request more than one node.

In general though I'm sure there are other settings people can play around with to optimize their job run time, including maybe on the CPLEX side for example, but I haven't really looked into that, and this is just what I've found so far.

Note on memory
--------------

If this is not specified, the default amount of memory that gets assigned to the job is 2GB.
I think more CPUs per job could also be requested instead, which would also give more memory (2 GB times the number of CPUs).
But instead, just request more memory.
I especially recommend this because if you're running legacy reporting, that requires a bit of memory, so your job might fail if
you don't request enough memory.

Changes
=======

2025-01-16
   Initial version of the guide by :gh-user:`measrainsey`.
