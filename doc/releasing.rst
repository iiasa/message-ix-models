Releasing
*********

Version numbers
===============

:mod:`message_ix_models` uses date-based version numbers like ``Y.M.D``.
Thus version ``2021.2.23`` is released on 23 February 2021.
This is to establish a more direct correspondence between outputs of the code and the version(s) used to produce it.

Procedure
=========

Before releasing, check:

- https://github.com/iiasa/message-ix-models/actions?query=branch:main to ensure that the push and scheduled builds are passing.
- https://readthedocs.com/projects/iiasa-energy-program-message-ix-models/builds/ to ensure that the docs build is passing.

Address any failures before releasing.

1. Edit :file:`doc/whatsnew.rst`.
   Comment the heading "Next release", then insert another heading below it, at the same level, with the version number and date.
   Make a commit with a message like "Mark vX.Y.Z in doc/whatsnew".

2. Tag the release candidate version, i.e. with a ``rcN`` suffix, and push::

    $ git tag v1.2.3rc1
    $ git push --tags origin main

3. Check:

   - at https://github.com/iiasa/message-ix-models/actions?query=workflow:publish that the workflow completes: the package builds successfully and is published to TestPyPI.
   - at https://test.pypi.org/project/message-ix-models/ that:

      - The package can be downloaded, installed and run.
      - The README is rendered correctly.

   Address any warnings or errors that appear.
   If needed, make a new commit and go back to step (2), incrementing the rc number.

4. (optional) Tag the release itself and push::

    $ git tag v1.2.3
    $ git push --tags origin main

   This step (but *not* step (2)) can also be performed directly on GitHub; see (5), next.

5. Visit https://github.com/iiasa/message-ix-models/releases and mark the new release: either using the pushed tag from (4), or by creating the tag and release simultaneously.

6. Check at https://github.com/iiasa/message-ix-models/actions?query=workflow:publish and https://pypi.org/project/message-ix-models/ that the distributions are published.
