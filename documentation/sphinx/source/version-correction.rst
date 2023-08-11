.. _version-correction:
.. default-domain:: bash
.. highlight:: bash

##################
Version Correction
##################

Version correction is a modification to how FDB calculates versions. It
attempts to keep the version FDB assigns to transactions consistent relative to
some base epoch, by speeding up or slowing down the rate at which versions are
calculated.

Version correction is disabled by default. See
:ref:`version-correction-production` for instructions on how to enable it.

Background
==========

All reads and writes in FDB occur at a version. Versions are handed out by a
singleton called the sequencer. When a new FDB cluster is created, the
sequencer hands out versions beginning at 0. Internally, the sequencer
continuously advances the version, at a rate of one million versions per
second. In this regard, the current version can be thought of as the uptime of
the cluster in microseconds. However, this is not quite true; other factors can
affect the version. For example, a recovery in FDB advances the version by
100,000,000, as a way to signal to other processes that a recovery has
occurred. Therefore, relying on FDB versions as a timestamp (prior to version
correction) was incorrect.

Overview
========

Version correction modifies how versions are calculated in two ways. First,
versions no longer need to have the zero epoch at cluster creation time.
Second, the rate at which versions are handed out is automatically adjusted to
keep them consistent with the epoch.

The original motivation for version correction was to enable different FDB
clusters to run with consistent versions. This is important for the Metacluster
project in order to enable movement of tenants between clusters. However,
version correction also enables other applications of FDB versions. Namely,
with version correction, versions can be roughly correlated to a timestamp and
vice versa.

.. _version-correction-epoch:

Epoch
-----

The first change version correction makes is to allow the user to set a base
epoch for versions. This is a timestamp that represents when the 0 version
occurred. The timestamp itself is based off the Unix epoch, and should be
calculated using the same time zone as each server uses to determine time. For
example, setting the base epoch to 0 means a cluster created on January 1, 2023
00:00:00+00:00 would start with a version of 1,672,531,200,000,000.

The base epoch can be set to a positive value to represent a date after January
1, 1970, or to a negative value to represent a date before January 1, 1970.

To set the epoch, use ``fdbcli``:

.. code-block:: bash

  fdb> versionepoch set 10000000
  
  or
  
  fdb> versionepoch enable  # sets the version epoch to the default value of 0

Note that ``versionepoch enable`` is equivalent to ``versionepoch set 0``.

Version Rate
------------

The second change version correction makes is to alter the rate at which
versions are calculated in an attempt to match the current version to the
number of microseconds since the configured epoch. This is necessary because
the version can jump due to recoveries, manual user action (see
:ref:`fdbcli-advanceversion`), or pausing of the cluster. FDB dynamically
modifies the version rate by calculating the offset of the actual version from
the expected version and increasing or decreasing the version within a clamped
interval. This is done in the ``figureVersion`` function.

Caveats
-------

The above two changes mean FDB versions become representative of time. However,
the user must take care to factor in an error bound. Immediately after a
recovery, the version will be off by 100 seconds and will take some time to
recover. If multiple FDB recoveries happen in a short time interval, the
version can become greatly elevated relative to time.


.. _version-correction-production:

Production
==========

Version correction is disabled by default. To enable version correction, use
``fdbcli`` on an existing cluster:

.. code-block:: bash

  fdb> versionepoch enable
  fdb> versionepoch commit

Because enabling version correction will typically involve irrevocably
advancing the existing cluster version, it is split into two commands.

The first command is used to set the version epoch (see :ref:`version-correction-epoch`).

The second command commits the change by performing a one time jump of the
version to the expected version. If the new expected version is smaller than
the current version, this command is a no-op.

The version jump is considered irrevocable because it will typically involve
jumping by more than one quadrillion versions (10\ :sup:`15`). This occurs because the
default version epoch is more than 50 years in the past. While version
correction could theoretically correct a version offset of one quadrillion, it
would take so much time as to be infeasible.

Some other useful ``fdbcli`` commands include:

.. code-block:: bash

  fdb> versionepoch          # prints information on the actual vs expected FDB versions
  fdb> versionepoch get      # prints the version epoch if enabled
  fdb> versionepoch disable  # unsets the version epoch. Does not modify the version
