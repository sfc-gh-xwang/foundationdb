# Stateless Process Colocation

Stateless process colocation is a modification to the FDB recruitment algorithm
to make it latency aware. When stateless process colocation is enabled, it will
prefer to colocate all stateless roles on a single machine, or in the same data
hall and zone as a fallback.

Stateless process colocation is disabled by default. It can be enabled by
setting the knob `COLOCATE_STATELESS_PROCESSES` to `true`.

## Background

An FDB cluster consists of many `fdbserver` processes. Each process is capable
of running zero or more roles, as directed by the cluster controller. Some
examples of roles are commit proxies, resolvers, or transaction logs. The
assignment of roles to processes is called recruitment, and occurs whenever the
cluster goes through a recovery. Recruitment is run on the cluster controller,
which has a global view of all processes in the system. The recruitment
algorithm takes many factors into account when deciding where to place roles,
including fitness levels for each process, the number of roles already running
on the process, whether the process is degraded, and more. The system also
regularly checks the current recruitment of roles, and will trigger a recovery
if it believes a more efficient recruitment is available. Importantly, the
system does not consider physical location when running recruitment, and
introduces some level of randomness, such that two recruitments may place roles
on entirely different machines. This can lead to changes in the performance
characteristics of the system when roles move around without regard for
communication latency.

Today, we only run FDB in single region mode. However, within a region we
distribute processes across fault domains, or availabilty zones (AZ). These
availability zones are geographically distributed such that the failure of one
AZ should be unilkely to affect another. Because of this distribution, messages
between AZs also take longer. Recent measurements taken in AWS's `us-west-2`
region returned the following latency numbers:

| Message              |  Time  |
|----------------------|--------|
| US-Europe roundtrip  | 150 ms |
| Cross-AZ roundtrip   | 800 us |
| Intra-AZ roundtrip   | 100 us |
| Loopback roundtrip   | 15 us  |
| Core-to-core latency | << 1us |

Transactions in FDB run through many different roles as part of the commit
process. A typical FDB transaction includes at least eight messages sent
between roles (excluding client communication), and potentially more depending
on how many resolvers and transaction logs are required for the commit. If each
message between roles occurs across availability zones, significant time may be
added to the overall commit duration.

## Overview

Stateless process colocation modifies recruitment to prefer recruiting all
stateless roles on a single machine if possible, and within an AZ if not. As
seen in the table above, communication between processes on the same machine
(`Loopback roundtrip`) is two orders of magnitude faster than cross-AZ
communication. Multiplied across many roundtrips, stateless process colocation
can significantly speed up commits.

Today, stateless process colocation is disabled by default. It can be enabled by
setting the knob `COLOCATE_STATELESS_PROCESSES` to `true`.

## Design

Stateless process colocation makes some assumptions about the configuration of
an FDB cluster is in running on. Whereas the existing recruitment algorithm is
designed to be flexible, stateless process colocation assumes a specific FDB
configuration to maximize the benefits.

### Production Configuration

To see the largest latency benefits, an FDB cluster with stateless process
colocation enabled should have at least one machine running enough stateless
processes to recruit all stateless roles and all singletons. Ideally, it should
have three such machines, one per AZ, to fall back on in case of failure. In
order to calculate the number of stateless processes required, sum the
stateless roles configured for your deployment:

  * GRV proxies
  * Commit proxies
  * Resolvers

Then, add seven to cover the:

  * Sequencer
  * Six singletons
  * Data Distributor
  * Ratekeeper
  * Blob Manager
  * Blob Migrator
  * Encrypt Key Proxy
  * Consistency Scanner

Then, double the result to cover additional growth and configuration changes
and provide some breathing room.

To ensure performance, make sure the stateless machines have enough cores to
allow each process its own dedicated core.

#### Example

For example, in a system with:

  * Two GRV proxies
  * Six commit proxies
  * Three resolvers

Each large stateless machine should be running 11 (2 + 6 + 3 + 1 stateless
roles, don't forget the sequencer!) + 6 (singletons) = 17 stateless processes.
Double it be safe, and around 32 stateless processes should be sufficient.

### Fallback

Today, our typical production configurations include a large number of machines
which each run a small number of stateless processes. For example, our
`xxlarge` t-shirt size uses 18 general instance machines, each running two
stateless processes, and 66 storage instance machines, each running one
stateless process. These stateless processes run on spare cores on these
machines whose true purpose is to run storage server processes. Stateless
process colocation can be enabled without removing these additional stateless
processes. The updated recruitment algorithm will always prefer recruitment on
machines with many stateless processes. Given the choice of a machine with 32
stateless processes and one with 2 stateless processes, recruitment will always
select the 32 process machine to recruit on first, falling back to other
machines only if necessary. So, continuing to run a small number of stateless
roles on storage machines should not negatively affect the system, and may be
useful in certain failure scenarios.

One issue with colocating stateless processes is that a single machine failure
can now take out all stateless roles. This is not too different from before,
where a failure of any stateless role would also trigger a recovery, but with
stateless process colocation, it is preferable to have a fallback machine to
avoid performance changes. Therefore, clusters with stateless process
colocation enabled should run three large stateless machines, one per AZ.
Typically, only one of these machines will be running stateless roles, with the
other two idle, but in case of failure of the primary, either of the other two
machines could take over with minimal performance changes.

## Algorithm Details

Details of the updated recruitment algorithm when the
`COLOCATE_STATELESS_PROCESSES` knob is enabled are provided in this section.

Stateless process colocation only modifies recruitment for the following roles:

  * Sequencer
  * GRV proxy
  * Commit proxy
  * Resolver

The simplest way to think about the updated algorithm is that it prefers
recruitment in descending order of process count on machines. For example, a
machine with 32 processes will always be selected for recruitment ahead of a
machine with 31 processes.

In more detail, the algorithm works as follows:

  1. Create a count of the number of processes on each machine.
      * This count is somewhat naive in that it does not take process class into
        account. So a machine with 10 storage processes and 10 stateless
        processes wolud be considered ahead of a machine with 15 stateless
        processes.
  2. Recruit the sequencer on the machine with the highest number of processes.
      * Note that the typical recruitment logic is still considered here. For
        example, a process specifically asking to be the sequencer will still
        be recruited ahead of stateless processes, even if the process count is
        lower.
  3. Recruit the rest of the stateless roles, taking fitness and previous usage
     into account, as well as locality with the sequencer. That is, the
     remaining stateless roles will prefer to be recruited on the same machine
     as the sequencer. If that is not possible, they will prefer the same data
     hall, and then the same zone, in that order.

## Testing

Specific stateless process colocation recruitment and failure scenarios are
tested in `RecruiterUnitTests.actor.cpp` under the
`/fdbserver/Recruiter/Colocate/` test case prefix.
