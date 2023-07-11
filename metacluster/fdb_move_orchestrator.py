#!/opt/rh/rh-python36/root/bin/python3
import base64
import fdb
import sys
import argparse
from fdb.tuple import pack, unpack
from fdb.impl import StreamingMode
import os
import json
import subprocess
import time
import uuid
from collections import defaultdict
import multiprocessing
from datetime import datetime

fdb.api_version(710300)

# Reserved keyrange on management cluster to store info for data movement
moveDataPrefix = "emergency_movement"

rangeLimit = 100000
timeoutLen = 60000  # in milliseconds
tmpSrcClusterFileName = "tmp_src_fdb.cluster"
tmpDstClusterFileName = "tmp_dst_fdb.cluster"


# Don't read everything into memory, only read a certain size, flush through commits then read some more
@fdb.transactional
def readSrcRange(tr, begin, end, throttlingTag):
    tr.options.set_lock_aware()
    tr.options.set_auto_throttle_tag(throttlingTag)
    readRange = tr.get_range(begin, end, 0, False, StreamingMode.want_all)
    return list(readRange)


# Returns the index of the last kv pair NOT set in this transaction
@fdb.transactional
def writeDstRange(tr, keyRange, throttlingTag):
    tr.options.set_lock_aware()
    tr.options.set_auto_throttle_tag(throttlingTag)
    sizeLimit = pow(10, 5)
    totalSize = 0
    index = 0
    for k, v in keyRange:
        totalSize += len(k) + len(v)
        if totalSize >= sizeLimit:
            break
        tr.set(k, v)
        index += 1
    return index


def copyData(
    srcClusterFile,
    destClusterFile,
    srcTenantName,
    dstTenantName,
    begin,
    end,
    throttlingTag,
):
    if not isinstance(begin, bytes):
        begin = begin.encode("utf-8")
    if not isinstance(end, bytes):
        end = end.encode("utf-8")

    if not isinstance(srcTenantName, bytes):
        srcTenantName = srcTenantName.encode("utf-8")
    if not isinstance(dstTenantName, bytes):
        dstTenantName = dstTenantName.encode("utf-8")
    # print(
    #     "Copying data from `{}' to `{}' for range `{}' to `{}'".format(
    #         srcTenantName, dstTenantName, begin, end
    #     )
    # )

    srcDb = fdb.open(srcClusterFile)
    srcDb.options.set_transaction_timeout(timeoutLen)

    dstDb = fdb.open(destClusterFile)
    dstDb.options.set_transaction_timeout(timeoutLen)

    srcTenant = srcDb.open_tenant(srcTenantName)
    dstTenant = dstDb.open_tenant(dstTenantName)

    while True:
        try:
            readRange = readSrcRange(srcTenant, begin, end, throttlingTag)
            break
        except Exception as e:
            print(
                "Exception while reading from {} for range {} - {}: {}".format(
                    srcTenantName, begin, end, str(e)
                )
            )
            None

    totalKeys = len(readRange)
    while len(readRange):
        while True:
            try:
                cutoffIndex = writeDstRange(dstTenant, readRange, throttlingTag)
                # print("Wrote destination range: {} - {}".format(readRange[0], readRange[cutoffIndex - 1]))
                readRange = readRange[cutoffIndex:]
                break
            except Exception as e:
                print(
                    "Exception while writing to {} for range {} - {}: {}".format(
                        dstTenantName, begin, end, str(e)
                    )
                )
                None
    print(
        "Wrote full destination range: {} - {} for tenant {}. Total Keys: {}".format(
            begin, end, dstTenantName, totalKeys
        )
    )


def run_fdbcli_command_mgmt(*args):
    """run the fdbcli statement on the management cluster: fdbcli --exec '<arg1> <arg2> ... <argN>'.

    Returns:
        string: Console output from fdbcli
    """
    commands = command_template_mgmt + ["{}".format(" ".join(args))]
    print("Management Cluster FDBCLI Commands: {}".format(commands))
    process = subprocess.run(
        commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=fdbcli_env
    )
    output = process.stdout.decode("utf-8").strip()
    error = process.stderr.decode("utf-8").strip()
    if error:
        print("Command Failed: {}".format(error))
        exit(0)
    print("Management Cluster FDBCLI Output: {}".format(output))
    return output


@fdb.transactional
def get_move_record(tr, tenantGroup):
    if not isinstance(tenantGroup, bytes):
        tenantGroup = tenantGroup.encode("utf-8")
    tr.options.set_raw_access()
    packKey = fdb.tuple.pack(
        (tenantGroup,), (moveDataPrefix + "/move/").encode("utf-8")
    )
    result = tr.get(packKey)
    moveTuple = fdb.tuple.unpack(result)
    return moveTuple


@fdb.transactional
def get_next_tenant(tr, tenantGroup, runId, currentTenant):
    if not isinstance(tenantGroup, bytes):
        tenantGroup = tenantGroup.encode("utf-8")
    tr.options.set_raw_access()
    packKeyBegin = fdb.tuple.pack(
        (tenantGroup, runId, currentTenant, "\xff"),
        (moveDataPrefix + "/split_points/").encode("utf-8"),
    )
    packKeyEnd = fdb.tuple.pack(
        (tenantGroup, runId, "\xff"),
        (moveDataPrefix + "/split_points/").encode("utf-8"),
    )
    readRange = tr.get_range(packKeyBegin, packKeyEnd, 2).to_list()
    if len(readRange) == 2:
        nextSplitPoint = readRange[1].key
        # By convention, tenant name is 3rd element in the tuple
        nextTuple = fdb.tuple.unpack(
            nextSplitPoint, len(moveDataPrefix + "/split_points/")
        )
        nextTenant = nextTuple[2]
        return nextTenant
    return None


@fdb.transactional
def get_queue_head(tr, tenantGroup, runId):
    if not isinstance(tenantGroup, bytes):
        tenantGroup = tenantGroup.encode("utf-8")
    tr.options.set_raw_access()
    packKey = fdb.tuple.pack(
        (tenantGroup, runId), (moveDataPrefix + "/queue/").encode("utf-8")
    )
    result = tr.get(packKey)
    if result == None:
        return None
    return fdb.tuple.unpack(result)


@fdb.transactional
def set_queue_head(tr, tenantGroup, runId, tenantName, key):
    if not isinstance(tenantGroup, bytes):
        tenantGroup = tenantGroup.encode("utf-8")
    if not isinstance(key, bytes):
        key = key.encode("utf-8")
    tr.options.set_raw_access()
    packKey = fdb.tuple.pack(
        (tenantGroup, runId), (moveDataPrefix + "/queue/").encode("utf-8")
    )
    packValue = fdb.tuple.pack((tenantName, key))
    tr.set(packKey, packValue)


@fdb.transactional
def clear_queue_head(tr, tenantGroup, runId):
    if not isinstance(tenantGroup, bytes):
        tenantGroup = tenantGroup.encode("utf-8")
    tr.options.set_raw_access()
    packKey = fdb.tuple.pack(
        (tenantGroup, runId), (moveDataPrefix + "/queue/").encode("utf-8")
    )
    tr.clear(packKey)


# return nextTenant, currentTenant, begin, end
@fdb.transactional
def get_and_update_queue_head(tr, tenantGroup, runId):
    if not isinstance(tenantGroup, bytes):
        tenantGroup = tenantGroup.encode("utf-8")
    queue_tuple = get_queue_head(tr, tenantGroup, runId)
    if queue_tuple == None:
        return None, None, None, None
    tenantName = queue_tuple[0]
    begin = queue_tuple[1]
    end = get_stored_split_point(tr, tenantGroup, runId, tenantName, begin)

    # Reached last split point for current tenant. Update the queue with the next tenant
    if end == b"\xff":
        nextTenant = get_next_tenant(tr, tenantGroup, runId, tenantName)
        if nextTenant == None:
            return None, tenantName, begin, end
        set_queue_head(tr, tenantGroup, runId, nextTenant, b"")
        return nextTenant, tenantName, begin, end
    set_queue_head(tr, tenantGroup, runId, tenantName, end)
    return tenantName, tenantName, begin, end


@fdb.transactional
def get_stored_split_point(tr, tenantGroup, runId, tenantName, key):
    if not isinstance(tenantGroup, bytes):
        tenantGroup = tenantGroup.encode("utf-8")
    tr.options.set_raw_access()
    packKey = fdb.tuple.pack(
        (tenantGroup, runId, tenantName, key),
        (moveDataPrefix + "/split_points/").encode("utf-8"),
    )
    result = tr.get(packKey)
    if result == None:
        return b"\xff"
    return fdb.tuple.unpack(result)[0]


@fdb.transactional
def clear_stored_split_point(tr, tenantGroup, runId, tenantName, key):
    if not isinstance(tenantGroup, bytes):
        tenantGroup = tenantGroup.encode("utf-8")
    tr.options.set_raw_access()
    packKey = fdb.tuple.pack(
        (tenantGroup, runId, tenantName, key),
        (moveDataPrefix + "/split_points/").encode("utf-8"),
    )
    tr.clear(packKey)


def process_queue(mgmtDbName, tenantGroup):
    print("Process Queue Started")
    mgmtDb = fdb.open(mgmtDbName)
    mgmtDb.options.set_transaction_timeout(timeoutLen)
    moveRecord = get_move_record(mgmtDb, args.tenant_group)
    runId = moveRecord[0]
    while True:
        nextTenant, currentTenant, begin, end = get_and_update_queue_head(
            mgmtDb, tenantGroup, runId
        )
        if begin is None and end is None:
            break
        copyData(
            tmpSrcClusterFileName,
            tmpDstClusterFileName,
            currentTenant,
            currentTenant,  # dst and src have the same name
            begin,
            end,
            tenantGroup,
        )
        clear_stored_split_point(mgmtDb, tenantGroup, runId, currentTenant, begin)
        if nextTenant == None:
            clear_queue_head(mgmtDb, tenantGroup, runId)
            break
    print("Process Queue Finished")
    return 0


if __name__ == "__main__":
    # Arguments should be in double quotes to preserve escape characters for tenant group and src/dst name
    parser = argparse.ArgumentParser(description="Orchestrates tenant movement")
    parser.add_argument(
        "--cluster",
        help="Location of the cluster file for the management cluster",
        required=True,
        type=str,
    )
    parser.add_argument(
        "--tenant-group",
        help="Name of the tenant group being moved",
        required=True,
        type=str,
    )
    parser.add_argument(
        "--src-name",
        help="Name of the cluster the data is being moved away from",
        required=True,
        type=str,
    )
    parser.add_argument(
        "--dst-name",
        help="Name of the cluster the data is being moved to",
        required=True,
        type=str,
    )
    parser.add_argument(
        "--num-procs",
        help="Number of processes desired to run concurrently for faster data movement",
        required=True,
        type=int,
    )

    args = parser.parse_args()

    # validate cluster file exists
    if not os.path.exists(args.cluster):
        print("The management cluster file does not exist or is inaccessible.")
        exit(0)
    # keep current environment variables
    fdbcli_env = os.environ.copy()

    # TODO: change this to where the fdbcli binary is expected to be
    #   Also add cert files and other args needed for AuthZ
    # mgmt fdbcli command template
    command_template_mgmt = [
        "/home/jfu/fdbcli",
        "-C",
        args.cluster,
        "--exec",
    ]

    # Check fdbcli functions as expected
    try:
        srcJson = json.loads(
            run_fdbcli_command_mgmt("metacluster get {} JSON".format(args.src_name))
        )
        dstJson = json.loads(
            run_fdbcli_command_mgmt("metacluster get {} JSON".format(args.dst_name))
        )
    except Exception as e:
        print(
            "Error invoking fdbcli on management cluster. Double check that the cluster file is correctly formatted."
        )
        print(e)
        exit(0)

    if "cluster" not in srcJson or "connection_string" not in srcJson["cluster"]:
        print("Error getting info for data cluster {}. Output:".format(args.src_name))
        print(srcJson)
        exit(0)

    if "cluster" not in dstJson or "connection_string" not in dstJson["cluster"]:
        print("Error getting info for data cluster {}. Output:".format(args.dst_name))
        print(dstJson)
        exit(0)

    # Create temporary cluster files to connect to src/dst clusters
    srcConnString = srcJson["cluster"]["connection_string"]
    dstConnString = dstJson["cluster"]["connection_string"]
    try:
        tmpSrcClusterFile = open(tmpSrcClusterFileName, "w")
        tmpDstClusterFile = open(tmpDstClusterFileName, "w")

        tmpSrcClusterFile.write(srcConnString)
        tmpDstClusterFile.write(dstConnString)

        tmpSrcClusterFile.close()
        tmpDstClusterFile.close()
    except Exception as e:
        print("Error creating temporary cluster files.")
        print(e)
        exit(0)

    poolArgs = (
        args.cluster,
        args.tenant_group,
    )
    multiArgs = [poolArgs for i in range(args.num_procs)]
    start = datetime.now()
    # TODO: handle fdb error and maybe retry if split points are left over
    with multiprocessing.Pool(processes=args.num_procs) as pool:
        pool.starmap(process_queue, multiArgs)
    print("Time Elapsed for Copy: ", (datetime.now() - start).total_seconds())
