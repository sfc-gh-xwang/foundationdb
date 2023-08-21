#!/usr/bin/env python3

#
# bulk_load.py
#
# Copyright (c) 2023 Snowflake Computing
#

import os
import subprocess
import sys

from cluster_args import CreateTmpFdbClusterArgParser
from local_cluster import TLSConfig
from tmp_cluster import TempCluster

if __name__ == "__main__":
    script_desc = """
    This script brings up a local cluster and tests bulkload fdbcli commands
    against it.
    """

    parser = CreateTmpFdbClusterArgParser(description=script_desc)
    args = parser.parse_args()
    errcode = 1

    try:
        # now start the cluster up
        cluster = TempCluster(
            build_dir=args.build_dir,
            tenant_mode="optional_experimental",
            tls_config=TLSConfig(server_chain_len=3, client_chain_len=2),
            authorization_kty="EC",
            authorization_keypair_id="test_keyid",
            blob_granules_enabled=True,
            custom_config={"knob_allow_bulk_loads": "true"},
            remove_at_exit=False,
        )
        cluster.__enter__()

        tenant_name = "test_tenant"
        cmd = "tenant create {}".format(tenant_name)
        print("fdb> {}".format(cmd))
        print(cluster.fdbcli_exec_and_get(cmd, timeout=10).decode())

        cmd = "usetenant {}; bulkload list".format(tenant_name)
        print("fdb> {}".format(cmd))
        bl_list = cluster.fdbcli_exec_and_get(cmd, timeout=10).decode().strip()
        print(bl_list)
        assert bl_list.find("No active bulk loads.") != -1

        cmd = "usetenant {}; bulkload start d e".format(tenant_name)
        print("fdb> {}".format(cmd))
        print(cluster.fdbcli_exec_and_get(cmd, timeout=10).decode())

        cmd = "usetenant {}; bulkload list".format(tenant_name)
        print("fdb> {}".format(cmd))
        bl_list = cluster.fdbcli_exec_and_get(cmd, timeout=10).decode().strip()
        print(bl_list)
        assert len(bl_list.split(". ")) == 2

        bulk_load_id = bl_list.split(". ")[1]
        cmd = "usetenant {}; bulkload status {}".format(tenant_name, bulk_load_id)
        print("fdb> {}".format(cmd))
        bl_status = cluster.fdbcli_exec_and_get(
            cmd,
            timeout=10,
        ).decode()
        print(bl_status)
        assert bl_status.find("Tenant: {}".format(tenant_name)) != -1

        cmd = "usetenant {}; bulkload stop {}".format(tenant_name, bulk_load_id)
        print("fdb> {}".format(cmd))
        print(
            cluster.fdbcli_exec_and_get(
                cmd,
                timeout=10,
            ).decode()
        )

        cmd = "usetenant {}; bulkload cleanup {}".format(tenant_name, bulk_load_id)
        print("fdb> {}".format(cmd))
        print(
            cluster.fdbcli_exec_and_get(
                cmd,
                timeout=10,
            ).decode()
        )
        errcode = 0
    finally:
        # shutdown the cluster
        cluster.close()

    sys.exit(errcode)
