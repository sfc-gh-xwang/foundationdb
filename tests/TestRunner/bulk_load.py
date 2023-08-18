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
        cluster.fdbcli_exec("tenant create {}".format(tenant_name), timeout=10)

        bl_list = cluster.fdbcli_exec_and_get(
            "usetenant {}; bulkload list".format(tenant_name), timeout=10
        ).strip()
        print(bl_list)
        assert bl_list.find(b"No active bulk loads.") != -1
        print(
            cluster.fdbcli_exec_and_get(
                "usetenant {}; bulkload start d e".format(tenant_name), timeout=10
            )
        )
        bl_list = (
            cluster.fdbcli_exec_and_get(
                "usetenant {}; bulkload list".format(tenant_name), timeout=10
            )
            .strip()
            .split(b". ")
        )
        assert len(bl_list) == 2
        print(bl_list)
        bulk_load_id = bl_list[1].decode("raw_unicode_escape")
        bl_status = cluster.fdbcli_exec_and_get(
            "usetenant {}; bulkload status {}".format(tenant_name, bulk_load_id),
            timeout=10,
        )
        print(bl_status)
        assert (
            bl_status.find(bytes("Tenant: {}".format(tenant_name), encoding="utf8"))
            != -1
        )
        cluster.fdbcli_exec(
            "usetenant {}; bulkload stop {}".format(tenant_name, bulk_load_id),
            timeout=10,
        )
        cluster.fdbcli_exec(
            "usetenant {}; bulkload cleanup {}".format(tenant_name, bulk_load_id),
            timeout=10,
        )
        errcode = 0
    finally:
        # shutdown the cluster
        cluster.close()

    sys.exit(errcode)
