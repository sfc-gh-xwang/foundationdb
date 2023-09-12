import argparse
import itertools
import json
import os
import platform
import subprocess
import sys

from binary_download import FdbBinaryDownloader
from local_cluster import TLSConfig
from tmp_cluster import TempCluster


def options_valid(tls_on, authz_on, tenant_mode, tenant_count):
    # Authz shouldn't be set if tls is set false, and vice versa
    if tls_on != authz_on:
        return False

    # Currently restrict the tenant case to tls/authz enabled case
    return (tls_on and tenant_count > 0) or (not tls_on and tenant_count == 0)


if __name__ == "__main__":
    # Create an ArgumentParser instance
    parser = argparse.ArgumentParser(description="Parser for combify.py")

    # The build directory path is required
    parser.add_argument(
        "--build_dir", required=True, type=str, help="Build directory path."
    )

    # The combify binary path is required
    parser.add_argument(
        "--combify_tests_bin", required=True, type=str, help="Combify binary path."
    )

    # The following options allow users to pin values of specified options
    parser.add_argument(
        "--tls_option", required=False, type=bool, help="Enable/disable TLS"
    )
    parser.add_argument(
        "--authz_option", required=False, type=bool, help="Enable/disable AuthZ"
    )
    parser.add_argument(
        "--tenant_mode",
        required=False,
        type=str,
        choices=["disabled", "optional", "required"],
        help="Specify a tenant mode: disabled, optional, required",
    )
    parser.add_argument(
        "--tenant_count", required=False, type=int, help="Tenant counts"
    )
    parser.add_argument(
        "--local_client_lib_version",
        required=False,
        type=str,
        help="Used to specify version of FDB local client library. Only used for testing the shim-layer",
    )

    options = parser.parse_args()

    # tlsEnabled = [True, False]
    # authzEnabled = [True, False]
    # tenantModes = ["disabled", "optional", "required"]
    # tenantCounts = [0, 5]
    build_dir = options.build_dir
    tls_options = [False, True] if options.tls_option is None else [options.tls_option]
    authz_options = (
        [False, True] if options.authz_option is None else [options.authz_option]
    )
    tenant_modes = (
        ["optional"] if options.tenant_mode is None else [options.tenant_mode]
    )
    tenant_counts = [0, 3] if options.tenant_count is None else [options.tenant_count]
    test_ok = True
    binary_rel_path = "bin/fdb_c_combify_test"
    binary_abs_path = os.path.join(build_dir, binary_rel_path)
    if not os.path.isfile(binary_abs_path):
        print("Cannot find {} in build directory {}".format(binary_rel_path, build_dir))
        sys.exit(2)
    for [tls_on, authz_on, tenant_mode, tenant_count] in itertools.product(
        tls_options, authz_options, tenant_modes, tenant_counts
    ):
        # Skip if the combination of options is not valid
        if not options_valid(tls_on, authz_on, tenant_mode, tenant_count):
            continue

        with TempCluster(
            build_dir=build_dir,
            tls_config=TLSConfig(server_chain_len=2, client_chain_len=2)
            if tls_on
            else None,
            tenant_mode=tenant_mode,
            authorization_kty="EC" if authz_on else None,
            authorization_keypair_id="test_keyid" if authz_on else None,
            remove_at_exit=True,
        ) as cluster:
            test_config = {
                "clusterFilePath": str(cluster.cluster_file),
                "tenantMode": tenant_mode,
                "tenantCount": tenant_count,
            }
            if tls_on:
                test_config["tls"] = {
                    "caPath": str(cluster.server_ca_file),
                    "certPath": str(cluster.client_cert_file),
                    "keyPath": str(cluster.client_key_file),
                }
            if authz_on:
                test_config["authz"] = {
                    "privateKeyPath": str(cluster.authorization_private_key_pem_file),
                    "keyName": cluster.authorization_keypair_id,
                }
            iter_desc = f"tls:{tls_on} authz:{authz_on} tenant_mode:{tenant_mode} tenant_count:{tenant_count}"
            print("===============================")
            print(f"Iteration: {iter_desc}")
            print("Static Configuration:\n{}".format(json.dumps(test_config, indent=2)))
            env = os.environ.copy()
            libpath_env_key = "LD_LIBRARY_PATH"
            if platform.system() == "Darwin":
                libpath_env_key = "DY" + libpath_env_key
            env[libpath_env_key] = "{}:{}".format(
                env[libpath_env_key], os.path.join(build_dir, "lib")
            )
            if options.local_client_lib_version:
                print(
                    "FDB_LOCAL_CLIENT_LIBRARY_PATH: {}".format(
                        options.local_client_lib_version
                    )
                )
                downloader = FdbBinaryDownloader(options.build_dir)
                version = options.local_client_lib_version
                env["FDB_LOCAL_CLIENT_LIBRARY_PATH"] = downloader.lib_path(
                    version, external=False
                )

            this_run = subprocess.run(
                [
                    binary_abs_path,
                    json.dumps(test_config),
                ],
                stdout=sys.stdout,
                stderr=sys.stderr,
                env=env,
            )
            if this_run.returncode != 0:
                print(
                    f"!!! Iteration {iter_desc} failed with return code {this_run.returncode}"
                )
                test_ok = False
            else:
                print("Iteration OK")
    if test_ok:
        print("TEST OK")
    else:
        print("SOME ITERATION(S) FAILED")
    sys.exit(0 if test_ok else 1)
