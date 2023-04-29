import itertools
from tmp_cluster import TempCluster
from local_cluster import TLSConfig
import sys
import json
import os
import platform
import subprocess

if __name__ == "__main__":
    assert len(sys.argv) == 2
    # tlsEnabled = [True, False]
    # authzEnabled = [True, False]
    # tenantModes = ["disabled", "optional_experimental", "required_experimental"]
    # tenantCounts = [0, 5]
    build_dir = sys.argv[1]
    tls_options = [True]
    authz_options = [True]
    tenant_modes = ["optional_experimental"]
    tenant_counts = [3]
    test_ok = True
    for [tls_on, authz_on, tenant_mode, tenant_count] in itertools.product(
        tls_options, authz_options, tenant_modes, tenant_counts
    ):
        with TempCluster(
            build_dir=build_dir,
            tls_config=TLSConfig(server_chain_len=2, client_chain_len=2)
            if tls_on
            else None,
            tenant_mode=tenant_mode,
            authorization_kty="EC" if authz_on else None,
            authorization_keypair_id="test_keyid" if authz_on else None,
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
            iter_desc = f"TLS:{tls_on} AuthZ:{authz_on} TenantMode:{tenant_mode} TenantCount:{tenant_count}"
            print("===============================")
            print(f"Iteration: {iter_desc}")
            print("Static Configuration:\n{}".format(json.dumps(test_config, indent=2)))
            env = os.environ.copy()
            libpath_env_key = "LD_LIBRARY_PATH"
            if platform.system() == "Darwin":
                libpath_env_key = "DY" + libpath_env_key
            env[libpath_env_key] = os.path.join(build_dir, "lib")
            this_run = subprocess.run(
                [
                    os.path.join(build_dir, "bin/fdb_c_combinatorial_test"),
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
