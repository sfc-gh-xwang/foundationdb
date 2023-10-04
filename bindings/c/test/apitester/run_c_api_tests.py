#!/usr/bin/env python3
#
# run_c_api_tests.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import sys
import argparse
import os
from subprocess import Popen, TimeoutExpired
import logging
import signal
from pathlib import Path
import glob
import random
import string
import toml

# fmt: off
from tmp_cluster import TempCluster
from local_cluster import TLSConfig
from test_util import get_logger, initialize_logger_level, api_version_from_str
from binary_download import FdbBinaryDownloader
from fdb_version import CURRENT_VERSION
# fmt: on

TESTER_STATS_INTERVAL_SEC = 5

logger = get_logger()


def random_string(len):
    return "".join(
        random.choice(string.ascii_letters + string.digits) for i in range(len)
    )


def dump_client_logs(log_dir):
    for log_file in glob.glob(os.path.join(log_dir, "*")):
        logger.error(">>>>>>>>>>>>>>>>>>>> Contents of {}:".format(log_file))
        with open(log_file, "r") as f:
            logger.error(f.read())
        logger.error(">>>>>>>>>>>>>>>>>>>> End of {}:".format(log_file))


def run_tester(args, cluster, test_file):
    build_dir = Path(args.build_dir).resolve()
    tester_binary = Path(args.api_tester_bin).resolve()
    downloader = FdbBinaryDownloader(build_dir)
    log_dir = Path(cluster.log).joinpath("client")
    log_dir.mkdir(exist_ok=True)
    cmd = [
        tester_binary,
        "--cluster-file",
        cluster.cluster_file,
        "--test-file",
        test_file,
        "--stats-interval",
        str(TESTER_STATS_INTERVAL_SEC * 1000),
        "--tmp-dir",
        cluster.tmp_dir,
        "--log",
        "--log-dir",
        str(log_dir),
        "--verbosity",
        args.logging_level,
        "--external-client-library",
        downloader.lib_path(args.cluster_version, external=True),
        "--api-version",
        str(api_version_from_str(args.cluster_version)),
    ]

    if args.run_with_gdb:
        cmd = ["gdb", "--args"] + cmd

    if args.retain_client_lib_copies:
        cmd += ["--retain-client-lib-copies"]

    if cluster.blob_granules_enabled:
        cmd += [
            "--blob-granule-local-file-path",
            str(cluster.data.joinpath("fdbblob")) + os.sep,
        ]

    if cluster.tls_config is not None:
        cmd += [
            "--tls-ca-file",
            cluster.server_ca_file,
            "--tls-key-file",
            cluster.client_key_file,
            "--tls-cert-file",
            cluster.client_cert_file,
        ]
        if cluster.authorization_kty:
            # authorization is enabled
            cmd += [
                "--authz-private-key-pem-file",
                cluster.authorization_private_key_pem_file,
                "--authz-keypair-id",
                cluster.authorization_keypair_id,
            ]

    for knob in args.knobs:
        knob_name, knob_value = knob.split("=")
        cmd += ["--knob-" + knob_name, knob_value]

    env_vars = os.environ.copy()
    if args.primary_client_version is not None:
        env_vars["FDB_LOCAL_CLIENT_LIBRARY_PATH"] = downloader.lib_path(
            args.primary_client_version, external=False
        )

    logger.info("Running tester '%s'..." % " ".join(map(str, cmd)))
    proc = Popen(cmd, stdout=sys.stdout, stderr=sys.stderr, env=env_vars)
    timed_out = False
    ret_code = 1
    try:
        ret_code = proc.wait(args.timeout)
    except TimeoutExpired:
        proc.kill()
        timed_out = True
    except Exception as e:
        raise Exception("Unable to run tester (%s)" % e)

    if ret_code != 0:
        if timed_out:
            reason = "timed out after %d seconds" % args.timeout
        elif ret_code < 0:
            reason = signal.Signals(-ret_code).name
        else:
            reason = "exit code: %d" % ret_code
        logger.error("'%s' did not complete succesfully (%s)" % (cmd[0], reason))
        if log_dir is not None and not args.disable_log_dump:
            dump_client_logs(log_dir)
    return ret_code


class TestConfig:
    def __init__(self, test_file):
        config = toml.load(test_file)
        server_config = config.get("server", [{}])[0]
        self.tenant_mode = (
            "optional_experimental"
            if server_config.get("tenants_enabled", True)
            else None
        )
        self.blob_granules_enabled = server_config.get("blob_granules_enabled", False)
        self.enable_encryption_at_rest = server_config.get(
            "enable_encryption_at_rest", False
        )
        self.tls_enabled = server_config.get("tls_enabled", False)
        self.client_chain_len = server_config.get("tls_client_chain_len", 2)
        self.server_chain_len = server_config.get("tls_server_chain_len", 3)
        self.min_num_processes = server_config.get("min_num_processes", 1)
        self.max_num_processes = server_config.get("max_num_processes", 3)
        self.num_processes = random.randint(
            self.min_num_processes, self.max_num_processes
        )
        self.authz_enabled = server_config.get("authz_enabled", False)

        self.server_knobs = {
            name: value
            for name, value in server_config.items()
            if name.startswith("knob_")
        }


def run_test(args, test_file):
    config = TestConfig(test_file)

    tls_config = None
    if config.tls_enabled:
        tls_config = TLSConfig(
            server_chain_len=config.client_chain_len,
            client_chain_len=config.server_chain_len,
        )

    with TempCluster(
        args.build_dir,
        config.num_processes,
        tenant_mode=config.tenant_mode,
        blob_granules_enabled=config.blob_granules_enabled,
        enable_encryption_at_rest=config.enable_encryption_at_rest,
        tls_config=tls_config,
        custom_config=config.server_knobs,
        cluster_version=args.cluster_version,
        authorization_kty="EC" if config.authz_enabled else "",
        authorization_keypair_id="apitester-authz-keypair"
        if config.authz_enabled
        else "",
    ) as cluster:
        ret_code = run_tester(args, cluster, test_file)
        if not cluster.check_cluster_logs():
            ret_code = 1 if ret_code == 0 else ret_code
        return ret_code


def run_tests(args):
    num_failed = 0
    if args.test_file is not None:
        test_files = [Path(args.test_file).resolve()]
    else:
        test_files = [
            f
            for f in os.listdir(args.test_dir)
            if os.path.isfile(os.path.join(args.test_dir, f)) and f.endswith(".toml")
        ]

    for test_file in test_files:
        logger.info("=========================================================")
        logger.info("Running test %s" % test_file)
        logger.info("=========================================================")
        ret_code = run_test(args, os.path.join(args.test_dir, test_file))
        if ret_code != 0:
            num_failed += 1

    return num_failed


def parse_args(argv):
    parser = argparse.ArgumentParser(description="FoundationDB C API Tester")
    parser.add_argument(
        "--build-dir", "-b", type=str, required=True, help="FDB build directory"
    )
    parser.add_argument(
        "--api-tester-bin",
        type=str,
        help="Path to the fdb_c_api_tester executable.",
        required=True,
    )
    parser.add_argument(
        "--retain-client-lib-copies",
        action="store_true",
        default=False,
        help="Retain temporary external client library copies.",
    )
    parser.add_argument(
        "--cluster-file",
        type=str,
        default="fdb.cluster",
        help="The cluster file for the cluster being connected to. (default: fdb.cluster)",
    )
    parser.add_argument(
        "--test-dir",
        type=str,
        default="./",
        help="Path to a directory with test definitions. (default: ./)",
    )
    parser.add_argument(
        "--test-file",
        type=str,
        default=None,
        help="Path to a single test definition to be executed, overrides --test-dir if set.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="The timeout in seconds for running each individual test. (default 300)",
    )
    parser.add_argument(
        "--logging-level",
        type=str,
        default="INFO",
        choices=["ERROR", "WARNING", "INFO", "DEBUG"],
        help="Specifies the level of detail in the tester output (default='INFO').",
    )
    parser.add_argument(
        "--knob",
        type=str,
        default=[],
        action="append",
        dest="knobs",
        help="[lowercase-knob-name]=[knob-value] (there may be multiple --knob options)",
    )
    parser.add_argument(
        "--disable-log-dump",
        help="Do not dump logs on error",
        action="store_true",
    )
    parser.add_argument(
        "--run-with-gdb", help="Execute the tester binary from gdb", action="store_true"
    )
    parser.add_argument(
        "--primary-client-version",
        type=str,
        default=None,
        help="Primary FDB client version for shim library tests",
    )
    parser.add_argument(
        "--cluster-version",
        type=str,
        default=CURRENT_VERSION,
        help="FDB cluster version to be used for testing",
    )

    return parser.parse_args(argv)


def main(argv):
    args = parse_args(argv)
    initialize_logger_level(args.logging_level)
    return run_tests(args)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
