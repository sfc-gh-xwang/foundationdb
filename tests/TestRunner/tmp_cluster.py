#!/usr/bin/env python3

import glob
import logging
import os
import shutil
import subprocess
import sys
import typing
from pathlib import Path

from cluster_args import CreateTmpFdbClusterArgParser
from local_cluster import LocalCluster, TLSConfig
from test_util import random_alphanum_string, get_logger
from binary_download import FdbBinaryDownloader
from fdb_version import CURRENT_VERSION

logger = get_logger()


class TempCluster(LocalCluster):
    def __init__(
        self,
        build_dir: str,
        process_number: int = 1,
        port: str = None,
        blob_granules_enabled: bool = False,
        tls_config: TLSConfig = None,
        authorization_kty: str = "",
        authorization_keypair_id: str = "",
        remove_at_exit: bool = True,
        custom_config: dict = {},
        tenant_mode: typing.Union[str, None] = "optional_experimental",
        enable_encryption_at_rest: bool = False,
        cluster_version=CURRENT_VERSION,
    ):
        self.build_dir = Path(build_dir).resolve()
        assert self.build_dir.exists(), "{} does not exist".format(build_dir)
        assert self.build_dir.is_dir(), "{} is not a directory".format(build_dir)
        tmp_dir = self.build_dir.joinpath("tmp", random_alphanum_string(16))
        tmp_dir.mkdir(parents=True)
        self.tmp_dir = tmp_dir
        self.remove_at_exit = remove_at_exit
        self.tenant_mode = tenant_mode
        self.enable_encryption_at_rest = enable_encryption_at_rest
        self.cluster_version = cluster_version
        self.downloader = FdbBinaryDownloader(self.build_dir)
        super().__init__(
            tmp_dir,
            self.downloader.binary_path(self.cluster_version, "fdbserver"),
            self.downloader.binary_path(self.cluster_version, "fdbmonitor"),
            self.downloader.binary_path(self.cluster_version, "fdbcli"),
            process_number,
            port=port,
            blob_granules_enabled=blob_granules_enabled,
            enable_encryption_at_rest=enable_encryption_at_rest,
            tls_config=tls_config,
            mkcert_binary=self.build_dir.joinpath("bin", "mkcert"),
            authorization_kty=authorization_kty,
            authorization_keypair_id=authorization_keypair_id,
            custom_config=custom_config,
        )
        self.set_env_var(
            "LD_LIBRARY_PATH",
            "%s:%s"
            % (self.downloader.lib_dir(cluster_version), os.getenv("LD_LIBRARY_PATH")),
        )

    def __enter__(self):
        super().__enter__()
        super().create_database(tenant_mode=self.tenant_mode)
        return self

    def __exit__(self, xc_type, exc_value, traceback):
        super().__exit__(xc_type, exc_value, traceback)
        if self.remove_at_exit:
            shutil.rmtree(self.tmp_dir)

    def close(self):
        super().__exit__(None, None, None)
        if self.remove_at_exit:
            shutil.rmtree(self.tmp_dir)


if __name__ == "__main__":
    script_desc = """
    This script automatically configures a temporary local cluster on the machine
    and then calls a command while this cluster is running. As soon as the command
    returns, the configured cluster is killed and all generated data is deleted.
    This is useful for testing: if a test needs access to a fresh fdb cluster, one
    can simply pass the test command to this script.

    The command to run after the cluster started. Before the command is executed,
    the following arguments will be preprocessed:
    - All occurrences of @CLUSTER_FILE@ will be replaced with the path to the generated cluster file.
    - All occurrences of @DATA_DIR@ will be replaced with the path to the data directory.
    - All occurrences of @LOG_DIR@ will be replaced with the path to the log directory.
    - All occurrences of @ETC_DIR@ will be replaced with the path to the configuration directory.

    The environment variable FDB_CLUSTER_FILE is set to the generated cluster for the command if it is not set already.
    """
    parser = CreateTmpFdbClusterArgParser(description=script_desc)

    parser.add_argument("cmd", metavar="COMMAND", nargs="+", help="The command to run")

    parser.add_argument(
        "--disable-log-dump",
        help="Do not dump cluster log on error",
        action="store_true",
    )

    args = parser.parse_args()
    custom_config = {}

    if args.disable_tenants:
        enable_tenants = False
    else:
        enable_tenants = True

    tls_config = None
    if args.tls_enabled:
        tls_config = TLSConfig(
            server_chain_len=args.server_cert_chain_len,
            client_chain_len=args.client_cert_chain_len,
        )

    if args.accelerate_read_metrics_generation:
        custom_config["knob_shard_read_sample_update_interval"] = "1"
        custom_config["knob_ops_read_units_per_sample"] = "1"

    errcode = 1
    with TempCluster(
        args.build_dir,
        args.process_number,
        blob_granules_enabled=args.blob_granules_enabled,
        tls_config=tls_config,
        tenant_mode="optional_experimental" if enable_tenants else None,
        authorization_kty=args.authorization_kty,
        authorization_keypair_id=args.authorization_keypair_id,
        remove_at_exit=not args.no_remove_at_exit,
        custom_config=custom_config,
    ) as cluster:
        logger.info("log-dir: {}".format(cluster.log))
        logger.info("etc-dir: {}".format(cluster.etc))
        logger.info("data-dir: {}".format(cluster.data))
        logger.info("cluster-file: {}".format(cluster.cluster_file))
        cmd_args = []
        substitution_table = [
            ("@CLUSTER_FILE@", str(cluster.cluster_file)),
            ("@DATA_DIR@", str(cluster.data)),
            ("@LOG_DIR@", str(cluster.log)),
            ("@ETC_DIR@", str(cluster.etc)),
            ("@TMP_DIR@", str(cluster.tmp_dir)),
            ("@SERVER_CERT_FILE@", str(cluster.server_cert_file)),
            ("@SERVER_KEY_FILE@", str(cluster.server_key_file)),
            ("@SERVER_CA_FILE@", str(cluster.server_ca_file)),
            ("@CLIENT_CERT_FILE@", str(cluster.client_cert_file)),
            ("@CLIENT_KEY_FILE@", str(cluster.client_key_file)),
            ("@CLIENT_CA_FILE@", str(cluster.client_ca_file)),
        ]

        for cmd in args.cmd:
            for (placeholder, value) in substitution_table:
                cmd = cmd.replace(placeholder, value)
            cmd_args.append(cmd)
        env = dict(**os.environ)
        env["FDB_CLUSTER_FILE"] = env.get("FDB_CLUSTER_FILE", cluster.cluster_file)
        logger.info("command: {}".format(cmd_args))
        errcode = subprocess.run(
            cmd_args, stdout=sys.stdout, stderr=sys.stderr, env=env
        ).returncode

        sev40s = (
            subprocess.getoutput(
                "grep -r 'Severity=\"40\"' {}".format(cluster.log.as_posix())
            )
            .rstrip()
            .splitlines()
        )

        for line in sev40s:
            # When running ASAN we expect to see this message. Boost coroutine should be using the correct asan
            # annotations so that it shouldn't produce any false positives.
            if (
                "WARNING: ASan doesn't fully support makecontext/swapcontext functions and may produce false positives in some cases!"
                in line
            ):
                continue
            logger.error(
                ">>>>>>>>>>>>>>>>>>>> Found severity 40 events - the test fails"
            )
            errcode = 1
            break

        if errcode and not args.disable_log_dump:
            for etc_file in glob.glob(os.path.join(cluster.etc, "*")):
                logger.error(">>>>>>>>>>>>>>>>>>>> Contents of {}:".format(etc_file))
                with open(etc_file, "r") as f:
                    logger.error(f.read())
            for log_file in glob.glob(os.path.join(cluster.log, "*")):
                logger.error(">>>>>>>>>>>>>>>>>>>> Contents of {}:".format(log_file))
                with open(log_file, "r") as f:
                    logger.error(f.read())

    sys.exit(errcode)
