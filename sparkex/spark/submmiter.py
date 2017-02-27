# -*- coding: utf-8 -*-
"""
Created on Mon Jun 06 11:20:01 2016

@author: t817682
"""

#  Imports & Definitions
import argparse
import sys
import os
import subprocess as sp
import shutil
import zipfile
from configobj import ConfigObj
import time


class PexSubmitter:
    _spark_cli = None
    _spark_conf = None
    _app_args = None
    _app_file_args = None
    _app_value_args = None
    _driver = None
    _tmpdir = None
    _tmp_path = None
    _prog = None
    _pex = None
    _parser = None
    _package_name = None
    _deps = None
    _app_parse_group = None

    def __init__(self, package_name):
        self._package_name = package_name
        self._parser = argparse.ArgumentParser(description=package_name)
        self._arg_parser()
        self._prog = str(sys.argv[0])
        self._app_args = dict()
        self._app_file_args = []
        self._app_value_args = []
        self._tmp_path = os.path.join(os.path.sep, "tmp")

    def _create_tmpdir(self):
        direname = "." + os.path.basename(self._prog).replace(".", "_") + "_" + str(int(time.time()))
        self._tmpdir = os.path.join(self._tmp_path, direname)
        driverdir = os.path.join(self._tmpdir, "driver")
        depsdir = os.path.join(self._tmpdir, "deps")
        if not os.path.exists(self._tmpdir):
            os.makedirs(self._tmpdir)
        if not os.path.exists(driverdir):
            os.makedirs(driverdir)
        if not os.path.exists(depsdir):
            os.makedirs(depsdir)

    def _delete_tmpdir(self):
        if os.path.exists(self._tmpdir):
            shutil.rmtree(self._tmpdir)

    def _make_zipfile(self, output_filename, source_path):
        relroot = os.path.abspath(source_path)
        with zipfile.ZipFile(output_filename, "w", zipfile.ZIP_DEFLATED) as z:
            for root, dirs, files in os.walk(source_path):
                z.write(root, os.path.relpath(root, relroot))
                for file in files:
                    filename = os.path.join(root, file)
                    if os.path.isfile(filename):
                        arcname = os.path.join(os.path.relpath(root, relroot), file)
                        z.write(filename, arcname)
        z.close()
        return os.path.abspath(output_filename)

    def _arg_parser(self):
        self._app_parse_group = self._parser.add_argument_group('APPLICATION', 'Application specific arguments')
        spark_cli_group = self._parser.add_argument_group('SPARK CLI', 'Spark commande line arguments')
        spark_cli_group.add_argument('--properties-file', required=True, metavar='FILE',
                                     help='application parameters')
        spark_cli_group.add_argument('--spark-home', metavar='SPARK_HOME',
                                     help="path to Spark Home directory")
        spark_cli_group.add_argument('--master', metavar='MASTER_URL',
                                     help="spark://host:port, mesos://host:port, yarn, or local.")
        spark_cli_group.add_argument('--deploy-mode', metavar='DEPLOY_MODE',
                                     help="Whether to launch the driver program locally ('client') or "
                                          "on one of the worker machines inside the cluster ('cluster') "
                                          "(Default: client).")
        spark_cli_group.add_argument('--name', metavar='NAME', help="A name of your application.")
        spark_cli_group.add_argument('--jars', metavar='JARS',
                                     help="Comma-separated list of local jars to include on the driver")
        spark_cli_group.add_argument('--py-files', metavar='PY_FILES',
                                     help="Comma-separated list of .zip, .egg, or .py files to place on the "
                                       "PYTHONPATH for Python apps.")
        spark_cli_group.add_argument('--files', metavar='FILES',
                                     help="Comma-separated list of files to be placed in the working directory "
                                       "of each executor.")
        spark_cli_group.add_argument('--driver-memory', metavar='MEM',
                                     help="Memory for driver (e.g. 1000M, 2G) (Default: 1024M).")
        spark_cli_group.add_argument('--driver-java-options', help="Extra Java options to pass to the driver.")
        spark_cli_group.add_argument('--driver-library-path', help="Extra library path entries to pass to the driver.")
        spark_cli_group.add_argument('--driver-class-path',
                                     help="Extra class path entries to pass to the driver. Note that jars added with "
                                       "--jars are automatically included in the classpath.")
        spark_cli_group.add_argument('--executor-memory', metavar='MEM',
                                     help="Memory per executor (e.g. 1000M, 2G) (Default: 1G).")
        spark_cli_group.add_argument('--proxy-user', metavar='NAME',
                                     help="User to impersonate when submitting the application. This argument does not "
                                       "work with --principal / --keytab.")
        spark_cli_group.add_argument('--executor-cores', metavar='NUM',
                                     help="Number of cores per executor. (Default: 1 in YARN mode, or all available cores"
                                       " on the worker in standalone mode)")
        spark_cli_group.add_argument('--driver-cores', metavar='NUM',
                                     help="Number of cores used by the driver, only in cluster mode (Default: 1).")
        spark_cli_group.add_argument('--queue', metavar='QUEUE_NAME',
                                     help="The YARN queue to submit to (Default: 'default').")
        spark_cli_group.add_argument('--num-executors', metavar='NUM', help="Number of executors to launch (Default: 2).")
        spark_cli_group.add_argument('--archives', metavar='ARCHIVES',
                                     help="Comma separated list of archives to be extracted into the working directory"
                                       " of each executor.")
        spark_cli_group.add_argument('--principal', metavar='PRINCIPAL',
                                     help="Principal to be used to login to KDC, while running on secure HDFS.")
        spark_cli_group.add_argument('--keytab', metavar='KEYTAB',
                                     help="The full path to the file that contains the keytab for the principal"
                                       " specified above. This keytab will be copied to the node running the Application "
                                       " Master via the Secure Distributed Cache, for renewing the login tickets and the "
                                       "delegation tokens periodically.")
        spark_conf_group = self._parser.add_argument_group('SPARK CONFIGURATION',
                                                           'Arbitrary Spark configuration property')
        spark_conf_group.add_argument('--conf', metavar='PROP=VALUE', help="Arbitrary Spark configuration property.",
                                      action='append')

    def _extract_dependences(self):
        deps = os.path.join(self._tmpdir, ".deps")
        my_deps = os.path.join(self._tmpdir, "deps")
        pyfiles = ""
        for egg in os.listdir(deps):
            pyfiles += "," + self._make_zipfile(os.path.join(my_deps, egg), os.path.join(deps, egg))
        return pyfiles.lstrip(",")

    def _extract_driver(self):
        search = os.path.join(self._package_name, "driver.py")
        driver = ""
        driverdir = os.path.join(self._tmpdir, "driver")
        for f in self._pex.namelist():
            if search in f and search+"c" not in f:
                driver = f
        return os.path.abspath(self._pex.extract(driver, driverdir))

    def _get_properties(self):
        args_cli = vars(self._parser.parse_args())
        properties_file = os.path.expandvars(args_cli["properties_file"])
        properties_dict = ConfigObj(properties_file)
        args_cli.pop("properties_file")
        file_cli = properties_dict["spark"]["cli"]
        file_conf = properties_dict["spark"]["configuration"]
        if "other" in properties_dict:
            if "local.tmp.dir" in properties_dict["other"]:
                self._tmp_path = properties_dict["other"]["local.tmp.dir"]

        self._pex = zipfile.ZipFile(self._prog)
        self._create_tmpdir()
        self._pex.extractall(self._tmpdir)
        self._deps = self._extract_dependences()
        self._driver = self._extract_driver()

        args_conf = dict()
        conf = args_cli["conf"]
        if conf is not None:
            for c in conf:
                a = c.split("=")
                args_conf[a[0]] = a[1]
        args_cli.pop("conf")
        for key in args_cli.keys():
            if args_cli[key] is None:
                args_cli.pop(key)
            else:
                if key.find("_") > -1:
                    args_cli[key.replace("_", "-")] = args_cli[key]
                    args_cli.pop(key)

        # Application arguments
        # values
        for k in self._app_value_args:
            if k in args_cli:
                self._app_args[k] = args_cli[k]
                args_cli.pop(k)

        # files
        files = ""
        for k in self._app_file_args:
            if k in args_cli:
                files += "," + args_cli[k]
                self._app_args[k] = args_cli[k]
                args_cli.pop(k)
        # Application arguments
        if "py-files" in args_cli and "py-files" in file_cli:
            args_cli["py-files"] += file_cli["py-files"]
        if "files" in args_cli and "files" in file_cli:
            args_cli["files"] += file_cli["files"]

        self._spark_cli = file_cli.copy()
        self._spark_cli.update(args_cli)
        if "py-files" in self._spark_cli:
            pyfiles = self._spark_cli["py-files"] + "," + self._deps
        else:
            pyfiles = self._deps
        self._spark_cli["py-files"] = pyfiles
        if "files" in self._spark_cli:
            sparkfiles = self._spark_cli["files"] + "," + files.strip(",")
        else:
            sparkfiles = files.strip(",")
        self._spark_cli["py-files"] = (None if pyfiles == "" else pyfiles)
        self._spark_cli["files"] = (None if sparkfiles == "" else sparkfiles)
        self._spark_conf = file_conf.copy()
        self._spark_conf.update(args_conf)
        if "master" in self._spark_cli:
            if self._spark_cli["master"] == "yarn-cluster":
                for k in self._app_file_args:
                    if k in self._app_args:
                        self._app_args[k] = os.path.basename(self._app_args[k])

    def print_help(self):
        self._parser.print_help()

    def get_arg_parser(self):
        return self._app_parse_group

    def add_file_argument(self, name, **kwargs):
        self._app_parse_group.add_argument(name, **kwargs)
        self._app_file_args.append(name.strip("-"))

    def add_value_argument(self, name, **kwargs):
        self._app_parse_group.add_argument(name, **kwargs)
        self._app_value_args.append(name.strip("-"))

    def spark_submit(self):
        if not self._prog.endswith(".pex"):
            raise RuntimeError(
                "The Prog file "+ self._prog + os.linesep + " is not a PEX file."
                " this submmiter is specifically designd for .pex packages"
            )
        self._get_properties()
        spark_home = (self._spark_cli["spark-home"] if "spark-home" in self._spark_cli
                      else os.getenv("SPARK_HOME"))
        if spark_home is None:
            raise RuntimeError(
                "Missing environment variable SPARK_HOME "
                "that points to spark home directory"
                "OR use the --spark-home commande line option")
        if "spark-home" in self._spark_cli:
            self._spark_cli.pop("spark-home")
        spark_submit = os.path.join(spark_home, 'bin', 'spark-submit')
        cmd_tab = [spark_submit]

        for (k_cli, v_cli) in self._spark_cli.items():
            if v_cli is not None and v_cli is not None:
                cmd_tab.append("--" + k_cli + " '" + v_cli + "'")

        for (k_conf, v_conf) in self._spark_conf.items():
            if v_conf is not None and v_conf is not None:
                cmd_tab.append('--conf ' + k_conf + "=" + v_conf)

        cmd_tab.append(self._driver)
        if len(self._app_args) != 0:
            app_args = "\"" + str(self._app_args) + "\""
            cmd_tab.append(app_args)
        cmd = ""
        for arg in cmd_tab:
            cmd += arg + " "
        p = sp.Popen(cmd, shell=True)
        return_value = p.wait()
        self._delete_tmpdir()
        return return_value


class BaseDriver:
    args = None
    logger = None

    def __init__(self):
        if len(sys.argv) > 1:
            self.args = eval(sys.argv[1])
        from sparkex.spark.context import Spark
        spark = Spark.instance()
        self.logger = spark.get_logger()

    def print_args(self):
        if self.args:
            return str(self.args)
        return "No application arguments found"
