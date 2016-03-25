(*
  Conda is a Python environment and package manager:
  http://conda.pydata.org/docs/

  We use it to ensure a consistent Python environment for tools that depend
  on Python.
*)

open Common
open Run_environment

let rm_path = Workflow_utilities.Remove.path_on_host

let dir ~install_path = install_path // "conda_dir"
let commands ~install_path com = dir ~install_path // "bin" // com
let bin = commands "conda"
let activate = commands "activate"
let deactivate = commands "deactivate"

(* give a conda command. *)
let com ~install_path fmt =
  Printf.sprintf ("%s " ^^ fmt) (bin ~install_path)

(* A workflow to ensure that conda is installed. *)
let installed ?host ~install_path =
  let open KEDSL in
  let url =
    "https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh"
  in
  let conda_exec  = single_file ?host (bin ~install_path) in
  let install_dir = dir ~install_path in
  workflow_node conda_exec
    ~name:"Install conda"
    ~make:(daemonize ?host
             Program.(exec ["mkdir"; "-p"; install_path]
                      && exec ["cd"; install_path]
                      && Workflow_utilities.Download.wget_program url
                                            (* -b : batch be silent -p prefix *)
                      && shf "bash Miniconda3-latest-Linux-x86_64.sh -b -p %s"
                                install_dir))

let config = "biokepi_conda_env"
let env_name = "biokepi"
let biokepi_conda_config =
{conda|# This file may be used to create an environment using:
# $ conda create --name <env> --file <this file>
# platform: linux-64
@EXPLICIT
https://repo.continuum.io/pkgs/free/linux-64/anaconda-client-1.2.2-py27_0.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/biopython-1.66-np110py27_0.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/cairo-1.12.18-6.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/clyent-1.2.0-py27_0.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/cycler-0.10.0-py27_0.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/distribute-0.6.45-py27_1.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/fontconfig-2.11.1-5.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/freetype-2.5.5-0.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/hdf5-1.8.15.1-2.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/libpng-1.6.17-0.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/libxml2-2.9.2-0.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/matplotlib-1.5.1-np110py27_0.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/mkl-11.3.1-0.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/ncurses-5.9-0.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/numexpr-2.4.6-np110py27_1.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/numpy-1.10.4-py27_1.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/openssl-1.0.2g-0.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/pandas-0.17.1-np110py27_0.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/pip-8.0.3-py27_0.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/pixman-0.32.6-0.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/pycairo-1.10.0-py27_0.tar.bz2
https://conda.anaconda.org/trung/linux-64/pyinstaller-3.1-py27_0.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/pyparsing-2.0.3-py27_0.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/pyqt-4.11.4-py27_1.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/pysam-0.6-py27_0.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/pytables-3.2.2-np110py27_0.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/python-2.7.11-0.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/python-dateutil-2.4.2-py27_0.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/pytz-2015.7-py27_0.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/pyyaml-3.11-py27_1.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/qt-4.8.7-1.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/requests-2.9.1-py27_0.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/setuptools-20.1.1-py27_0.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/sip-4.16.9-py27_0.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/six-1.10.0-py27_0.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/sqlite-3.9.2-0.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/tk-8.5.18-0.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/wheel-0.29.0-py27_0.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/yaml-0.1.6-0.tar.bz2
https://repo.continuum.io/pkgs/free/linux-64/zlib-1.2.8-0.tar.bz2|conda}

(* Removed:
   https://repo.continuum.io/pkgs/free/linux-64/readline-6.2-2.tar.bz2
   This default conda compiled readline isn't linked appropriately:
   symbol lookup error: /tmp/_MEIzf9vto/libreadline.so.6: undefined symbol: PC
   We'll ignore it from the configuration and hope that the computer has a sane
   readline lib installed by default.  *)

(* Ensure that a file exists describing the default linux conda enviroment.
   TODO: figure out a better solution to distribute this configuration.  *)
let cfg_exists ?host ~install_path =
  let open KEDSL in
  let file = install_path // config in
  let make = daemonize ?host
      Program.(exec ["mkdir"; "-p"; install_path]
               && shf "echo %s >> %s" (Filename.quote biokepi_conda_config) file)
  in
  workflow_node (single_file ?host file)
    ~name:("Make sure we have a biokepi conda config file: " ^ config)
    ~make

let configured ?host ~install_path () =
  let open KEDSL in
  let conf =
    com ~install_path "create --name %s --file %s/%s" env_name
      install_path config
  in
  let make = daemonize ?host (Program.sh conf) in
  let edges =
    [ depends_on (installed ?host ~install_path)
    ; depends_on (cfg_exists ?host ~install_path)
    ] in
  let biokepi_env =
    Command.shell ?host (com ~install_path "env list | grep %s" env_name) in
  let product =
    object method is_done = Some (`Command_returns (biokepi_env, 0)) end in
  workflow_node product ~make ~name:"Conda is configured." ~edges

let run_in_biokepi_env ~install_path inside =
  KEDSL.Program.(shf "source %s %s" (activate ~install_path) env_name
                && inside
                && shf "source %s" (deactivate ~install_path))
