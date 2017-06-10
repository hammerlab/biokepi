
open Biokepi_run_environment
open Common

let rm_path = Workflow_utilities.Remove.path_on_host

let generic_installation
    ~(run_program : Machine.Make_fun.t)
    ~host ~install_path
    ~install_program ~witness ~url
    ?unarchived_directory
    ?(archive_is_directory = true)
    tool_name =
  let archive = Filename.basename url in
  let archive_kind =
    if Filename.check_suffix url "bz2" then `Tar "j"
    else if Filename.check_suffix url "gz"  then `Tar "z"
    else if Filename.check_suffix url "tar" then `Tar ""
    else if Filename.check_suffix url "zip" then `Zip
    else if Filename.check_suffix url "deb" then `Deb
    else `None
  in
  let open KEDSL in
  let unarchival =
    let open Program in
    let and_cd =
      if archive_is_directory then
        [shf "cd %s" (Option.value unarchived_directory
                        ~default:(tool_name ^ "*"))]
      else [] in
    match archive_kind with
    | `Tar tar_option ->
      chain ([shf "tar xvf%s %s" tar_option archive;
              shf "rm -f %s" archive; ] @ and_cd)
    | `Zip ->
      chain ([shf "unzip %s" archive; shf "rm -f %s" archive;] @ and_cd)
    | `Deb ->
      chain [
        exec ["ar"; "x"; archive];
        exec ["tar"; "xvfz"; "data.tar.gz"];
        exec ["rm"; "-f"; "data.tar.gz"];
      ]
    | `None -> sh "echo Not-an-archive"
  in
  workflow_node
    ~name:(sprintf "Install %s" tool_name)
    witness
    (* (single_file ~host *)
    (*    (Option.value witness ~default:(install_path // tool_name))) *)
    ~edges:[
      on_failure_activate (rm_path ~host install_path);
    ]
    ~make:(
      run_program
        ~requirements:[
          `Internet_access;
          `Self_identification ["generic-instalation"; tool_name];
        ]
        Program.(
          shf "mkdir -p %s" install_path
          && shf "cd %s" install_path
          && Workflow_utilities.Download.wget_program url
          && unarchival
          && install_program
          && sh "echo Done"
        ))

let git_installation
    ~(run_program : Machine.Make_fun.t)
    ~host ~install_path
    ~install_program ~witness
    ~repository ~recursive tool
  =
  let open KEDSL in
  let recursive = if recursive then "--recursive" else "" in
  let version =
    (Option.value_exn
       tool.Machine.Tool.Definition.version
       ~msg:"Git_installable tool must have a verison") in
  let name = tool.Machine.Tool.Definition.name in
  workflow_node
    ~name:(sprintf "Install %s %s" name version)
    witness
    ~edges:[
      on_failure_activate (rm_path ~host install_path);
    ]
    ~make:(
      run_program
        ~requirements:[
          `Internet_access;
          `Self_identification ["git-instalation"; name];
        ]
        Program.(
          shf "mkdir -p %s" install_path
          && shf "cd %s" install_path
          && shf "git clone %s %s" recursive repository
          && shf "cd %s" name
          && shf "git checkout %s" version
          && install_program
          && sh "echo Done"
        ))


module Tool_def = Machine.Tool.Definition

module Installable_tool = struct
  let noop = KEDSL.Program.sh "echo Nothing-done-here"
  type t = {
    tool_definition : Tool_def.t;
    url : string;
    install_program : path: string -> KEDSL.Program.t;
    init_program : path: string -> KEDSL.Program.t;
    witness: host: KEDSL.Host.t -> path: string -> KEDSL.unknown_product;
    unarchived_directory : string option;
    archive_is_directory : bool;
  }

  let make ~url
    ?(install_program = fun ~path -> noop)
    ?(init_program = fun ~path -> noop)
    ~witness ?(archive_is_directory = true)
    ?unarchived_directory
    tool_definition =
  {tool_definition; url; install_program;
   init_program; witness; unarchived_directory; archive_is_directory}

  let render ~run_program ~host ~install_tools_path tool =
    let path =
      install_tools_path // Tool_def.to_directory_name tool.tool_definition in
    let ensure =
      generic_installation
        ?unarchived_directory:tool.unarchived_directory
        ~archive_is_directory:tool.archive_is_directory
        ~run_program ~host
        ~install_path:path
        ~install_program:(tool.install_program ~path)
        ~witness:(tool.witness ~host ~path)
        ~url:tool.url
        (tool.tool_definition.Tool_def.name)
    in
    Machine.Tool.create tool.tool_definition ~ensure
      ~init:(tool.init_program path)
end

module Git_installable_tool = struct
  let noop = KEDSL.Program.sh "echo Nothing-done-here"
  type t = {
    tool_definition : Tool_def.t;
    repository : string;
    recursive : bool;
    install_program : path: string -> KEDSL.Program.t;
    init_program : path: string -> KEDSL.Program.t;
    witness: host: KEDSL.Host.t -> path: string -> KEDSL.unknown_product;
  }

  let make ~repository
    ?(install_program = fun ~path -> noop)
    ?(init_program = fun ~path -> noop)
    ?(recursive = false)
    ~witness
    tool_definition =
  {tool_definition; repository; recursive; install_program; init_program; witness;}

  let render ~run_program ~host ~install_tools_path tool =
    let path =
      install_tools_path // Tool_def.to_directory_name tool.tool_definition in
    let ensure =
      git_installation
        ~run_program ~host
        ~install_path:path
        ~install_program:(tool.install_program ~path)
        ~witness:(tool.witness ~host ~path)
        ~repository:tool.repository
        ~recursive:tool.recursive
        tool.tool_definition
    in
    Machine.Tool.create tool.tool_definition ~ensure
      ~init:(tool.init_program path)
end

let add_to_dollar_path ~path = KEDSL.Program.shf "export PATH=%s:$PATH" path

let make_and_copy_bin bin =
  fun ~path -> KEDSL.Program.(
      sh "make" && shf "cp %s %s" bin path)
let witness_file bin =
  fun ~host ~path ->
    let p = KEDSL.single_file ~host (path // bin) in
    object method is_done = p#is_done end
let witness_list l =
  fun ~host ~path ->
    KEDSL.list_of_files ~host (List.map l ~f:(fun bin -> path // bin))
    |> fun p -> object method is_done = p#is_done end

let bwa =
  Installable_tool.make
    Machine.Tool.Default.bwa
    ~url:"http://downloads.sourceforge.net/project/bio-bwa/bwa-0.7.10.tar.bz2"
    ~install_program:(make_and_copy_bin "bwa")
    ~init_program:add_to_dollar_path
    ~witness:(witness_file "bwa")

let freebayes =
  Git_installable_tool.make
    Machine.Tool.Default.freebayes
    ~repository:"https://github.com/ekg/freebayes.git"
    ~recursive:true
    ~install_program:(fun ~path -> KEDSL.Program.(
        sh "make"
        && shf "cp -r bin %s" path
      ))
    ~init_program:(fun ~path ->
        KEDSL.Program.(shf "export PATH=%s/bin/:$PATH" path))
    ~witness:(witness_list ["bin/freebayes"; "bin/bamleftalign"])

let delly2 =
  Installable_tool.make
    Machine.Tool.Default.delly2
    ~url:"https://github.com/dellytools/delly/releases/download/v0.7.7/delly_v0.7.7_parallel_linux_x86_64bit"
    ~install_program:(fun ~path -> KEDSL.Program.(
        sh "mv delly_v0.7.7_parallel_linux_x86_64bit delly2"
        && sh "chmod 777 delly2"
      ))
    ~archive_is_directory:false
    ~init_program:add_to_dollar_path
    ~witness:(witness_file "delly2")

let sambamba =
  Installable_tool.make
    Machine.Tool.Default.sambamba
    ~archive_is_directory:false
    ~url:"https://github.com/lomereiter/sambamba/releases/download/v0.6.5/sambamba_v0.6.5_linux.tar.bz2"
    ~init_program:add_to_dollar_path
    ~witness:(witness_file "sambamba_v0.6.5")

let stringtie =
  Installable_tool.make
    Machine.Tool.Default.stringtie
    ~url:"https://github.com/gpertea/stringtie/archive/v1.2.2.tar.gz"
    ~install_program:(make_and_copy_bin "stringtie")
    ~init_program:add_to_dollar_path
    ~witness:(witness_file "stringtie")

let vcftools =
  Installable_tool.make Machine.Tool.Default.vcftools
    ~url:"http://downloads.sourceforge.net/project/\
          vcftools/vcftools_0.1.12b.tar.gz"
    ~install_program:(fun ~path -> KEDSL.Program.(
        sh "make"
        && shf  "cp -r bin %s" path
        && shf  "cp -r lib/perl5/site_perl %s" path
      ))
    ~witness:(witness_file @@ "bin" // "vcftools")
    ~init_program:(fun ~path ->
        KEDSL.Program.(shf "export PATH=%s/bin/:$PATH" path
                       && shf "export PERL5LIB=$PERL5LIB:%s/site_perl/" path))

let bedtools =
  Installable_tool.make Machine.Tool.Default.bedtools
    ~url:"https://github.com/arq5x/bedtools2/\
          archive/v2.23.0.tar.gz"
    ~install_program:(fun ~path -> KEDSL.Program.(
        sh "make" && shf "cp -r bin %s" path))
    ~init_program:(fun ~path ->
        KEDSL.Program.(shf "export PATH=%s/bin/:$PATH" path))
    ~witness:(witness_file @@ "bin" // "bedtools")

let mosaik =
  let url =
    "https://mosaik-aligner.googlecode.com/files/MOSAIK-2.2.3-source.tar" in
  Installable_tool.make Machine.Tool.Default.mosaik ~url
    ~unarchived_directory:"MOSAIK*"
    ~init_program:(fun ~path ->
        KEDSL.Program.(
          shf "export PATH=%s:$PATH" path
          && shf "export MOSAIK_PE_ANN=%s/pe.ann" path
          && shf "export MOSAIK_SE_ANN=%s/se.ann" path
        ))
    ~witness:(witness_file "MosaikAligner")
    ~install_program:KEDSL.Program.(fun ~path ->
        sh "make"
        && shf "cp networkFile/*pe.ann %s/pe.ann" path
        && shf "cp networkFile/*se.ann %s/se.ann" path
        && shf "cp bin/* %s" path
      )

let star =
  let url = "https://github.com/alexdobin/STAR/archive/STAR_2.4.1d.tar.gz" in
  let star_binary = "STAR" in
  (* TODO: there are other binaries in `bin/` *)
  let star_binary_path = sprintf "bin/Linux_x86_64/%s" star_binary in
  Installable_tool.make ~url Machine.Tool.Default.star
    ~init_program:add_to_dollar_path
    ~unarchived_directory:"STAR-*"
    ~install_program:KEDSL.Program.(fun ~path ->
        shf "cp %s %s" star_binary_path path)
    ~witness:(witness_file star_binary)

let hisat tool =
  let open KEDSL in
  let url, hisat_binary =
    let open Machine.Tool.Default in
    match tool with
    | one when one = hisat ->
      "http://ccb.jhu.edu/software/hisat/downloads/hisat-0.1.6-beta-Linux_x86_64.zip",
      "hisat"
    | two when two = hisat2 ->
      "ftp://ftp.ccb.jhu.edu/pub/infphilo/hisat2/downloads/hisat2-2.0.2-beta-Linux_x86_64.zip",
      "hisat2"
    | other ->
      failwithf "Can't install Hisat version: %s" (Tool_def.to_string other)
  in
  Installable_tool.make tool
    ~url
    ~witness:(witness_file hisat_binary)
    ~install_program:KEDSL.Program.(fun ~path ->
        shf "mv hisat* %s" path
      )
  ~init_program:add_to_dollar_path

let kallisto =
  let url = "https://github.com/pachterlab/kallisto/releases/download\
             /v0.42.3/kallisto_linux-v0.42.3.tar.gz" in
  Installable_tool.make Machine.Tool.Default.kallisto ~url
    ~witness:(witness_file "kallisto")
    ~install_program:KEDSL.Program.(fun ~path ->
        shf "cp -r * %s" path
      )
  ~init_program:add_to_dollar_path

let samtools =
  let url = "https://github.com/samtools/samtools/releases/download/1.4/\
             samtools-1.4.tar.bz2" in
  let samtools = "samtools" in
  let htslib = ["bgzip"; "tabix" ] in
  let install_program ~path =
    let open KEDSL.Program in
    sh "make"
    && shf "cp %s %s" samtools path
    && sh "cd htslib*/"
    && sh "make"
    && shf "cp %s %s" (String.concat htslib ~sep:" ") path
    && sh "echo Done"
  in
  let witness = witness_list (samtools :: htslib) in
  Installable_tool.make Machine.Tool.Default.samtools ~url ~install_program
    ~init_program:add_to_dollar_path ~witness

let bcftools =
  let url = "https://github.com/samtools/bcftools/releases/download/1.4/\
             bcftools-1.4.tar.bz2" in
  let install_program ~path =
    let open KEDSL.Program in
    sh "make" && shf "cp bcftools %s" path
  in
  let witness = witness_file "bcftools" in
  Installable_tool.make Machine.Tool.Default.bcftools ~url ~install_program
    ~init_program:add_to_dollar_path ~witness

let cufflinks =
  let url =
    "http://cole-trapnell-lab.github.io/cufflinks/assets/downloads/\
     cufflinks-2.2.1.Linux_x86_64.tar.gz" in
  let witness = witness_file "cufflinks" in
  let install_program ~path = KEDSL.Program.(shf "cp * %s" path) in
  Installable_tool.make Machine.Tool.Default.cufflinks ~install_program ~url
    ~init_program:add_to_dollar_path ~witness

let somaticsniper =
  let url =
    let deb_file = "somatic-sniper1.0.3_1.0.3_amd64.deb" in
    sprintf
      "http://apt.genome.wustl.edu/ubuntu/pool/main/s/somatic-sniper1.0.3/%s"
      deb_file
  in
  let binary = "somaticsniper" in
  let binary_in_deb = "usr/bin/bam-somaticsniper1.0.3" in
  let install_program ~path =
    KEDSL.Program.(shf "mv %s/%s %s/%s" path binary_in_deb path binary) in
  Installable_tool.make Machine.Tool.Default.somaticsniper ~install_program ~url
    ~witness:(witness_file binary) ~init_program:add_to_dollar_path

let varscan =
  let url =
    "http://downloads.sourceforge.net/project/varscan/VarScan.v2.3.5.jar" in
  let jar = "VarScan.v2.3.5.jar" in
  let witness = witness_file jar in
  let init_program ~path =
    KEDSL.Program.(shf "export VARSCAN_JAR=%s/%s" path jar) in
  Installable_tool.make Machine.Tool.Default.varscan ~url ~init_program ~witness

(**
   Mutect (and some other tools) are behind some web-login annoying thing:
   c.f. <http://www.broadinstitute.org/cancer/cga/mutect_download>
   So the user of the lib must provide an SSH or HTTP URL (or
   reimplement the `Tool.t` is some other way).
*)

let get_broad_jar =
  Workflow_utilities.Download.get_tool_file ~identifier:"broad-jar"

let mutect_tool
    ~(run_program : Machine.Make_fun.t)
    ~host ~install_tools_path loc =
  let tool = Machine.Tool.Default.mutect in
  let open KEDSL in
  let install_path = install_tools_path // Tool_def.to_directory_name tool in
  let conda_env = (* mutect doesn't run on Java; so need to provide Java 7 *)
    Conda.(setup_environment 
      ~python_version:`Python2
      ~base_packages:[("java-jdk", `Version "7.0.91")]
      install_path 
      "mutect_env")
  in
  let conda_ensure = Conda.(configured ~run_program ~host ~conda_env) in
  let conda_init = Conda.init_env ~conda_env () in
  let get_mutect = get_broad_jar ~run_program ~host ~install_path loc in
  let edges = [depends_on conda_ensure; depends_on get_mutect] in
  let ensure =
    workflow_node without_product ~name:"MuTect setup" ~edges
  in
  let init = 
    Program.(conda_init && shf "export mutect_HOME=%s" install_path)
  in
  Machine.Tool.create tool ~ensure ~init

let gatk_tool
    ~(run_program : Machine.Make_fun.t)
    ~host ~install_tools_path loc =
  let tool = Machine.Tool.Default.gatk in
  let open KEDSL in
  let install_path = install_tools_path // Tool_def.to_directory_name tool in
  let ensure = get_broad_jar ~run_program ~host ~install_path loc in
  Machine.Tool.create tool ~ensure
    ~init:Program.(shf "export GATK_JAR=%s" ensure#product#path)


(* CIBERSORT is yet another utility that is not distributed through a
   conventioal channel but it is rather is provided based on a per-case
   approval process which is online: https://cibersort.stanford.edu/ 

   Once approved, a previously disabled menu item shows up and let the
   user download a ZIP archive that has a bunch of helper scripts and
   data in it. Note that the archive is not leveled, so it expands into
   the current directory as of v1.6. Also important to note that,
   the team is rewriting the app in R but although they also allow
   access to that script, it is still lacking the inference function.
   Until that becomes mature, we will continue using the Java/Python/R
   mish-mash version.
*)
(*
let cibersort_tool
    ~(run_program : Machine.Make_fun.t)
    ~host ~install_tools_path loc =
  let tool = Machine.Tool.Default.cibersort in 
  let open KEDSL in
  let install_path = install_tools_path // Tool_def.to_directory_name tool in
*)




(**

Strelka is built from source but does not seem to build on MacOSX.

*)
let strelka =
  let url =
    "ftp://strelka:%27%27@ftp.illumina.com/v1-branch/v1.0.14/\
     strelka_workflow-1.0.14.tar.gz" in
  let strelka_bin = "usr" // "bin" in
  let witness = witness_file @@ strelka_bin // "configureStrelkaWorkflow.pl" in
  let install_program ~path =
    (* C.f. ftp://ftp.illumina.com/v1-branch/v1.0.14/README *)
    KEDSL.Program.(
      shf "./configure --prefix=%s" (path // "usr")
      && sh "make && make install"
    )
  in
  let init_program ~path =
    KEDSL.Program.(shf "export STRELKA_BIN=%s/%s" path strelka_bin) in
  Installable_tool.make Machine.Tool.Default.strelka ~url
    ~init_program ~install_program ~witness

let virmid =
  let url =
    "http://downloads.sourceforge.net/project/virmid/virmid-1.1.1.tar.gz" in
  let jar = "Virmid-1.1.1" // "Virmid.jar" in
  let init_program ~path =
    KEDSL.Program.(shf "export VIRMID_JAR=%s/%s" path jar) in
  Installable_tool.make Machine.Tool.Default.virmid ~url ~init_program
    ~unarchived_directory:"."
    ~witness:(witness_file jar)

let muse =
  let url =
    "http://bioinformatics.mdanderson.org/Software/MuSE/MuSEv1.0b" in
  let binary = "MuSEv1.0b" in
  let install_program ~path =
    KEDSL.Program.( shf "chmod +x %s/%s" path binary) in
  let init_program ~path =
    KEDSL.Program.(shf "export muse_bin=%s/%s" path binary) in
  Installable_tool.make Machine.Tool.Default.muse ~url
    ~install_program ~init_program
    ~witness:(witness_file binary)

let fastqc =
  let url =
    "http://www.bioinformatics.babraham.ac.uk/projects/fastqc/fastqc_v0.11.5.zip"
  in
  let binary = "fastqc" in
  let binary_path path = path // binary in
  let init_program ~path =
    KEDSL.Program.(shf "export FASTQC_BIN=%s" (binary_path path))
  in
  Installable_tool.make Machine.Tool.Default.fastqc ~url
    ~witness:(witness_file binary)
    ~install_program:KEDSL.Program.(fun ~path ->
        shf "cp -r * %s" path
        && shf "chmod +x %s" (binary_path path)
      )
    ~init_program
    ~unarchived_directory:"FastQC"

  let samblaster =
    let binary = "samblaster" in
    Installable_tool.make
      Machine.Tool.Default.samblaster
      ~url:"https://github.com/GregoryFaust/samblaster/releases/download/v.0.1.22/samblaster-v.0.1.22.tar.gz"
      ~install_program:(make_and_copy_bin binary)
      ~init_program:add_to_dollar_path
      ~witness:(witness_file binary)

let default_tool_location
    msg
    (): Workflow_utilities.Download.tool_file_location
  =
  `Fail (sprintf "No location provided for %s" msg)

let default_netmhc_config () = Netmhc.(
  create_netmhc_config
    ~netmhc_loc:(default_tool_location "NetMHC" ())
    ~netmhcpan_loc:(default_tool_location "NetMHCpan" ())
    ~pickpocket_loc:(default_tool_location "PickPocket" ())
    ~netmhccons_loc:(default_tool_location "NetMHCcons" ())
    ()
)

let default_toolkit
    ~run_program
    ~host ~install_tools_path
    ?(mutect_jar_location = default_tool_location "Mutect")
    ?(gatk_jar_location = default_tool_location "GATK")
    ?(netmhc_config = default_netmhc_config)
    () =
  let install installable =
    Installable_tool.render ~host installable ~install_tools_path ~run_program
  in
  let install_git installable =
    Git_installable_tool.render ~host installable ~install_tools_path ~run_program
  in
  Machine.Tool.Kit.concat [
    Machine.Tool.Kit.of_list [
      mutect_tool ~run_program ~host ~install_tools_path (mutect_jar_location ());
      gatk_tool ~run_program ~host ~install_tools_path (gatk_jar_location ());
      install bwa;
      install samtools;
      install bcftools;
      install bedtools;
      install vcftools;
      install strelka;
      install somaticsniper;
      install sambamba;
      install varscan;
      install muse;
      install virmid;
      install star;
      install stringtie;
      install cufflinks;
      install @@ hisat Machine.Tool.Default.hisat;
      install @@ hisat Machine.Tool.Default.hisat2;
      install mosaik;
      install kallisto;
      install fastqc;
      install samblaster;
      install_git freebayes;
      install delly2;
    ];
    Biopam.default ~run_program ~host
      ~install_path:(install_tools_path // "biopam-kit") ();
    Python_package.default ~run_program ~host
      ~install_path: (install_tools_path // "python-tools") ();
    Bioconda.default ~run_program ~host
      ~install_path: (install_tools_path // "bioconda") ();
    Netmhc.default ~run_program ~host
      ~install_path: (install_tools_path // "netmhc-tools")
      ~netmhc_config: (netmhc_config ())
      ();
  ]
