
open Biokepi_run_environment
open Common

let rm_path = Workflow_utilities.Remove.path_on_host

let generic_installation
    ~(run_program : Machine.Make_fun.t)
    ~host ~install_path
    ~install_program ~witness ~url
    ?unarchived_directory
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
      shf "cd %s" (Option.value unarchived_directory
                     ~default:(tool_name ^ "*")) in
    match archive_kind with
    | `Tar tar_option ->
      chain [shf "tar xvf%s %s" tar_option archive;
             shf "rm -f %s" archive; and_cd]
    | `Zip ->
      chain [shf "unzip %s" archive; shf "rm -f %s" archive; and_cd]
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

module Tool_def = Machine.Tool.Definition

type installable = {
  tool_definition : Tool_def.t;
  url : string;
  install_program : path: string -> KEDSL.Program.t;
  init_program : path: string -> KEDSL.Program.t;
  witness: host: KEDSL.Host.t -> path: string -> KEDSL.unknown_product;
  unarchived_directory : string option;
}
let noop = KEDSL.Program.sh "echo Nothing-done-here"


let installable_tool ~url
    ?(install_program = fun ~path -> noop)
    ?(init_program = fun ~path -> noop)
    ~witness
    ?unarchived_directory
    tool_definition =
  {tool_definition; url; install_program;
   init_program; witness; unarchived_directory}

let render_installable_tool ~run_program ~host ~install_tools_path tool =
  let path =
    install_tools_path // Tool_def.to_directory_name tool.tool_definition in
  let  ensure =
    generic_installation
      ?unarchived_directory:tool.unarchived_directory
      ~run_program ~host
      ~install_path:path
      ~install_program:(tool.install_program ~path)
      ~witness:(tool.witness ~host ~path)
      ~url:tool.url
      (tool.tool_definition.Tool_def.name)
  in
  Machine.Tool.create tool.tool_definition ~ensure
    ~init:(tool.init_program path)

let add_to_dollar_path ~path = KEDSL.Program.shf "export PATH=%s:$PATH" path

let make_and_copy_bin bin =
  fun ~path -> KEDSL.Program.(
      sh "make" && shf "cp %s %s" bin path
    )
let witness_file bin =
  fun ~host ~path ->
    let p = KEDSL.single_file ~host (path // bin) in
    object method is_done = p#is_done end
let witness_list l =
  fun ~host ~path ->
    KEDSL.list_of_files ~host (List.map l ~f:(fun bin -> path // bin))
    |> fun p -> object method is_done = p#is_done end

let bwa =
  installable_tool
    Machine.Tool.Default.bwa
    ~url:"http://downloads.sourceforge.net/project/bio-bwa/bwa-0.7.10.tar.bz2"
    ~install_program:(make_and_copy_bin "bwa")
    ~init_program:add_to_dollar_path
    ~witness:(witness_file "bwa")

let stringtie =
  installable_tool
    Machine.Tool.Default.stringtie
    ~url:"https://github.com/gpertea/stringtie/archive/v1.2.2.tar.gz"
    ~install_program:(make_and_copy_bin "stringtie")
    ~init_program:add_to_dollar_path
    ~witness:(witness_file "stringtie")

let vcftools =
  installable_tool Machine.Tool.Default.vcftools
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
  installable_tool Machine.Tool.Default.bedtools
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
  installable_tool Machine.Tool.Default.mosaik ~url
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
  installable_tool ~url Machine.Tool.Default.star
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
  installable_tool tool
    ~url
    ~witness:(witness_file hisat_binary)
    ~install_program:KEDSL.Program.(fun ~path ->
        shf "mv hisat* %s" path
      )
  ~init_program:add_to_dollar_path

let kallisto =
  let url = "https://github.com/pachterlab/kallisto/releases/download\
             /v0.42.3/kallisto_linux-v0.42.3.tar.gz" in
  installable_tool Machine.Tool.Default.kallisto ~url
    ~witness:(witness_file "kallisto")
    ~install_program:KEDSL.Program.(fun ~path ->
        shf "cp -r * %s" path
      )
  ~init_program:add_to_dollar_path

let samtools =
  let url = "https://github.com/samtools/samtools/releases/download/1.3/\
             samtools-1.3.tar.bz2" in
  let toplevel_tools = ["samtools"] in
  let htslib = ["bgzip"; "tabix" ] in
  let tools = toplevel_tools @ htslib in
  let install_program ~path =
    let open KEDSL.Program in
    sh "make"
    && shf "cp %s %s" (String.concat toplevel_tools ~sep:" ")  path
    && sh "cd htslib*/"
    && sh "make"
    && shf "cp %s %s" (String.concat htslib ~sep:" ") path
    && sh "echo Done"
  in
  let witness = witness_list tools in
  installable_tool Machine.Tool.Default.samtools ~url ~install_program
    ~init_program:add_to_dollar_path ~witness


let cufflinks =
  let url =
    "http://cole-trapnell-lab.github.io/cufflinks/assets/downloads/\
     cufflinks-2.2.1.Linux_x86_64.tar.gz" in
  let witness = witness_file "cufflinks" in
  let install_program ~path = KEDSL.Program.(shf "cp * %s" path) in
  installable_tool Machine.Tool.Default.cufflinks ~install_program ~url
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
  installable_tool Machine.Tool.Default.somaticsniper ~install_program ~url
    ~witness:(witness_file binary) ~init_program:add_to_dollar_path



let varscan =
  let url =
    "http://downloads.sourceforge.net/project/varscan/VarScan.v2.3.5.jar" in
  let jar = "VarScan.v2.3.5.jar" in
  let witness = witness_file jar in
  let init_program ~path =
    KEDSL.Program.(shf "export VARSCAN_JAR=%s/%s" path jar) in
  installable_tool Machine.Tool.Default.varscan ~url ~init_program ~witness

let picard =
  let url =
    "https://github.com/broadinstitute/picard/releases/download/1.127/\
     picard-tools-1.127.zip"
  in
  let jar = "picard-tools-1.127" // "picard.jar" in
  let init_program ~path = KEDSL.Program.(shf "export PICARD_JAR=%s/%s" path jar) in
  installable_tool Machine.Tool.Default.picard ~url ~init_program
    ~witness:(witness_file jar)

type broad_jar_location = [
  | `Scp of string
  | `Wget of string
  | `Fail of string
]
(**
   Mutect (and some other tools) are behind some web-login annoying thing:
   c.f. <http://www.broadinstitute.org/cancer/cga/mutect_download>
   So the user of the lib must provide an SSH or HTTP URL (or
   reimplement the `Tool.t` is some other way).
*)

let get_broad_jar
    ~(run_program : Machine.Make_fun.t)
    ~host ~install_path
    loc =
  let open KEDSL in
  let jar_name =
    match loc with
    | `Fail s -> "cannot-get-broad-jar.jar"
    | `Scp s -> Filename.basename s
    | `Wget s -> Filename.basename s in
  let local_box_path = install_path // jar_name in
  let open KEDSL in
  workflow_node (single_file local_box_path ~host)
    ~name:(sprintf "get-%s" jar_name)
    ~edges:[
      on_failure_activate (rm_path ~host local_box_path)
    ]
    ~make:(
      run_program
        ~requirements:[
          `Internet_access;
          `Self_identification ["broad-jar-instalation"; jar_name];
        ]
        Program.(
          shf "mkdir -p %s" install_path
          && begin match loc with
          | `Fail msg ->
            shf "echo 'Cannot download Broad JAR: %s'" msg
            && sh "exit 4"
          | `Scp s ->
            shf "scp %s %s"
              (Filename.quote s) (Filename.quote local_box_path)
          | `Wget s ->
            shf "wget %s -O %s"
              (Filename.quote s) (Filename.quote local_box_path)
          end))

let mutect_tool
    ~(run_program : Machine.Make_fun.t)
    ~host ~install_tools_path loc =
  let tool = Machine.Tool.Default.mutect in
  let open KEDSL in
  let install_path = install_tools_path // Tool_def.to_directory_name tool in
  let get_mutect = get_broad_jar ~run_program ~host ~install_path loc in
  Machine.Tool.create tool ~ensure:get_mutect
    ~init:Program.(shf "export mutect_HOME=%s" install_path)

let gatk_tool
    ~(run_program : Machine.Make_fun.t)
    ~host ~install_tools_path loc =
  let tool = Machine.Tool.Default.gatk in
  let open KEDSL in
  let install_path = install_tools_path // Tool_def.to_directory_name tool in
  let ensure = get_broad_jar ~run_program ~host ~install_path loc in
  Machine.Tool.create tool ~ensure
    ~init:Program.(shf "export GATK_JAR=%s" ensure#product#path)

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
  installable_tool Machine.Tool.Default.strelka ~url
    ~init_program ~install_program ~witness

let virmid =
  let url =
    "http://downloads.sourceforge.net/project/virmid/virmid-1.1.1.tar.gz" in
  let jar = "Virmid-1.1.1" // "Virmid.jar" in
  let init_program ~path =
    KEDSL.Program.(shf "export VIRMID_JAR=%s/%s" path jar) in
  installable_tool Machine.Tool.Default.virmid ~url ~init_program
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
  installable_tool Machine.Tool.Default.muse ~url
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
  installable_tool Machine.Tool.Default.fastqc ~url
    ~witness:(witness_file binary)
    ~install_program:KEDSL.Program.(fun ~path ->
        shf "cp -r * %s" path
        && shf "chmod +x %s" (binary_path path)
      )
    ~init_program
    ~unarchived_directory:"FastQC"

let default_jar_location msg (): broad_jar_location =
  `Fail (sprintf "No location provided for %s" msg)

let default_toolkit
    ~run_program
    ~host ~install_tools_path
    ?(mutect_jar_location = default_jar_location "Mutect")
    ?(gatk_jar_location = default_jar_location "GATK")
    () =
  let install installable =
    render_installable_tool ~host installable ~install_tools_path ~run_program
  in
  Machine.Tool.Kit.concat [
    Machine.Tool.Kit.of_list [
      mutect_tool ~run_program ~host ~install_tools_path (mutect_jar_location ());
      gatk_tool ~run_program ~host ~install_tools_path (gatk_jar_location ());
      install bwa;
      install samtools;
      install bedtools;
      install vcftools;
      install strelka;
      install picard;
      install somaticsniper;
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
    ];
    Biopam.default ~run_program ~host
      ~install_path:(install_tools_path // "biopam-kit") ();
    Python_package.default ~run_program ~host 
      ~install_path: (install_tools_path // "python-tools") ();
  ]
