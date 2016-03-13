open Common
open Run_environment

let rm_path = Workflow_utilities.Remove.path_on_host

let download_url_program ?output_filename url =
  KEDSL.Program.exec [
    "wget";
    "-O"; Option.value output_filename ~default:Filename.(basename url);
    url
  ]

let install_bwa_like ~host ?install_command ?witness ~install_path ~url tool_name =
  let open KEDSL in
  let archive = Filename.basename url in
  let install_program =
    Program.(Option.value_map install_command
               ~f:sh ~default:(shf "cp %s ../" tool_name)) in
  let tar_option = 
    if Filename.check_suffix url "bz2" then "j" 
    else if Filename.check_suffix url "gz"  then "z"
    else "" 
  in
  workflow_node
    ~name:(sprintf "Install %s" tool_name)
    (single_file ~host
       (Option.value witness ~default:(install_path // tool_name)))
    ~edges:[
      on_failure_activate (rm_path ~host install_path);
    ]
    ~make:(
      daemonize ~using:`Python_daemon ~host
        Program.(
          shf "mkdir -p %s" install_path
          && shf "cd %s" install_path
          && download_url_program url
          && shf "tar xvf%s %s" tar_option archive
          && shf "cd %s*" tool_name
          && sh "make"
          && install_program
          && sh "echo Done"
        ))

let bwa_tool ~host ~meta_playground =
  let open KEDSL in
  let install_path = meta_playground // "bwa" in
  let ensure =
    install_bwa_like ~host "bwa"
      ~url:"http://downloads.sourceforge.net/project/bio-bwa/\
            bwa-0.7.10.tar.bz2"
      (*http://downloads.sourceforge.net/project/bio-bwa/bwa-0.7.10.tar.bz2*)
      ~install_path in
  Tool.create (`Bwa `V_0_7_10) ~ensure
    ~init:(Program.shf "export PATH=%s:$PATH" install_path)

let mosaik_tool ~host ~meta_playground = 
  let open KEDSL in
  let install_path = meta_playground // "mosaik" in
  let url = "https://mosaik-aligner.googlecode.com/files/MOSAIK-2.2.3-source.tar" in
  let archive = Filename.basename url in
  let ensure =
    workflow_node (single_file ~host (install_path // "MosaikAligner"))
      ~name:(sprintf "Install MOSAIK")
      ~edges:[
        on_failure_activate (rm_path ~host install_path);
      ]
      ~make:(
        daemonize ~using:`Python_daemon ~host
          Program.(
            shf "mkdir -p %s" install_path
            && shf "cd %s" install_path
            && download_url_program url
            && shf "tar xvf %s" archive
            && shf "cd MOSAIK*"
            && sh "make"
            && sh "cp networkFile/*pe.ann ../pe.ann"
            && sh "cp networkFile/*se.ann ../se.ann"
            && sh "cp ../bin/* ../"
            && sh "echo Done"
          ))
  in
  Tool.create Tool.Default.mosaik ~ensure 
    ~init:(
      Program.(
        shf "export PATH=%s:$PATH" install_path
        && shf "export MOSAIK_PE_ANN=%s/pe.ann" install_path
        && shf "export MOSAIK_SE_ANN=%s/se.ann" install_path
      ))



let star_tool ~host ~meta_playground =
  let open KEDSL in
  let install_path = meta_playground // "star" in
  let url = "https://github.com/alexdobin/STAR/archive/STAR_2.4.1d.tar.gz" in
  let archive = Filename.basename url in
  let tar_option = if Filename.check_suffix url "bz2" then "j" else "z" in
  let star_binary = "STAR" in
  let star_binary_path = sprintf "bin/Linux_x86_64/%s" star_binary in
  let ensure =
    workflow_node (single_file ~host (install_path // star_binary))
      ~name:(sprintf "Install STAR")
      ~edges:[
        on_failure_activate (rm_path ~host install_path);
      ]
      ~make:(
        daemonize ~using:`Python_daemon ~host
          Program.(
            shf "mkdir -p %s" install_path
            && shf "cd %s" install_path
            && download_url_program url
            && shf "tar xvf%s %s" tar_option archive
            && shf "cd STAR-*"
            && shf "cp %s ../" star_binary_path
            && sh "echo Done"
          ))
  in
  Tool.create Tool.Default.star ~ensure 
    ~init:(Program.shf "export PATH=%s:$PATH" install_path)

let hisat_tool ~host ~meta_playground =
  let open KEDSL in
  let install_path = meta_playground // "hisat" in
  let url = "http://ccb.jhu.edu/software/hisat/downloads/hisat-0.1.6-beta-Linux_x86_64.zip" in
  let archive = Filename.basename url in
  let hisat_binary = "hisat" in
  let ensure =
    workflow_node (single_file ~host (install_path // hisat_binary))
      ~name:(sprintf "Install HISAT")
      ~edges:[
        on_failure_activate (rm_path ~host install_path);
      ]
      ~make:(
        daemonize ~using:`Python_daemon ~host
          Program.(
            shf "mkdir -p %s" install_path
            && shf "cd %s" install_path
            && download_url_program url
            && shf "unzip %s" archive
            && sh "cd hisat-*"
            && sh "mv hisat* ../"
            && sh "echo Done"
          ))
  in
  Tool.create Tool.Default.hisat ~ensure 
    ~init:(Program.shf "export PATH=%s:$PATH" install_path)

let kallisto_tool ~host ~meta_playground =
  let open KEDSL in
  let install_path  = meta_playground // "kallisto" in
  let url = "https://github.com/pachterlab/kallisto/releases/download/v0.42.3/kallisto_linux-v0.42.3.tar.gz" in
  let archive = Filename.basename url in
  let kallisto_binary = "kallisto" in 
  let ensure =
    workflow_node (single_file ~host (install_path // kallisto_binary))
      ~name:(sprintf "Install kallisto")
      ~edges:[
        on_failure_activate (rm_path ~host install_path);
      ]
      ~make:(
        daemonize ~using:`Python_daemon ~host
          Program.(
            shf "mkdir -p %s" install_path
            && shf "cd %s" install_path
            && download_url_program url
            && shf "tar xvzf %s" archive
            && shf "cd kallisto_*"
            && sh "cp -r * ../"
            && sh "echo Done"
          ))
  in
  Tool.create Tool.Default.kallisto ~ensure
    ~init:(Program.shf "export PATH=%s:$PATH" install_path)

let stringtie_tool ~host ~meta_playground =
  let open KEDSL in
  let install_path = meta_playground // "stringtie" in
  let ensure =
    install_bwa_like ~host "stringtie"
      ~url:"http://ccb.jhu.edu/software/stringtie/dl/stringtie-1.0.4.tar.gz"
      ~install_path in
  Tool.create Tool.Default.stringtie ~ensure
    ~init:(Program.shf "export PATH=%s:$PATH" install_path)

let samtools ~host ~meta_playground =
  let open KEDSL in
  let install_path = meta_playground // "samtools" in
  let url = "https://github.com/samtools/samtools/releases/download/1.3/\
             samtools-1.3.tar.bz2" in
  let archive = Filename.basename url in
  let ensure =
    let tar_option = if Filename.check_suffix url "bz2" then "j" else "z" in
    let toplevel_tools = ["samtools"] in
    let htslib = ["bgzip"; "tabix" ] in
    let tools = toplevel_tools @ htslib in
    let files = List.map ~f:(fun e -> install_path // e) tools in
    workflow_node (list_of_files ~host files)
      ~name:(sprintf "Install Samtools")
      ~edges:[
        on_failure_activate (rm_path ~host install_path);
      ]
      ~make:(
        daemonize ~using:`Python_daemon ~host
          Program.(
            shf "mkdir -p %s" install_path
            && shf "cd %s" install_path
            && download_url_program url
            && shf "tar xvf%s %s" tar_option archive
            && shf "cd samtools-*"
            && sh "make"
            && shf "cp %s ../" (String.concat toplevel_tools ~sep:" ") 
            && sh "cd htslib*/"
            && sh "make"
            && shf "cp %s ../../" (String.concat htslib ~sep:" ") 
            && sh "echo Done"
          ))
  in
  Tool.create Tool.Default.samtools ~ensure
    ~init:(Program.shf "export PATH=%s:$PATH" install_path)

let cufflinks_tools ~host ~meta_playground = 
  let open KEDSL in
  let install_path = meta_playground // "cufflinks" in
  let url = 
    "http://cole-trapnell-lab.github.io/cufflinks/assets/downloads/cufflinks-2.2.1.Linux_x86_64.tar.gz" in
  let tar_option = if Filename.check_suffix url "bz2" then "j" else "z" in
  let archive = Filename.basename url in
  let cufflinks_binary = "cufflinks" in
  let ensure =
    workflow_node (single_file ~host (install_path // cufflinks_binary))
      ~name:(sprintf "Install Cufflinks")
      ~edges:[
        on_failure_activate (rm_path ~host install_path);
      ]
      ~make:(
        daemonize ~using:`Python_daemon ~host
          Program.(
            shf "mkdir -p %s" install_path
            && shf "cd %s" install_path
            && download_url_program url
            && shf "tar xvf%s %s" tar_option archive
            && shf "cd cufflinks-*"
            && sh "cp * ../"
            && sh "echo Done"
          ))
  in
  Tool.create Tool.Default.cufflinks ~ensure 
    ~init:(Program.shf "export PATH=%s:$PATH" install_path)

let vcftools ~host ~meta_playground =
  let open KEDSL in
  let install_path = meta_playground // "vcftools" in
  let ensure =
    install_bwa_like ~host "vcftools"
      ~install_path ~install_command:"cp -r bin ../ && \
                                      cp -r lib/perl5/site_perl ../"
      ~witness:(install_path // "bin" // "vcftools")
      ~url:"http://downloads.sourceforge.net/project/\
            vcftools/vcftools_0.1.12b.tar.gz"
  in
  Tool.create Tool.Default.vcftools ~ensure
    ~init:Program.(shf "export PATH=%s/bin/:$PATH" install_path
                   && shf "export PERL5LIB=$PERL5LIB:%s/site_perl/"
                     install_path)

let bedtools ~host ~meta_playground =
  let open KEDSL in
  let install_path = meta_playground // "bedtools" in
  let ensure =
    install_bwa_like ~host "bedtools"
      ~install_path ~install_command:"cp -r bin ../"
      ~witness:(install_path // "bin" // "bedtools")
      ~url:"https://github.com/arq5x/bedtools2/\
            archive/v2.23.0.tar.gz"
  in
  Tool.create Tool.Default.bedtools ~ensure
    ~init:Program.(shf "export PATH=%s/bin/:$PATH" install_path)

let get_somaticsniper_binary ~host ~path = function
| `AMD64 ->
  let deb_file = "somatic-sniper1.0.3_1.0.3_amd64.deb" in
  let deb_url =
    sprintf
      "http://apt.genome.wustl.edu/ubuntu/pool/main/s/somatic-sniper1.0.3/%s"
      deb_file in
  let binary = path // "usr/bin/bam-somaticsniper1.0.3" in
  let open KEDSL in
  workflow_node (single_file binary ~host)
    ~name:(sprintf "get_somaticsniper-on-%s"
             (Ketrew_pure.Host.to_string_hum host))
    ~make:(daemonize ~using:`Nohup_setsid ~host
             Program.(
               exec ["mkdir"; "-p"; path]
               && exec ["cd"; path]
               && download_url_program deb_url
               && exec ["ar"; "x"; deb_file]
               && exec ["tar"; "xvfz"; "data.tar.gz"]
             ))

let somaticsniper_tool ~host ~meta_playground =
  let open KEDSL in
  let install_path = meta_playground // "somaticsniper" in
  let binary_got =
    get_somaticsniper_binary  ~host  ~path:install_path `AMD64 in
  let binary = install_path // "somaticsniper" in
  let ensure =
    workflow_node (single_file binary ~host)
      ~name:(sprintf "Install somaticsniper")
      ~edges:[depends_on binary_got]
      ~make:(daemonize ~using:`Python_daemon ~host
               Program.(shf "mv %s %s"
                          Filename.(quote binary_got#product#path)
                          Filename.(quote binary)))
  in
  let init = Program.(shf "export PATH=%s/:$PATH" install_path) in
  Tool.create Tool.Default.somaticsniper ~ensure ~init

let varscan_tool ~host ~meta_playground =
  let open KEDSL in
  let url =
    "http://downloads.sourceforge.net/project/varscan/VarScan.v2.3.5.jar" in
  let install_path = meta_playground // "varscan.2.3.5" in
  let jar = install_path // "VarScan.v2.3.5.jar" in
  let ensure =
    workflow_node (single_file jar ~host)
      ~name:"Install varscan"
      ~make:(daemonize ~host ~using:`Python_daemon
               Program.(
                 exec ["mkdir"; "-p"; install_path]
                 && exec ["cd"; install_path]
                 && download_url_program url))
  in
  let init = Program.(shf "export VARSCAN_JAR=%s" jar) in
  Tool.create Tool.Default.varscan ~ensure ~init

let picard_tool ~host ~meta_playground =
  let open KEDSL in
  let url =
    "https://github.com/broadinstitute/picard/releases/download/1.127/picard-tools-1.127.zip"
  in
  let install_path = meta_playground // "picard"  in
  let jar = install_path // "picard-tools-1.127" // "picard.jar" in
  let ensure =
    workflow_node (single_file jar ~host)
      ~name:"Install picard"
      ~make:(daemonize ~host ~using:`Python_daemon
               Program.(
                 exec ["mkdir"; "-p"; install_path]
                 && exec ["cd"; install_path]
                 && download_url_program url
                 && exec ["unzip"; Filename.basename url]
               ))
  in
  let init = Program.(shf "export PICARD_JAR=%s" jar) in
  Tool.create Tool.Default.picard ~ensure ~init

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

let get_broad_jar ~host ~install_path loc =
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
    ~make:(daemonize ~using:`Python_daemon
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

let mutect_tool ~host ~meta_playground loc =
  let open KEDSL in
  let install_path = meta_playground // "mutect" in
  let get_mutect = get_broad_jar ~host ~install_path loc in
  Tool.create Tool.Default.mutect ~ensure:get_mutect
    ~init:Program.(shf "export mutect_HOME=%s" install_path)

let gatk_tool ~host ~meta_playground loc =
  let open KEDSL in
  let install_path = meta_playground // "gatk" in
  let ensure = get_broad_jar ~host ~install_path loc in
  Tool.create Tool.Default.gatk ~ensure
    ~init:Program.(shf "export GATK_JAR=%s" ensure#product#path)

let strelka_tool ~host ~meta_playground =
  let open KEDSL in
  let url =
    "ftp://strelka:%27%27@ftp.illumina.com/v1-branch/v1.0.14/strelka_workflow-1.0.14.tar.gz"
  in
  let install_path = meta_playground // "strelka.1.0.14"  in
  let strelka_bin = install_path // "usr" // "bin" in
  let witness = strelka_bin // "configureStrelkaWorkflow.pl" in
  let ensure =
    (* C.f. ftp://ftp.illumina.com/v1-branch/v1.0.14/README *)
    workflow_node (single_file witness ~host)
      ~name:"Build/install Strelka"
      ~make:(daemonize ~host ~using:`Python_daemon
               Program.(
                 exec ["mkdir"; "-p"; install_path]
                 && exec ["cd"; install_path]
                 && download_url_program url
                 && shf "tar xvfz %s" (Filename.basename url)
                 && sh "cd strelka_workflow-1.0.14/"
                 && shf "./configure --prefix=%s" (install_path // "usr")
                 && sh "make && make install"
               ))
  in
  let init = Program.(shf "export STRELKA_BIN=%s" strelka_bin) in
  Tool.create Tool.Default.strelka ~ensure ~init

let virmid_tool ~host ~meta_playground =
  let open KEDSL in
  let url =
    "http://downloads.sourceforge.net/project/virmid/virmid-1.1.1.tar.gz" in
  let install_path = meta_playground // "virmid.1.1.1"  in
  let jar = install_path // "Virmid-1.1.1" // "Virmid.jar" in
  let ensure =
    workflow_node (single_file jar ~host)
      ~name:"Build/install Virmid"
      ~make:(daemonize ~host ~using:`Python_daemon
               Program.(
                 exec ["mkdir"; "-p"; install_path]
                 && exec ["cd"; install_path]
                 && download_url_program url
                 && shf "tar xvfz %s" (Filename.basename url)
               ))
  in
  let init = Program.(shf "export VIRMID_JAR=%s" jar) in
  Tool.create Tool.Default.virmid ~ensure ~init

let muse_tool ~host ~meta_playground =
  let open KEDSL in
  let url =
    "http://bioinformatics.mdanderson.org/Software/MuSE/MuSEv1.0b" in
  let install_path = meta_playground // "MuSEv1.0b" in
  let binary_path = install_path // "MuSEv1.0b" in
  let ensure =
    workflow_node (single_file binary_path ~host)
      ~name:"Install Muse"
      ~make:(daemonize ~host ~using:`Python_daemon
               Program.(
                 exec ["mkdir"; "-p"; install_path]
                 && exec ["cd"; install_path]
                 && download_url_program url
                 && shf "chmod +x %s" binary_path
               ))
  in
  let init = Program.(shf "export muse_bin=%s" binary_path) in
  Tool.create Tool.Default.muse ~ensure ~init

let default_jar_location msg (): broad_jar_location =
  `Fail (sprintf "No location provided for %s" msg)

let default_toolkit
    ~host ~meta_playground
    ?(mutect_jar_location = default_jar_location "Mutect")
    ?(gatk_jar_location = default_jar_location "GATK")
    () =
  Tool.Kit.create [
    bwa_tool ~host ~meta_playground;
    samtools ~host ~meta_playground;
    bedtools ~host ~meta_playground;
    vcftools ~host ~meta_playground;
    strelka_tool ~host ~meta_playground;
    mutect_tool
      ~host ~meta_playground (mutect_jar_location ());
    gatk_tool
      ~host ~meta_playground (gatk_jar_location ());
    picard_tool ~host ~meta_playground;
    somaticsniper_tool ~host ~meta_playground;
    varscan_tool ~host ~meta_playground;
    muse_tool ~host ~meta_playground;
    virmid_tool ~host ~meta_playground;
    star_tool ~host ~meta_playground;
    stringtie_tool ~host ~meta_playground;
    cufflinks_tools ~host ~meta_playground;
    hisat_tool ~host ~meta_playground;
    mosaik_tool ~host ~meta_playground;
    kallisto_tool ~host ~meta_playground;
  ]

