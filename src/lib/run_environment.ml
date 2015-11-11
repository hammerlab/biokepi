(**************************************************************************)
(*  Copyright 2014, Sebastien Mondet <seb@mondet.org>                     *)
(*                                                                        *)
(*  Licensed under the Apache License, Version 2.0 (the "License");       *)
(*  you may not use this file except in compliance with the License.      *)
(*  You may obtain a copy of the License at                               *)
(*                                                                        *)
(*      http://www.apache.org/licenses/LICENSE-2.0                        *)
(*                                                                        *)
(*  Unless required by applicable law or agreed to in writing, software   *)
(*  distributed under the License is distributed on an "AS IS" BASIS,     *)
(*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or       *)
(*  implied.  See the License for the specific language governing         *)
(*  permissions and limitations under the License.                        *)
(**************************************************************************)


open Common

open KEDSL


module Tool = struct
  module Definition = struct
    include struct
      (* hack to remove warning from generated code, cf.
         https://github.com/whitequark/ppx_deriving/issues/41 *)
      [@@@ocaml.warning "-11"]
      type t = [
        | `Bwa of [ `V_0_7_10 ]
        | `Custom of string * string
      ] [@@deriving yojson, show, eq]
    end
    let custom n ~version = `Custom (n, version)
  end
  module Default = struct
    open Definition
    let bwa = `Bwa `V_0_7_10
    let samtools = custom "samtools" ~version:"1.1"
    let vcftools = custom "vcftools" ~version:"0.1.12b"
    let bedtools = custom "bedtools" ~version:"2.23.0"
    let somaticsniper = custom "somaticsniper" ~version:"1.0.3"
    let varscan = custom "varscan" ~version:"2.3.5"
    let picard = custom "picard" ~version:"1.127"
    let mutect = custom "mutect" ~version:"unknown"
    let gatk = custom "gatk" ~version:"unknown"
    let strelka = custom "strelka" ~version:"1.0.14"
    let virmid = custom "virmid" ~version:"1.1.1"
    let muse = custom "muse" ~version:"1.0b"
    let star = custom "star" ~version:"2.4.1d"
    let stringtie = custom "stringtie" ~version:"1.0.4"
    let cufflinks = custom "cufflinks" ~version:"2.2.1"
    let hisat = custom "hisat" ~version:"0.1.6-beta"
    let mosaik = custom "mosaik" ~version:"2.2.3"
    let kallisto = custom "kallisto" ~version:"0.42.3"
  end
  type t = {
    definition: Definition.t;
    init: Program.t;
    ensure: phony_workflow;
  }
  let create ?init ?ensure definition = {
    definition;
    init =
      Option.value init
        ~default:(Program.shf "echo '%s: default init'"
                    (Definition.show definition));
    ensure =
      Option.value_map
        ensure
        ~f:KEDSL.forget_product
        ~default:(workflow_node nothing
                    ~name:(sprintf "%s-ensured" (Definition.show definition)));
  }
  let init t = t.init
  let ensure t = t.ensure

  module Kit = struct
    type tool = t
    type t = {
      tools: tool list;
    }
    let create tools = {tools}
    let get_exn t def =
      List.find t.tools
        ~f:(fun {definition; _ } -> Definition.equal definition def)
      |> Option.value_exn ~msg:(sprintf "Can't find tool %s"
                                  Definition.(show def))

  end
end (* Tool *)

module Machine = struct

  type run_function = ?name:string -> ?processors:int -> Program.t ->
    Ketrew_pure.Target.Build_process.t

  type t = {
    name: string;
    ssh_name: string;
    host: Host.t;
    get_reference_genome: Reference_genome.specification -> Reference_genome.t;
    toolkit: Tool.Kit.t;
    quick_command: Program.t -> Ketrew_pure.Target.Build_process.t;
    run_program: run_function;
    work_dir: string;
  }
  let create
      ~ssh_name ~host ~get_reference_genome ~toolkit
      ~quick_command ~run_program ~work_dir  name =
    {name; ssh_name; toolkit; get_reference_genome; host;
     quick_command; run_program; work_dir}

  let name t = t.name
  let ssh_name t = t.ssh_name
  let as_host t = t.host
  let get_reference_genome t = t.get_reference_genome
  let get_tool t =
    Tool.Kit.get_exn t.toolkit
  let quick_command t = t.quick_command
  let run_program t = t.run_program
  let work_dir t = t.work_dir

end (* Machine *)

let rm_path ~host path =
  workflow_node nothing
    ~name:(sprintf "rm-%s" (Filename.basename path))
    ~make:(daemonize ~using:`Python_daemon ~host
             Program.(exec ["rm"; "-rf"; path]))


module Tool_providers = struct

  let download_url_program ?output_filename url =
    KEDSL.Program.exec [
      "wget";
      "-O"; Option.value output_filename ~default:Filename.(basename url);
      url
    ]

  let install_bwa_like ~host ?install_command ?witness ~install_path ~url tool_name =
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
    let install_path = meta_playground // "stringtie" in
    let ensure =
      install_bwa_like ~host "stringtie"
        ~url:"http://ccb.jhu.edu/software/stringtie/dl/stringtie-1.0.4.tar.gz"
        ~install_path in
    Tool.create Tool.Default.stringtie ~ensure
      ~init:(Program.shf "export PATH=%s:$PATH" install_path)

  let samtools ~host ~meta_playground =
    let install_path = meta_playground // "samtools" in
    let url = "http://downloads.sourceforge.net/project/samtools/samtools/\
               1.1/samtools-1.1.tar.bz2" in
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
  ]
  (**
   Mutect (and some other tools) are behind some web-login annoying thing:
   c.f. <http://www.broadinstitute.org/cancer/cga/mutect_download>
   So the user of the lib must provide an SSH or HTTP URL (or
   reimplement the `Tool.t` is some other way).
  *)

  let get_broad_jar ~host ~install_path loc =
    let jar_name =
      match loc with
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
                 | `Scp s ->
                   shf "scp %s %s"
                     (Filename.quote s) (Filename.quote local_box_path)
                 | `Wget s ->
                   shf "wget %s -O %s"
                     (Filename.quote s) (Filename.quote local_box_path)
                 end))

  let mutect_tool ~host ~meta_playground loc =
    let install_path = meta_playground // "mutect" in
    let get_mutect = get_broad_jar ~host ~install_path loc in
    Tool.create Tool.Default.mutect ~ensure:get_mutect
      ~init:Program.(shf "export mutect_HOME=%s" install_path)

  let gatk_tool ~host ~meta_playground loc =
    let install_path = meta_playground // "gatk" in
    let ensure = get_broad_jar ~host ~install_path loc in
    Tool.create Tool.Default.gatk ~ensure
      ~init:Program.(shf "export GATK_JAR=%s" ensure#product#path)

  let strelka_tool ~host ~meta_playground =
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
    

    
  let default_toolkit
      ~host ~meta_playground
      ~mutect_jar_location ~gatk_jar_location =
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
end (* Tool_providers *)

module Data_providers = struct

  let wget_to_folder ~host ~(run_program : Machine.run_function) ~test_file ~destination url  =
    let name = "wget-" ^ Filename.basename destination in
    let test_target = destination // test_file in
    workflow_node (single_file test_target ~host) ~name
      ~make:(
        run_program ~name ~processors:1
          Program.(
            exec ["mkdir"; "-p"; destination]
            && shf "wget %s -P %s"
              (Filename.quote url)
              (Filename.quote destination)))
      ~edges:[
        on_failure_activate (rm_path ~host destination);
      ]

  let wget ~host ~(run_program : Machine.run_function) url destination =
    let name = "wget-" ^ Filename.basename destination in
    workflow_node
      (single_file destination ~host) ~name
      ~make:(
        run_program ~name ~processors:1
          Program.(
            exec ["mkdir"; "-p"; Filename.dirname destination]
            && shf "wget %s -O %s"
              (Filename.quote url) (Filename.quote destination)))
      ~edges:[
        on_failure_activate (rm_path ~host destination);
      ]

  let wget_gunzip ~host ~(run_program : Machine.run_function) ~destination url =
    let open KEDSL in
    let is_gz = Filename.check_suffix url ".gz" in
    if is_gz then (
      let name = "gunzip-" ^ Filename.basename (destination ^ ".gz") in
      let wgot = wget ~host ~run_program url (destination ^ ".gz") in
      workflow_node
        (single_file destination ~host)
        ~edges:[
          depends_on (wgot);
          on_failure_activate (rm_path ~host destination);
        ]
        ~name
        ~make:(
          run_program ~name ~processors:1
            Program.(shf "gunzip -c %s > %s"
                       (Filename.quote wgot#product#path)
                       (Filename.quote destination)))
    ) else (
      wget ~host ~run_program url destination
    )

  let wget_untar ~host ~(run_program : Machine.run_function) 
      ~destination_folder ~tar_contains url =
    let open KEDSL in
    let zip_flags =
      let is_gz = Filename.check_suffix url ".gz" in
      let is_bzip = Filename.check_suffix url ".bz2" in
      if is_gz then "z" else if is_bzip then "j" else ""
    in
    let tar_filename = (destination_folder ^ ".tar") in
    let name = "untar-" ^ tar_filename in
    let wgot = wget ~host ~run_program url tar_filename in
    let file_in_tar = (destination_folder // tar_contains) in
    workflow_node
      (single_file file_in_tar ~host)
      ~edges:[
        depends_on (wgot);
        on_failure_activate (rm_path ~host destination_folder);
      ]
      ~name
      ~make:(
        run_program ~name ~processors:1
          Program.(
            exec ["mkdir"; "-p"; destination_folder]
            && shf "tar -x%s -f %s -C %s" 
              zip_flags 
              (Filename.quote wgot#product#path)
              (Filename.quote destination_folder)))

  let cat_folder ~host ~(run_program : Machine.run_function) ?(depends_on=[]) ~files_gzipped ~folder ~destination = 
    let deps = depends_on in
    let open KEDSL in
    let name = "cat-folder-" ^ Filename.quote folder in
    let edges =
      on_failure_activate (rm_path ~host destination)
      :: List.map ~f:depends_on deps in
    if files_gzipped then (
      workflow_node (single_file destination ~host)
        ~edges ~name
        ~make:(
          run_program ~name ~processors:1
            Program.(
              shf "gunzip -c %s/* > %s" (Filename.quote folder) (Filename.quote destination)))
    ) else (
      workflow_node
        (single_file destination ~host)
        ~edges ~name
        ~make:(
          run_program ~name ~processors:1
            Program.(
              shf "cat %s/* > %s" (Filename.quote folder) (Filename.quote destination)))
    )

  (*
http://gatkforums.broadinstitute.org/discussion/2226/cosmic-and-dbsnp-files-for-mutect
     
ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle

  *)
  let b37_broad_url =
    "ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/b37/human_g1k_v37.fasta.gz"

  let b37_wdecoy_url =
    "ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/reference/phase2_reference_assembly_sequence/hs37d5.fa.gz"
  
  let b37_alt_url =
    "ftp://ftp.sanger.ac.uk/pub/1000genomes/tk2/\
     main_project_reference/human_g1k_v37.fasta.gz"
  let dbsnp_broad_url =
    "ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/b37/dbsnp_138.b37.vcf.gz"
  let dbsnp_alt_url =
    "ftp://ftp.ncbi.nlm.nih.gov/snp/organisms/human_9606/VCF/v4.0/00-All.vcf.gz"
  let cosmic_broad_url =
    "http://www.broadinstitute.org/cancer/cga/sites/default/files/data/tools/mutect/b37_cosmic_v54_120711.vcf"

  let gtf_b37_url = "http://ftp.ensembl.org/pub/release-75/gtf/homo_sapiens/Homo_sapiens.GRCh37.75.gtf.gz"
  let cdna_b37_url = "http://ftp.ensembl.org/pub/release-75/fasta/homo_sapiens/cdna/Homo_sapiens.GRCh37.75.cdna.all.fa.gz"


  let pull_b37 ~host ~(run_program : Machine.run_function) ~destination_path =
    let fasta =
      wget_gunzip ~host ~run_program b37_broad_url
        ~destination:(destination_path // "b37.fasta") in
    let dbsnp =
      wget_gunzip ~host ~run_program dbsnp_broad_url
        ~destination:(destination_path // "dbsnp.vcf") in
    let cosmic =
      wget_gunzip ~host ~run_program cosmic_broad_url
        ~destination:(destination_path // "cosmic.vcf") in
    let gtf = 
      wget_gunzip ~host ~run_program gtf_b37_url
        ~destination:(destination_path // "transcripts.gtf") in
    let cdna = 
      wget_gunzip ~host ~run_program cdna_b37_url
      ~destination:(destination_path // "Homo_sapiens.GRCh37.75.cdna.all.fa") in
    Reference_genome.create  "B37" fasta ~dbsnp ~cosmic ~gtf ~cdna

  let pull_b37decoy ~host ~(run_program : Machine.run_function) ~destination_path =
    let fasta =
      wget_gunzip ~host ~run_program b37_wdecoy_url
        ~destination:(destination_path // "hs37d5.fasta") in
    let dbsnp =
      wget_gunzip ~host ~run_program dbsnp_broad_url
        ~destination:(destination_path // "dbsnp.vcf") in
    let cosmic =
      wget_gunzip ~host ~run_program cosmic_broad_url
        ~destination:(destination_path // "cosmic.vcf") in
    Reference_genome.create "hs37d5" fasta ~dbsnp ~cosmic

  let b38_url =
    "ftp://ftp.ensembl.org/pub/release-79/fasta/homo_sapiens/dna/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz"
  let dbsnp_b38 =
    "http://ftp.ncbi.nlm.nih.gov/snp/organisms/human_9606_b142_GRCh38/VCF/00-All.vcf.gz"

  let gtf_b38_url = "http://ftp.ensembl.org/pub/release-80/gtf/homo_sapiens/Homo_sapiens.GRCh38.80.gtf.gz"
  let cdna_b38_url = "http://ftp.ensembl.org/pub/release-80/fasta/homo_sapiens/cdna/Homo_sapiens.GRCh38.cdna.all.fa.gz"


  let pull_b38 ~host ~(run_program : Machine.run_function) ~destination_path =
    let fasta =
      wget_gunzip ~host ~run_program b38_url
        ~destination:(destination_path // "b38.fasta") in
    let dbsnp =
      wget_gunzip ~host ~run_program dbsnp_b38
        ~destination:(destination_path // "dbsnp.vcf") in
    let gtf = 
      wget_gunzip ~host ~run_program gtf_b38_url
        ~destination:(destination_path // "transcripts.gtf") in
    let cdna = 
      wget_gunzip ~host ~run_program cdna_b38_url
        ~destination:(destination_path // "GRCh38.cdna.all.fa") in 
    Reference_genome.create  "B38" fasta ~dbsnp ~gtf ~cdna

  let hg19_url =
    "ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/hg19/ucsc.hg19.fasta.gz"
  let dbsnp_hg19_url =
    "ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/hg19/dbsnp_138.hg19.vcf.gz"

  let pull_hg19 ~host ~(run_program : Machine.run_function) ~destination_path =
    let fasta =
      wget_gunzip ~host ~run_program hg19_url
        ~destination:(destination_path // "hg19.fasta") in
    let dbsnp =
      wget_gunzip ~host ~run_program dbsnp_hg19_url
        ~destination:(destination_path // "dbsnp.vcf") in
    Reference_genome.create "hg19" fasta ~dbsnp

  let hg18_url =
    "ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/hg18/Homo_sapiens_assembly18.fasta.gz"
  let dbsnp_hg18_url =
    "ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/hg18/dbsnp_138.hg18.vcf.gz"

  let pull_hg18 ~host ~(run_program : Machine.run_function) ~destination_path =
    let fasta =
      wget_gunzip ~host ~run_program hg18_url
        ~destination:(destination_path // "hg18.fasta") in
    let dbsnp =
      wget_gunzip ~host ~run_program dbsnp_hg18_url
        ~destination:(destination_path // "dbsnp.vcf") in
    Reference_genome.create "hg18" fasta ~dbsnp

end (* Data_providers *)

module Ssh_box = struct
 
  let default_run_program : host:KEDSL.Host.t -> Machine.run_function =
      fun ~host ?(name="biokepi-ssh-box") ?(processors=1) program ->
        daemonize ~using:`Python_daemon ~host program

  let create
      ~gatk_jar_location
      ~mutect_jar_location
      ?run_program ?b37 uri =
    let open KEDSL in
    let host = Host.parse (uri // "ketrew_playground") in
    let meta_playground = Uri.of_string uri |> Uri.path in
    let run_program =
      match run_program with
      | None -> default_run_program ~host
      | Some r -> r
    in
    let actual_b37 =
      match b37 with
      | None  ->
        let destination_path = meta_playground // "B37-reference-genome" in
        Data_providers.pull_b37 ~host ~run_program ~destination_path
      | Some s -> s
    in
    Machine.create (sprintf "ssh-box-%s" uri)
      ~ssh_name:(
        Uri.of_string uri |> Uri.host |> Option.value ~default:"No-NAME")
      ~get_reference_genome:(function
        | `B37 -> actual_b37
        | `B38 -> 
          Data_providers.pull_b38 
            ~host ~run_program ~destination_path:(meta_playground // "B38-reference-genome")
        | `hg18 ->
          Data_providers.pull_hg18
            ~host ~run_program ~destination_path:(meta_playground // "hg18-reference-genome")
        | `hg19 -> 
          Data_providers.pull_hg19 
            ~host ~run_program ~destination_path:(meta_playground // "hg19-reference-genome")
        | `B37decoy -> Data_providers.pull_b37decoy
                         ~host ~run_program ~destination_path:(meta_playground // "hs37d5-reference-genome")
        )
      ~host
      ~toolkit:(
        Tool_providers.default_toolkit
          ~host ~meta_playground
          ~gatk_jar_location ~mutect_jar_location)
      ~run_program
      ~quick_command:(fun program -> run_program program)
      ~work_dir:(meta_playground // "work")

end (* Ssh_box *)
