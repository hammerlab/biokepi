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


open Biokepi_common

open Ketrew.EDSL


module Tool = struct
  type t = {
    name: string;
    init: Program.t;
    ensure: user_target;
  }
  let create ?init ?ensure name = {
    name;
    init =
      Option.value init
        ~default:(Program.shf "echo '%s: default init'" name);
    ensure =
      Option.value ensure
        ~default:(target (sprintf "%s-ensured" name));
  }
  let init t = t.init
  let ensure t = t.ensure
end

module Machine = struct

  type run_function = ?name:string -> ?processors:int -> Program.t ->
    Ketrew_target.Build_process.t

  type t = {
    name: string;
    ssh_name: string;
    host: Host.t;
    get_reference_genome: [`B37 | `hg19 | `B38 | `B37decoy ] -> Biokepi_reference_genome.t;
    get_tool: string -> Tool.t;
    quick_command: Program.t -> Ketrew_target.Build_process.t;
    run_program: run_function;
    work_dir: string;
  }
  let create
      ~ssh_name ~host ~get_reference_genome ~get_tool
      ~quick_command ~run_program ~work_dir  name =
    {name; ssh_name; get_tool; get_reference_genome; host;
     quick_command; run_program; work_dir}

  let name t = t.name
  let ssh_name t = t.ssh_name
  let as_host t = t.host
  let get_reference_genome t = t.get_reference_genome
  let get_tool t = t.get_tool
  let quick_command t = t.quick_command
  let run_program t = t.run_program
  let work_dir t = t.work_dir

end

let rm_path ~host path =
  let open Ketrew.EDSL in
  target (sprintf "rm-%s" (Filename.basename path))
    ~make:(daemonize ~using:`Python_daemon ~host
             Program.(exec ["rm"; "-rf"; path]))

module Gunzip = struct
  (**
     Example: call ["gunzip <list of fastq.gz files> > some_name_cat.fastq"].
  *)
  let concat ~(run_with : Machine.t) bunch_of_dot_gzs ~result_path =
    let open Ketrew.EDSL in
    let program =
      Program.(
        exec ["mkdir"; "-p"; Filename.dirname result_path]
        && shf "gunzip -c  %s > %s"
          (List.map bunch_of_dot_gzs
             ~f:(fun o -> Filename.quote o#product#path)
           |> String.concat ~sep:" ") result_path
      ) in
    let name =
      sprintf "gunzipcat-%s" (Filename.basename result_path) in
    file_target result_path ~host:Machine.(as_host run_with) ~name
      ~dependencies:bunch_of_dot_gzs
      ~make:(Machine.run_program run_with ~processors:1 ~name  program)
end


module Cat = struct
  let concat ~(run_with : Machine.t) bunch_of_files ~result_path =
    let open Ketrew.EDSL in
    let program =
      Program.(
        exec ["mkdir"; "-p"; Filename.dirname result_path]
        && shf "cat %s > %s"
          (List.map bunch_of_files
             ~f:(fun o -> Filename.quote o#product#path)
           |> String.concat ~sep:" ") result_path
      ) in
    let name =
      sprintf "concat-all-%s" (Filename.basename result_path) in
    file_target result_path ~host:Machine.(as_host run_with) ~name
      ~dependencies:bunch_of_files
      ~make:(Machine.run_program run_with ~processors:1 ~name  program)
end

module Tool_providers = struct

  let download_url_program ?output_filename url =
    Ketrew.EDSL.Program.exec [
      "wget";
      "-O"; Option.value output_filename ~default:Filename.(basename url);
      url
    ]

  let install_bwa_like ~host ?install_command ?witness ~install_path ~url tool_name =
    let archive = Filename.basename url in
    let install_program =
      Program.(Option.value_map install_command
                 ~f:sh ~default:(shf "cp %s ../" tool_name)) in
    let tar_option = if Filename.check_suffix url "bz2" then "j" else "z" in
    file_target ~host
      (Option.value witness ~default:(install_path // tool_name))
      ~if_fails_activate:[rm_path ~host install_path]
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
    Tool.create "bwa" ~ensure
      ~init:(Program.shf "export PATH=%s:$PATH" install_path)

  let samtools ~host ~meta_playground =
    let install_path = meta_playground // "samtools" in
    let ensure =
      install_bwa_like ~host "samtools"
        ~url:"http://downloads.sourceforge.net/project/samtools/samtools/\
              1.1/samtools-1.1.tar.bz2"
        ~install_path in
    Tool.create "samtools" ~ensure
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
    Tool.create "vcftools" ~ensure
      ~init:Program.(shf "export PATH=%s/bin/:$PATH" install_path
                     && shf "export PERL5LIB=$PERL5LIB:%s/site_perl/"
                       install_path)

  let get_somaticsniper_binary ~host ~path = function
  | `AMD64 ->
    let deb_file = "somatic-sniper1.0.3_1.0.3_amd64.deb" in
    let deb_url =
      sprintf
        "http://apt.genome.wustl.edu/ubuntu/pool/main/s/somatic-sniper1.0.3/%s"
        deb_file in
    let binary = path // "usr/bin/bam-somaticsniper1.0.3" in
    let open Ketrew.EDSL in
    file_target binary ~host
      ~name:(sprintf "get_somaticsniper-on-%s" (Ketrew_host.to_string_hum host))
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
      file_target binary ~host ~dependencies:[binary_got]
        ~make:(daemonize ~using:`Python_daemon ~host
                 Program.(shf "mv %s %s"
                            Filename.(quote binary_got#product#path)
                            Filename.(quote binary)))
    in
    let init = Program.(shf "export PATH=%s/:$PATH" install_path) in
    Tool.create "somaticsniper" ~ensure ~init

  let varscan_tool ~host ~meta_playground =
    let url =
      "http://downloads.sourceforge.net/project/varscan/VarScan.v2.3.5.jar" in
    let install_path = meta_playground // "varscan.2.3.5" in
    let jar = install_path // "VarScan.v2.3.5.jar" in
    let ensure =
      file_target jar ~host
        ~make:(daemonize ~host ~using:`Python_daemon
                 Program.(
                   exec ["mkdir"; "-p"; install_path]
                   && exec ["cd"; install_path]
                   && download_url_program url))
    in
    let init = Program.(shf "export VARSCAN_JAR=%s" jar) in
    Tool.create "varscan" ~ensure ~init

  let picard_tool ~host ~meta_playground =
    let url =
      "https://github.com/broadinstitute/picard/releases/download/1.127/picard-tools-1.127.zip"
    in
    let install_path = meta_playground // "picard"  in
    let jar = install_path // "picard-tools-1.127" // "picard.jar" in
    let ensure =
      file_target jar ~host
        ~make:(daemonize ~host ~using:`Python_daemon
                 Program.(
                   exec ["mkdir"; "-p"; install_path]
                   && exec ["cd"; install_path]
                   && download_url_program url
                   && exec ["unzip"; Filename.basename url]
                 ))
    in
    let init = Program.(shf "export PICARD_JAR=%s" jar) in
    Tool.create "picard" ~ensure ~init

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
    let open Ketrew.EDSL in
    file_target local_box_path ~name:(sprintf "get-%s" jar_name)
      ~if_fails_activate:[rm_path ~host local_box_path]
      ~make:(daemonize ~using:`Python_daemon ~host
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
    Tool.create "mutect" ~ensure:get_mutect
      ~init:Program.(shf "export mutect_HOME=%s" install_path)

  let gatk_tool ~host ~meta_playground loc =
    let install_path = meta_playground // "gatk" in
    let ensure = get_broad_jar ~host ~install_path loc in
    Tool.create "mutect" ~ensure
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
      file_target witness ~host
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
    Tool.create "strelka" ~ensure ~init

  let virmid_tool ~host ~meta_playground =
    let url =
      "http://downloads.sourceforge.net/project/virmid/virmid-1.1.1.tar.gz" in
    let install_path = meta_playground // "virmid.1.1.1"  in
    let jar = install_path // "Virmid-1.1.1" // "Virmid.jar" in
    let ensure =
      file_target jar ~host
        ~make:(daemonize ~host ~using:`Python_daemon
                 Program.(
                   exec ["mkdir"; "-p"; install_path]
                   && exec ["cd"; install_path]
                   && download_url_program url
                   && shf "tar xvfz %s" (Filename.basename url)
                 ))
    in
    let init = Program.(shf "export VIRMID_JAR=%s" jar) in
    Tool.create "virmid" ~ensure ~init

end

module Data_providers = struct

  let wget_to_folder ~host ~(run_program : Machine.run_function) ~test_file ~destination url  =
    let name = "wget-" ^ Filename.basename destination in
    let test_target = destination // test_file in
    file_target test_target ~host ~name
      ~make:(
        run_program ~name ~processors:1
          Program.(
            exec ["mkdir"; "-p"; destination]
            && shf "wget %s -P %s"
                      (Filename.quote url)
                      (Filename.quote destination)))
      ~if_fails_activate:[rm_path ~host destination]

  let wget ~host ~(run_program : Machine.run_function) url destination =
    let name = "wget-" ^ Filename.basename destination in
    file_target destination ~host ~name
      ~make:(
        run_program ~name ~processors:1
          Program.(
            exec ["mkdir"; "-p"; Filename.dirname destination]
            && shf "wget %s -O %s"
                      (Filename.quote url) (Filename.quote destination)))
      ~if_fails_activate:[rm_path ~host destination]

  let wget_gunzip ~host ~(run_program : Machine.run_function) ~destination url =
    let open Ketrew.EDSL in
    let is_gz = Filename.check_suffix url ".gz" in
    if is_gz then (
      let name = "gunzip-" ^ (destination ^ ".gz") in
      let wgot = wget ~host ~run_program url (destination ^ ".gz") in
      file_target destination ~host ~dependencies:[wgot] ~name
        ~make:(
          run_program ~name ~processors:1
            Program.(shf "gunzip -c %s > %s"
                       (Filename.quote wgot#product#path)
                       (Filename.quote destination)))
        ~if_fails_activate:[rm_path ~host destination]
    ) else (
       wget ~host ~run_program url destination
    )

  let wget_untar ~host ~(run_program : Machine.run_function) 
                ~destination_folder ~tar_contains url =
    let open Ketrew.EDSL in
    let zip_flags =
      let is_gz = Filename.check_suffix url ".gz" in
      let is_bzip = Filename.check_suffix url ".bz2" in
      if is_gz then "z" else if is_bzip then "j" else ""
    in
    let tar_filename = (destination_folder ^ ".tar") in
    let name = "untar-" ^ tar_filename in
    let wgot = wget ~host ~run_program url tar_filename in
    let file_in_tar = (destination_folder // tar_contains) in
    file_target file_in_tar ~host ~dependencies:[wgot] ~name
      ~make:(
        run_program ~name ~processors:1
          Program.(
            exec ["mkdir"; "-p"; destination_folder]
            && shf "tar -x%s -f %s -C %s" 
                    zip_flags 
                    (Filename.quote wgot#product#path)
                    (Filename.quote destination_folder)))
      ~if_fails_activate:[rm_path ~host destination_folder]

  let cat_folder ~host ~(run_program : Machine.run_function) ?(dependencies=[]) ~files_gzipped ~folder ~destination = 
    let open Ketrew.EDSL in
    let name = "cat-folder-" ^ Filename.quote folder in
    if files_gzipped then (
      file_target destination ~host ~dependencies:dependencies ~name
        ~make:(
          run_program ~name ~processors:1
            Program.(
              shf "gunzip -c %s/* > %s" (Filename.quote folder) (Filename.quote destination)))
        ~if_fails_activate:[rm_path ~host destination]
    ) else (
      file_target destination ~host ~dependencies:dependencies ~name
        ~make:(
          run_program ~name ~processors:1
            Program.(
              shf "cat %s/* > %s" (Filename.quote folder) (Filename.quote destination)))
        ~if_fails_activate:[rm_path ~host destination]
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
    Biokepi_reference_genome.create  "B37" fasta ~dbsnp ~cosmic

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
    Biokepi_reference_genome.create "hs37d5" fasta ~dbsnp ~cosmic

  let b38_url =
    "ftp://ftp.ensembl.org/pub/release-79/fasta/homo_sapiens/dna/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz"
  let dbsnp_b38 =
    "http://ftp.ncbi.nlm.nih.gov/snp/organisms/human_9606_b142_GRCh38/VCF/00-All.vcf.gz"

  let pull_b38 ~host ~(run_program : Machine.run_function) ~destination_path =
    let fasta =
      wget_gunzip ~host ~run_program b38_url
        ~destination:(destination_path // "b38.fasta") in
    let dbsnp =
      wget_gunzip ~host ~run_program dbsnp_b38
        ~destination:(destination_path // "dbsnp.vcf") in
    Biokepi_reference_genome.create  "B38" fasta ~dbsnp

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
    Biokepi_reference_genome.create "hg19" fasta ~dbsnp

end

module Ssh_box = struct
 
  let default_run_program : host:Ketrew.EDSL.Host.t -> Machine.run_function =
      fun ~host ?(name="biokepi-ssh-box") ?(processors=1) program ->
        daemonize ~using:`Python_daemon ~host program

  let create
    ~gatk_jar_location
    ~mutect_jar_location
    ?run_program ?b37 ~meta_playground ssh_hostname =
    let open Ketrew.EDSL in
    let playground = meta_playground // "ketrew_playground" in
    let host = Host.ssh ssh_hostname ~playground in
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
    Machine.create (sprintf "ssh-box-%s" ssh_hostname) ~ssh_name:ssh_hostname
      ~get_reference_genome:(function
        | `B37 -> actual_b37
        | `B38 -> 
            Data_providers.pull_b38 
              ~host ~run_program ~destination_path:(meta_playground // "B38-reference-genome")
        | `hg19 -> 
            Data_providers.pull_hg19 
              ~host ~run_program ~destination_path:(meta_playground // "hg19-reference-genome")
        | `B37decoy -> Data_providers.pull_b37decoy
              ~host ~run_program ~destination_path:(meta_playground // "hs37d5-reference-genome")
      )
      ~host
      ~get_tool:(function
        | "bwa" -> Tool_providers.bwa_tool ~host ~meta_playground
        | "samtools" -> Tool_providers.samtools ~host ~meta_playground
        | "vcftools" -> Tool_providers.vcftools ~host ~meta_playground
        | "mutect" ->
          Tool_providers.mutect_tool
            ~host ~meta_playground (mutect_jar_location ())
        | "gatk" ->
          Tool_providers.gatk_tool
            ~host ~meta_playground (gatk_jar_location ())
        | "picard" -> Tool_providers.picard_tool ~host ~meta_playground
        | "somaticsniper" ->
          Tool_providers.somaticsniper_tool ~host ~meta_playground
        | "varscan" -> Tool_providers.varscan_tool ~host ~meta_playground
        | other -> failwithf "ssh-box: get_tool: unknown tool: %s" other)
      ~run_program
      ~quick_command:(fun program -> run_program program)
      ~work_dir:(meta_playground // "work")

end
