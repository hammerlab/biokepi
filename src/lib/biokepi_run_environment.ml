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
  type t = {
    name: string;
    ssh_name: string;
    host: Host.t;
    get_reference_genome: [`B37] -> Biokepi_reference_genome.t;
    get_tool: string -> Tool.t;
    quick_command: Program.t -> Ketrew_target.Build_process.t;
    run_program: ?name:string -> ?processors:int -> Program.t ->
      Ketrew_target.Build_process.t;
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

module Tool_providers = struct

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
            && shf "wget %s" Filename.(quote url)
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
                 && exec ["wget"; deb_url]
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
                   && exec ["wget"; url]))
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
                   && exec ["wget"; url]
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
end
