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
    type t = {name: string; version: string option}
    let create ?version name  = {name; version}
    let to_string {name; version} =
      sprintf "%s.%s" name (Option.value ~default:"NOVERSION" version)
    let to_directory_name = to_string
    let biopam (* TODO : TMP *) p = create p ~version:"biopam"
  end
  module Default = struct
    open Definition
    let bwa = create "bwa" ~version:"0.7.10"
    let samtools = create "samtools" ~version:"1.3"
    let vcftools = create "vcftools" ~version:"0.1.12b"
    let bedtools = create "bedtools" ~version:"2.23.0"
    let somaticsniper = create "somaticsniper" ~version:"1.0.3"
    let varscan = create "varscan" ~version:"2.3.5"
    let picard = create "picard" ~version:"1.127"
    let mutect = create "mutect" (* We don't know the versions of the users' GATKs *)
    let gatk = create "gatk" (* idem, because of their non-open-source licenses *)
    let strelka = create "strelka" ~version:"1.0.14"
    let virmid = create "virmid" ~version:"1.1.1"
    let muse = create "muse" ~version:"1.0b"
    let star = create "star" ~version:"2.4.1d"
    let stringtie = create "stringtie" ~version:"1.2.2"
    let cufflinks = create "cufflinks" ~version:"2.2.1"
    let hisat = create "hisat" ~version:"0.1.6-beta"
    let hisat2 = create "hisat" ~version:"2.0.2-beta"
    let mosaik = create "mosaik" ~version:"2.2.3"
    let kallisto = create "kallisto" ~version:"0.42.3"
    let optitype = biopam "optitype"
    let seq2hla = biopam "seq2HLA"
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
        ~default:(Program.shf "echo 'Tool %s: default init'"
                    (Definition.to_string definition));
    ensure =
      Option.value_map
        ensure
        ~f:KEDSL.forget_product
        ~default:(workflow_node nothing
                    ~name:(sprintf "%s-ensured"
                             (Definition.to_string definition)));
  }
  let init t = t.init
  let ensure t = t.ensure

  module Kit = struct
    type tool = t
    type t = Definition.t -> tool option

    let concat : t list -> t =
      fun l ->
      fun def ->
        List.find_map l ~f:(fun kit -> kit def)

    let of_list l : t =
      fun def ->
        List.find l ~f:(fun {definition; _} -> definition = def)

    let get_exn t tool =
      match t tool with
      | Some s -> s
      | None ->
        failwithf "Toolkit cannot provide the tool %s"
          (Definition.to_string tool)
  end
end

(** Jobs in Biokepi ask the computing environment (defined below in
    {!Machine}) for resources.

    The implementation of the {!Make_fun.t} function defined by the user
    is free to interpret those requirements according to the user's
    computing infrastructure.
*)
module Make_fun = struct
  module Requirement = struct
    type t = [
      | `Processors of int (** A number of cores on a shared-memory setting. *)
      | `Internet_access (** Able to access public HTTP(S) or FTP URLs. *)
      | `Memory of [
          | `GB of float (** Ask for a specific amount of memory. *)
          | `Small (** Tell that the program does not expect HPC-like memory
                       usage (i.e. not more than 2 GB or your usual laptop). *)
          | `Big (** Tell that the program may ask for a lot of memory
                     but you don't know how much precisely. *)
        ]
      | `Quick_run (** Programs that run fast, with little resources.  Usually,
                       you can interpret this as "OK to run on the login node
                       of my cluster." *)
      | `Spark of string list (** Ask for a Spark (on-YARN) environment with
                                  custom parameters (not in use for now,
                                  ["#WIP"]). *)
      | `Custom of string (** Pass arbitrary data (useful for temporary
                              extensions/experiements outside of Biokepi). *)
      | `Self_identification of string list
        (** Set of names or tags for a workflow-node program to identify
            itself to the {!Machine.t}.
            This is useful for quickly bypassing incorrect requirements set
            in the library (please also report an issue if you need this).  *)
    ] [@@deriving yojson, show]
  end

  type t =
    ?name: string ->
    ?requirements: Requirement.t list ->
    Program.t ->
    KEDSL.Build_process.t
  (** The type of the “run function” used across the library. *)

  (** A stream processor, for this purpose, is a program that runs on one core
      and does not grow in memory arbitrarily. *)
  let stream_processor requirements =
    `Processors 1 :: `Memory `Small :: requirements

  let quick requirements = `Quick_run :: requirements

  let downloading requirements =
    `Internet_access :: stream_processor requirements 

  let with_self_ids ?self_ids l =
    match self_ids with
    | Some tags -> `Self_identification tags :: l
    | None -> l

  let with_requirements : t -> Requirement.t list -> t = fun f l ->
    fun ?name ?(requirements = []) prog ->
      f ?name ~requirements:(l @ requirements) prog
end

type t = {
  name: string;
  host: Host.t;
  get_reference_genome: string -> Reference_genome.t;
  toolkit: Tool.Kit.t;
  run_program: Make_fun.t;
  work_dir: string;
  max_processors: int;
}
let create
    ~host ~get_reference_genome ~toolkit
    ~run_program ~work_dir ~max_processors  name =
  {name; toolkit; get_reference_genome; host;
   run_program; work_dir; max_processors}

let name t = t.name
let as_host t = t.host
let get_reference_genome t = t.get_reference_genome
let get_tool t tool =
  match t.toolkit tool with
  | Some s -> s
  | None ->
    failwithf "Machine %S cannot provide the tool %s"
      t.name (Tool.Definition.to_string tool)

let run_program t = t.run_program

let max_processors t = t.max_processors
(** Get the maximum number of processors that a single job can use in the
    [Machine.t] (i.e. usually the “number-of-threads” paramters of most tools)
*)

let quick_run_program t : Make_fun.t =
  Make_fun.with_requirements t.run_program (Make_fun.quick [])

(** Run a program that does not use much memory and runs on one core. *)
let run_stream_processor ?self_ids t : Make_fun.t =
  Make_fun.with_requirements t.run_program
    (Make_fun.stream_processor [] |> Make_fun.with_self_ids ?self_ids)

(** Run a program that does not use much memory, runs on one core, and needs
    the internet. *)
let run_download_program t : Make_fun.t =
  Make_fun.with_requirements t.run_program (Make_fun.downloading [])

let run_big_program t :
  ?processors: int -> ?self_ids : string list -> Make_fun.t =
  fun ?(processors = 1) ?self_ids ->
    Make_fun.with_requirements
      t.run_program
      (Make_fun.with_self_ids ?self_ids [`Memory `Big; `Processors processors])

let work_dir t = t.work_dir


