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

(** Module opened by default (like
{{:http://caml.inria.fr/pub/docs/manual-ocaml/libref/Pervasives.html}Pervasives})
for our library. *)

(** A {{:http://seb.mondet.org/software/nonstd/index.html}Non-standard mini
library}. *)
include Nonstd

(** A String module with more capabilities *)
module String = struct
  include Sosa.Native_string
end

let (//) = Filename.concat
(** [path // filename] will concat [filename] to the end of [path]. *)

let debug_mode =
  ref (try Sys.getenv "BIOKEPI_DEBUG" = "true" with _ -> false)
let dbg fmt = ksprintf (fun s ->
    if !debug_mode
    then eprintf "biokepi-debug: %s\n%!" s
    else ()
  ) fmt
(** A consistent debugging mechanism. *)

let failwithf fmt = ksprintf failwith fmt
(** A formatted [failwith]. *)

module Unique_id = struct
  include Ketrew_pure.Internal_pervasives.Unique_id
end

(**

   This is an experimental extension of Ketrew's EDSL. If we're happy
   with it we'll push it upstream.

   The idea is carry around a type parameter to have arbitrary products.

*)
module KEDSL = struct

  include Ketrew.EDSL
  module Command = Ketrew_pure.Target.Command

  type nothing = < is_done : Condition.t option >
  let nothing  = object method is_done = None end

  let target _ = `Please_KEDSL_workflow
  let file_target _ = `Please_KEDSL_workflow


  type file_workflow = single_file workflow_node
  type phony_workflow = nothing workflow_node

  type fastq_reads = <
    is_done: Ketrew_pure.Target.Condition.t option;
    paths : string * (string option);
    r1 : single_file;
    r2 : single_file option;
    sample_name: string;
    escaped_sample_name: string;
    fragment_id: string option;
    fragment_id_forced: string;
  >
  let fastq_reads ?host ?name ?fragment_id r1 r2_opt : fastq_reads =
    object (self)
      val r1_file = single_file ?host r1
      val r2_file_opt = Option.map r2_opt ~f:(single_file ?host)
      method r1 = r1_file
        (* workflow_node r1_file
          ~name:(sprintf "File: %s" (Filename.basename r1_file#path)) *)
      method r2 =  r2_file_opt
        (* Option.map r2_file_opt ~f:(fun r2_file ->
            workflow_node r2_file
              ~name:(sprintf "File: %s" (Filename.basename r2_file#path))
          ) *)
      method paths = (r1, r2_opt)
      method is_done =
        Some (match r2_file_opt with
          | Some r2 -> `And [r1_file#exists; r2#exists]
          | None -> `And [r1_file#exists; r1_file#exists;])
      method sample_name =
        Option.value name ~default:(Filename.basename r1)
      method fragment_id = fragment_id
      method fragment_id_forced =
        Option.value fragment_id ~default:(Filename.basename r1)
      method escaped_sample_name =
        String.map self#sample_name ~f:(function
          | '0' .. '9' | 'a' .. 'z' | 'A' .. 'Z' | '-' | '_' as c -> c
          | other -> '_')
    end

  let read_1_file_node (fq : fastq_reads workflow_node) =
    let product = fq#product#r1 in
    workflow_node product
      ~name:(sprintf "READ1 of %s-%s"
               fq#product#sample_name
               fq#product#fragment_id_forced)
      ~equivalence:`None
      ~edges:[depends_on fq]

  let read_2_file_node (fq : fastq_reads workflow_node) =
    Option.map fq#product#r2 ~f:(fun product ->
        workflow_node product
          ~name:(sprintf "READ2 of %s-%s"
                   fq#product#sample_name
                   fq#product#fragment_id_forced)
          ~equivalence:`None
          ~edges:[depends_on fq]
      )

  (** Create a [fastq_reads workflow_node] from one or two
      [single_file workflow_node](s).
  *)
  let fastq_node_of_single_file_nodes
      ~host ~name ?fragment_id fastq_r1 fastq_r2 =
    let product =
      let r2 = Option.map fastq_r2 ~f:(fun r -> r#product#path) in
      fastq_reads ~host ~name ?fragment_id fastq_r1#product#path r2
    in
    let edges =
      match fastq_r2 with
      | Some r2 -> [depends_on fastq_r1; depends_on r2]
      | None -> [depends_on fastq_r1]
    in
    workflow_node product
      ~equivalence:`None
      ~name:(sprintf "Assembled-fastq: %s (%s)"
               name (Option.value fragment_id
                       ~default:(Filename.basename fastq_r1#product#path)))
      ~edges

  type bam_file = <
    is_done: Ketrew_pure.Target.Condition.t option;
    host: Host.t;
    path : string;
    sample_name: string;
    escaped_sample_name: string;
    sorting: [ `Coordinate | `Read_name ] option;
    reference_build: string;
  >
  let bam_file ~host ?name ?sorting ~reference_build path : bam_file =
    object (self)
      val file = single_file ~host path
      method host = host
      method sample_name =
        Option.value name ~default:(Filename.chop_extension (Filename.basename path))
      method escaped_sample_name =
        String.map self#sample_name ~f:(function
          | '0' .. '9' | 'a' .. 'z' | 'A' .. 'Z' | '-' | '_' as c -> c
          | other -> '_')
      method path = file#path
      method is_done = file#is_done
      method sorting = sorting
      method reference_build = reference_build
    end

  (** Make a new bam sharing most of the metadata. *)
  let transform_bam ?change_sorting (bam : bam_file) ~path : bam_file =
    bam_file
      ~host:bam#host
      ?sorting:(
        match change_sorting with
        | Some new_sorting -> Some new_sorting
        | None -> bam#sorting
      )
      ~reference_build:bam#reference_build
      path


  type bam_list = <
    is_done:  Ketrew_pure.Target.Condition.t option;
    bams: bam_file list;
  >
  let bam_list (bams : bam_file list) : bam_list =
    object
      method bams = bams
      method is_done =
        Some (
          `And (List.map bams
                  ~f:(fun b ->
                      b#is_done
                      |> Option.value_exn ~msg:"Bams should have a Condition.t"))
        )
    end

  let explode_bam_list_node (bln : bam_list workflow_node) =
    List.map bln#product#bams ~f:(fun bam ->
        workflow_node bam
          ~name:(Filename.basename bam#path)
          ~tags:["expolode_bam_list_node"]
          ~edges:[depends_on bln]
          ~equivalence:`None)

  (* this may be overkill: *)
  type _ bam_or_bams =
    | Single_bam: bam_file workflow_node -> bam_file workflow_node bam_or_bams
    | Bam_workflow_list: bam_file workflow_node list -> bam_list workflow_node bam_or_bams

  let submit w = Ketrew.Client.submit_workflow w

end

(** An attempt at standardizing “tags.” *)
module Target_tags = struct
  let aligner = "aligner"
  let variant_caller = "variant-caller"
  let clean_up = "clean-up"
end
