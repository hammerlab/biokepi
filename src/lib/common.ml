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

(** [Pervasives] module for the library. *)

include Nonstd
module String = struct
  include Sosa.Native_string
end

let (//) = Filename.concat
(** A very standard operator. *)

let dbg fmt = ksprintf (eprintf "biokepi-debug: %s\n%!") fmt

let failwithf fmt = ksprintf failwith fmt

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

  type 'product workflow_node = <
    product : 'product;
    target: user_target;
  > constraint 'product = < is_done : Condition.t option ; .. >

  type workflow_edge =
    | Depends_on: 'a workflow_node -> workflow_edge
    | On_success_activate: _ workflow_node -> workflow_edge
    | On_failure_activate: _ workflow_node -> workflow_edge
    | Empty_edge: workflow_edge

  let depends_on l =  Depends_on l
  let on_success_activate n = On_success_activate n
  let on_failure_activate n = On_failure_activate n

  type nothing = < is_done : Condition.t option >
  let nothing  = object method is_done = None end

  let workflow_node
      ?active
      ?make ?done_when ?metadata
      ?equivalence
      ?(tags=[]) ?name ?(edges=[])
      (product: 'product) : 'product workflow_node =
    let user_target =
      let done_when =
        match done_when with
        | Some s -> Some s
        | None -> product#is_done
      in
      let depends_on =
        List.filter_map edges ~f:(function
          | Depends_on we -> Some we#target
          | _ -> None) in
      let on_success_activate =
        List.filter_map edges ~f:(function
          | On_success_activate we -> Some we#target
          | _ -> None) in
      let on_failure_activate =
        List.filter_map edges ~f:(function
          | On_failure_activate we -> Some we#target
          | _ -> None) in
      let actual_name = Option.value name ~default:"Biokepi" in
      let tags = "biokepi" :: tags in
      target
        actual_name
        ?equivalence ~tags
        ~on_success_activate ~on_failure_activate ~depends_on
        ?active ?make ?metadata ?done_when
    in
    object
      method product = product
      method target = user_target
    end


  let target _ = `Please_KEDSL_workflow
  let file_target _ = `Please_KEDSL_workflow

  type single_file = <
    exists: Ketrew_pure.Target.Condition.t;
    is_done: Ketrew_pure.Target.Condition.t option;
    path : string;
    is_bigger_than: int -> Ketrew_pure.Target.Condition.t;
  >
  let single_file ?(host= Host.tmp_on_localhost) path : single_file =
    let basename = Filename.basename path in
    object
      val vol =
        Ketrew_pure.Target.Volume.(
          create ~host
            ~root:(Ketrew_pure.Path.absolute_directory_exn (Filename.dirname path))
            (file basename))
      method path = path
      method exists = `Volume_exists vol
      method is_done = Some (`Volume_exists vol)
      method is_bigger_than n = `Volume_size_bigger_than (vol, n)
    end

  let forget_product node : nothing workflow_node =
    object method product = nothing  method target = node#target end

  type file_workflow = single_file workflow_node
  type phony_workflow = nothing workflow_node

  type list_of_files = <
    is_done: Ketrew_pure.Target.Condition.t option;
    paths : string list;
  >
  let list_of_files ?host paths =
    object
      val files = List.map paths ~f:(fun p -> single_file ?host p)
      method is_done =
        Some (`And (List.filter_map files ~f:(fun f -> f#is_done)))
      method paths = paths
    end


  type fastq_reads = <
    is_done: Ketrew_pure.Target.Condition.t option;
    paths : string * (string option);
  >
  let fastq_reads ?host r1 r2_opt : fastq_reads =
    object
      val r1_file = single_file ?host r1
      val r2_file_opt = Option.map r2_opt ~f:(single_file ?host)
      method paths = (r1, r2_opt)
      method is_done =
        Some (match r2_file_opt with
          | Some r2 -> `And [r1_file#exists; r2#exists]
          | None -> `And [r1_file#exists; r1_file#exists;])
    end

  type bam_file = <
    is_done: Ketrew_pure.Target.Condition.t option;
    path : string;
    sorting: [ `Coordinate | `Read_name ] option;
    content_type: [ `DNA | `RNA ];
  >
  let bam_file ?host ?sorting ?(contains=`DNA) path : bam_file =
    object
      val file = single_file ?host path
      method path = file#path
      method is_done = file#is_done
      method sorting = sorting
      method content_type = contains
    end


  let submit w = Ketrew.Client.submit w#target

end

(** and then we forbid any other access to Ketrew, to force everything
    to throught the above module. *)
module Ketrew = struct end


(** An attempt at standardizing “tags.” *)
module Target_tags = struct
  let aligner = "aligner"
  let variant_caller = "variant-caller"
  let clean_up = "clean-up"
end
