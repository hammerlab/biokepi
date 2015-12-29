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

let dbg fmt = ksprintf (eprintf "biokepi-debug: %s\n%!") fmt
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

  let submit w = Ketrew.Client.submit_workflow w

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
