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


(** Representation of Reference Genomes *)


open Common

type t = {
  name : string;
  location : KEDSL.file_workflow;
  cosmic :  KEDSL.file_workflow option;
  dbsnp :  KEDSL.file_workflow option;
}
(** A reference genome has a name (for display/matching) and a
     cluster-dependent path.
     Corresponding Cosmic and dbSNP databases (VCFs) can be added to the mix.
*)

(** {3 Creation } *)

val create :
  ?cosmic:KEDSL.file_workflow ->
  ?dbsnp:KEDSL.file_workflow ->
  string -> KEDSL.file_workflow -> t
(** Build a [Reference_genome.t] record. *)

val on_host :
  host:KEDSL.Host.t ->
  ?cosmic:string -> ?dbsnp:string -> string -> string -> t
(** Create a [Reference_genome.t] by applying [Ketrew.EDSL.file_target] for
    each path on a given [host]. *)

(** {3 Usual Accessors } *)

val name : t -> string
val path : t -> string
val cosmic_path_exn : t -> string
val dbsnp_path_exn : t -> string
 
(** {3 Targets} *)

val fasta: t -> KEDSL.file_workflow
val cosmic_exn: t -> KEDSL.file_workflow
val dbsnp_exn: t -> KEDSL.file_workflow
