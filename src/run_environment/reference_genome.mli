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

type name = string

module Specification : sig
  module Location : sig
    type t = [
      | `Url of string
      | `Vcf_concat of (string * t) list (* name × location *)
      | `Concat of t list
      | `Gunzip of t (* Should this be guessed from the URL extension? *)
      | `Bunzip2 of t
      | `Untar of t
    ]
    val url : 'a -> [> `Url of 'a ]
    val vcf_concat : 'a -> [> `Vcf_concat of 'a ]
    val concat : 'a -> [> `Concat of 'a ]
    val gunzip : 'a -> [> `Gunzip of 'a ]
    val bunzip2 : 'a -> [> `Bunzip2 of 'a ]
    val untar : 'a -> [> `Untar of 'a ]
  end
  type t = private {
    name : name;
    ensembl : int;
    species : string;
    metadata : string option;
    fasta : Location.t;
    dbsnp : Location.t option;
    known_indels : Location.t option;
    cosmic : Location.t option;
    exome_gtf : Location.t option;
    cdna : Location.t option;
    whess : Location.t option;
    major_contigs : string list option;
  }
  val create :
    ?metadata:string ->
    fasta:Location.t ->
    ensembl:int ->
    species:string ->
    ?dbsnp:Location.t ->
    ?known_indels:Location.t ->
    ?cosmic:Location.t ->
    ?exome_gtf:Location.t ->
    ?cdna:Location.t ->
    ?whess:Location.t ->
    ?major_contigs:string list ->
    string ->
    t
  module Default :
  sig
    module Name : sig
      (** The “names” of the default genomes; the values are provided to
          simplify code and make it less typo-error-prone but the string can be
          ipused directly (e.g. [b37] is just ["b37"]). *)
      val b37 : name
      val b37decoy : name
      val b38 : name
      val hg18 : name
      val hg19 : name
      val mm10 : name
    end
    val b37 : t
    val b37decoy : t
    val b38 : t
    val hg18 : t
    val hg19 : t
    val mm10 : t
  end
end

type t = private {
  specification: Specification.t;
  location : KEDSL.file_workflow;
  cosmic :  KEDSL.file_workflow option;
  dbsnp :  KEDSL.file_workflow option;
  known_indels : KEDSL.file_workflow option;
  gtf : KEDSL.file_workflow option;
  cdna : KEDSL.file_workflow option;
  whess : KEDSL.file_workflow option;
}
(** A reference genome has a name (for display/matching) and a
     cluster-dependent path.
     Corresponding Cosmic and dbSNP databases (VCFs) can be added to the mix.
*)


val create :
  ?cosmic:KEDSL.file_workflow ->
  ?dbsnp:KEDSL.file_workflow ->
  ?known_indels:KEDSL.file_workflow ->
  ?gtf:KEDSL.file_workflow ->
  ?cdna:KEDSL.file_workflow ->
  ?whess:KEDSL.file_workflow ->
  Specification.t -> KEDSL.file_workflow -> t
(** Build a [Reference_genome.t] record. *)

(** {5 Usual Accessors } *)

val name : t -> name
val ensembl : t -> int
val species : t -> string
val path : t -> string
val cosmic_path_exn : t -> string
val dbsnp_path_exn : t -> string
val known_indels_path_exn : t -> string
val gtf_path_exn : t -> string
val cdna_path_exn : t -> string
val whess_path_exn : t -> string

val major_contigs : t -> Region.t list
(** {5 Targets} *)

val fasta: t -> KEDSL.file_workflow
val cosmic_exn: t -> KEDSL.file_workflow
val dbsnp_exn: t -> KEDSL.file_workflow
val known_indels_exn: t -> KEDSL.file_workflow
val gtf_exn: t -> KEDSL.file_workflow
val gtf: t -> KEDSL.file_workflow option
val cdna_exn: t -> KEDSL.file_workflow
val whess_exn: t -> KEDSL.file_workflow
