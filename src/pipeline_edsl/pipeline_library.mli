

module Input : sig

  type t =
    | Fastq of fastq
  and fastq_fragment = (string option * fastq_data)
  and fastq = {
    sample_name : string;
    files : fastq_fragment list;
  }
  and fastq_data =
    | PE of string * string
    | SE of string
    | Of_bam of [ `PE | `SE ] * [ `Coordinate | `Read_name ] option * string * string

  val pp : Format.formatter -> t -> unit
  val show : t -> string

  val pe : ?fragment_id:string -> string -> string -> fastq_fragment

  val se : ?fragment_id:string -> string -> fastq_fragment

  val of_bam :
    ?fragment_id: string ->
    ?sorted:[ `Coordinate | `Read_name ] ->
    reference_build:string ->
    [ `PE | `SE ] -> string -> fastq_fragment

  val fastq_sample :
    sample_name:string -> fastq_fragment list -> t

  val to_yojson : t -> Yojson.Safe.json
  val of_yojson : Yojson.Safe.json -> (t, string) Pvem.Result.t

  module Derive : sig
    val ensure_one_flowcell_max :
      string list ->
      (string list,
       [> `Multiple_flowcells of string list | `Re_group_error of string ])
        Pvem_lwt_unix.Deferred_result.t
    val fastqs :
      ?paired_end:bool ->
      host:Ketrew_pure.Host.t ->
      string ->
      (fastq_fragment list,
       [> `Host of _ Ketrew.Host_io.Error.execution Ketrew.Host_io.Error.non_zero_execution
       | `Multiple_flowcells of string list
       | `R2_expected_for_r1 of string
       | `Re_group_error of string])
        Pvem_lwt_unix.Deferred_result.t
  end

end

module Make:
  functor (Bfx : Semantics.Bioinformatics_base) ->
  sig

    val fastq_of_files :
      sample_name:string ->
      ?fragment_id:string ->
      r1:string -> ?r2:string -> unit -> [ `Fastq ] Bfx.repr

    val fastq_of_input : Input.t -> [ `Fastq ] list Bfx.repr

  end
