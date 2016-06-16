(** Reusable and useful pieces of pipeline.


*)
open Biokepi_run_environment.Common

module Input = struct
  type t =
    | Fastq of fastq
  and fastq = {
    sample_name : string;
    files : (string option * fastq_data) list;
  }
  and fastq_data =
    | PE of string * string
    | SE of string
    | Of_bam of [ `PE | `SE ] * [ `Coordinate | `Read_name ] option * string * string
    [@@deriving show,yojson]

  let pe ?fragment_id a b = (fragment_id, PE (a, b))
  let se ?fragment_id a = (fragment_id, SE a)
  let of_bam ?fragment_id ?sorted ~reference_build how s =
    (fragment_id, Of_bam (how, sorted, reference_build,  s))
  let fastq_sample ~sample_name files = Fastq {sample_name; files}
end

module Make (Bfx : Semantics.Bioinformatics_base) = struct

  (**
     This functions guesses whether to use [fastq] or [fastq_gz] given the
     file name extension. If [r1] and [r2] dot match, it fails with an
     exception.
  *)
  let fastq_of_files  ~sample_name ?fragment_id ~r1 ?r2 () =
    let is_gz r =
      Filename.check_suffix r1 ".gz" || Filename.check_suffix r1 ".fqz"
    in
    match is_gz r1, is_gz r2 with
    | true, true ->
      Bfx.(fastq_gz ~sample_name ?fragment_id ~r1 ?r2 () |> gunzip)
    | false, false ->
      Bfx.(fastq ~sample_name ?fragment_id ~r1 ?r2 ())
    | _ ->
      failwithf "fastq_of_files: cannot handle mixed gzipped \
                 and non-gzipped fastq pairs (for a given same fragment)"

(** Transform a value of type {!Input.t} into a pipeline returning FASTQ data,
    providing the necessary intermediary steps. *)
  let fastq_of_input u =
    let open Input in
    match u with
    | Fastq {sample_name; files} ->
      List.map files ~f:(fun (fragment_id, source) ->
          match source with
          | PE (r1, r2) ->
            fastq_of_files ~sample_name ?fragment_id ~r1 ~r2 ()
          | SE r ->
            fastq_of_files ~sample_name ?fragment_id ~r1:r  ()
          | Of_bam (how, sorting, reference_build, path) ->
            let bam = Bfx.bam ~path ?sorting ~reference_build () in
            Bfx.bam_to_fastq ~sample_name ?fragment_id how bam
        )
      |> Bfx.list


end
