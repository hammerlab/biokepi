(** Reusable and useful pieces of pipeline.


*)
open Biokepi_run_environment.Common

module Input = struct
  type t =
    | Fastq of fastq
  and fastq_fragment = (string option * fastq_data)
  and fastq = {
    sample_name : string;
    files : fastq_fragment list; (* TODO: rename to “fragments” *)
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

  let tag_v0 = "biokepi-input-v0"
  let current_version_tag = tag_v0

  let to_yojson =
    let string s = `String s in
    let option f o : Yojson.Safe.json = Option.value_map o ~default:`Null ~f in
    let data_to_yojson =
      function
      | PE (r1, r2) ->
        `Assoc ["paired-end", `Assoc ["r1", string r1; "r2", string r2]]
      | SE f -> `Assoc ["single-end", string f]
      | Of_bam (how, sorto, refb, fil) ->
        `Assoc ["bam",
                `Assoc ["kind",
                        string (match how with
                          | `PE -> "paired-end"
                          | `SE -> "single-end");
                        "sorting",
                        (match sorto with
                        | None  -> `Null
                        | Some `Read_name -> `String "read-name"
                        | Some `Coordinate -> `String "coordinate");
                        "reference-genome", string refb;
                        "path", string fil]]
    in
    let file_to_yojson (fragment_option, data) =
      `Assoc [
        "fragment-id", option string fragment_option;
        "data", data_to_yojson data;
    ] in
    let files_to_yojson files = `List (List.map ~f:file_to_yojson files) in
    function
    | Fastq {sample_name; files} ->
      `Assoc [current_version_tag,
              `Assoc ["fastq", `Assoc [
                  "sample-name", string sample_name;
                  "fragments", files_to_yojson files;
                ]]]

  let of_yojson j =
    let open Pvem.Result in
    let error ?json fmt =
      ksprintf (fun s ->
          fail (sprintf "%s%s" s
                  (Option.value_map ~default:"" json
                     ~f:(fun j ->
                         sprintf " but got %s" @@
                         Yojson.Safe.pretty_to_string ~std:true j)))
        ) fmt in
    let data_of_yojson =
      function
      | `Assoc ["paired-end", `Assoc ["r1", `String r1; "r2", `String r2]] ->
        return (PE (r1, r2))
      | `Assoc ["single-end", `String file] -> SE file |> return
      | `Assoc ["bam", `Assoc ["kind", `String kind;
                               "sorting", sorting;
                               "reference-genome", `String refb;
                               "path", `String path;]] ->
        begin match sorting with
        | `Null -> return None
        | `String "coordinate" -> Some `Coordinate |> return
        | `String "read-name" -> Some `Read_name |> return
        | other ->
          error ~json:other "Expecting %S, %S or null (in \"sorting\": ...)"
            "coordinate" "read-name"
        end
        >>= fun sorting ->
        begin match kind with
        | "single-end" -> return `SE
        | "paired-end" -> return `PE
        | other -> error "Kind in bam must be \"SE\" or \"PE\""
        end
        >>= fun kind ->
        return (Of_bam (kind, sorting, refb, path))
      | other ->
        error ~json:other "Expecting string or null (in \"fragment\": ...)"
    in
    let fragment_of_yojson =
      function
      | `Assoc ["fragment-id", frag; "data", data] ->
        begin match frag with
        | `String s -> return (Some s)
        | `Null -> return (None)
        | other ->
          error ~json:other "Expecting string or null (in \"fragment\": ...)"
        end
        >>= fun fragment_id ->
        data_of_yojson data
        >>= fun data_parsed ->
        return (fragment_id, data_parsed)
      | other -> error ~json:other "Expecting {\"fragment\": ... , \"data\": ...}"
    in
    match j with
    | `Assoc [vtag, more] when vtag = current_version_tag ->
      begin match more with
      | `Assoc ["fastq",
                `Assoc ["sample-name", `String sample; "fragments", `List frgs]] ->
        List.fold ~init:(return []) frgs ~f:(fun prev frag ->
            prev >>= fun p ->
            fragment_of_yojson frag
            >>= fun more ->
            return (more :: p))
        >>= fun l ->
        return (Fastq { sample_name = sample; files = List.rev l })
      | other ->
        error ~json:other "Expecting Fastq"
      end
    | other ->
      error ~json:other "Expecting Biokepi_input_v0"
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
            let bam = Bfx.bam ~path ~sample_name ?sorting ~reference_build () in
            Bfx.bam_to_fastq ?fragment_id how bam
        )
      |> Bfx.list


end
