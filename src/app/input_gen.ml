open Biokepi.EDSL.Library.Input
open Nonstd
module String = Sosa.Native_string

let cmd () =
  let open Cmdliner in
  let version = "0.0.0" in
  let doc = "Generate Input.t JSON" in
  let man = [
    `S "Description";
    `P "Given a directory and a host, generate Input.t JSON representing all";
    `Noblank;
    `P "the FASTQs found therein.";
  ] in
  let name' =
    let doc = "Name of the dataset; should be unique."
    in
    Arg.(required & opt (some string) None & info ["name"; "N"] ~doc)
  in
  let host =
    let doc = "Host where the files are stored, e.g. ssh://dev1,\
               assuming there is a named SSH host at dev1."
    in
    Arg.(required & opt (some string) None & info ["host"; "H"] ~doc)
  in
  let fastqs_directory =
    let doc = "Directory on `host` where tumor fastq.gzs can be found." in
    Arg.(required & opt (some string) None & info ["directory"; "d"] ~doc)
  in
  let single_ended =
    let doc = "Pass if the reads aren't paired." in
    Arg.(value & flag & info ["single-ended"] ~doc)
  in
  Term.(pure (fun name host directory single_ended ->
      let host = match Ketrew_pure.Host.of_string host with
      | `Ok host -> host
      | `Error msg -> printf "Error parsing host.\n%!"; exit 1
      in
      let open Pvem_lwt_unix.Deferred_result in
      Derive.fastqs ~paired_end:(not single_ended) ~host:host directory
      >>= fun fqs ->
      return (fastq_sample ~sample_name:name fqs)
    ) $ name' $ host $ fastqs_directory $ single_ended),
  Term.info "PGV Datasets" ~version ~doc ~man

let () =
  match Cmdliner.Term.eval (cmd ()) with
  | `Error _ -> exit 1
  | `Ok r -> begin match Lwt_main.run r with
    | `Ok fqs ->
      to_yojson fqs |> Yojson.Safe.pretty_to_channel ~std:true stdout;
      flush stdout
    | `Error err ->
      begin match err with
      | `Host hosterr ->
        failwith ("Host could not be used: " ^
                  (Ketrew.Host_io.Error.log hosterr
                   |> Ketrew_pure.Internal_pervasives.Log.to_long_string))
      | `Multiple_flowcells flowcells ->
        failwith (sprintf "Too many flowcells: '%s'\n%!"
                    (String.concat ~sep:"," flowcells))
      | `Re_group_error msg -> failwith msg
      | `R2_expected_for_r1 r1 -> failwith (sprintf "Didn't find an r2 for r1 %s" r1)
      end
    end
  | `Help | `Version -> exit 0
