
open Biokepi_run_environment
open Biokepi_environment_setup
open Biokepi
open Common

let construct_pipeline dataset ~r1fn ~r2fn =
  let wf1 = KEDSL.(workflow_node (single_file r1fn) ~name:"Read1") in
  let wf2 = KEDSL.(workflow_node (single_file r2fn) ~name:"Read2") in
  let open Pipeline.Construct in
  input_fastq ~dataset (`Paired_end ([wf1], [wf2]))
  |> seq2hla

let pipeline_to_json ppln =
  Pipeline.to_json ppln
  |> Yojson.Basic.pretty_to_string ~std:true

let pipeline_to_workflow ~work_dir ?(uri="/tmp/ht") ppln =
  let open Pipeline.Compiler in
  let machine =
    let toolkit = Biopam.default ~install_path:(work_dir // "biopam") () in
    Build_machine.create ~toolkit uri
  in
  let compiler =
    create ~processors:1
      ~reference_build:"should not need this" (*Reference_genome.Specification.Default.Name.b37*)
      ~work_dir:(work_dir // "compiler")
      ~machine ()
  in
  seq2hla_hla_types_step ~compiler ppln

(* Argument extras *)
let () = Random.self_init ()

let generate_dataset_name () = sprintf "HLA_ds%d" (Random.int 1000)

let dataset_env_name = "HLA_DATASET"

(* Main *)
let () =
  let open Cmdliner in
  let version = "0.0.0" in
  (* Args *)
  let dataset_flag =
    Arg.(value
          & opt string (generate_dataset_name ())
          & info
              ~doc:("Name of the dataset (used for identification).\
                     A randomly numbered name prefixed by 'HLA_ds' is chosen if unspecified.")
              ~env:(env_var dataset_env_name) ["D"; "dataset"])
  in
  let r1_fn =
    Arg.(required
          & opt (some non_dir_file) None
          & info ~doc:"Filename of the first read pair."
              ~env:(env_var "HLA_R1") ["r1"])
  in
  let r2_fn =
    Arg.(required
          & opt (some non_dir_file) None
          & info ~doc:"Filename of the second read pair."
              ~env:(env_var "HLA_R2") ["r2"])
  in
  let work_dir =
    Arg.(required
          & opt (some dir) None
          & info ~doc:"Work directory, where tools are installed."
              ~env:(env_var "HLA_WD") ["work_dir"]) in
  (* Commands. *)
  let sub_command ~info ~term =
    (term,
     Term.info (fst info) ~version ~sdocs:"COMMON OPTIONS" ~doc:(snd info))
  in
  let print = sub_command
    ~info:("print", "Print the pipeline as JSON.")
    ~term:(Term.(const (fun dataset r1fn r2fn ->
                          construct_pipeline dataset ~r1fn ~r2fn
                          |> pipeline_to_json
                          |> printf "%s\n")
                  $ dataset_flag $ r1_fn $ r2_fn))
  in
  let exec = sub_command
    ~info:("exec", "Execute the pipeline.")
    ~term:(Term.(const (fun dataset r1fn r2fn work_dir ->
                          construct_pipeline dataset ~r1fn ~r2fn
                          |> pipeline_to_workflow ~work_dir
                          |> KEDSL.submit)
                  $ dataset_flag $ r1_fn $ r2_fn $ work_dir))
  in
  let default_cmd =
    let doc = "HLA Typing Pipeline example" in
    let man = [
      `S "AUTHORS";
      `P "Leonid Rozenberg <leonidr@gmail.com>"; `Noblank;
      `S "BUGS";
      `P "Browse and report new issues at"; `Noblank;
      `P "<https://github.com/hammerlab/biokepi>.";
    ] in
    Term.( ret (const (`Help (`Plain, None)))
         , info "hla_typer" ~version ~doc ~man)
  in
  match Term.eval_choice default_cmd [print; exec] with
  | `Ok () -> ()
  | `Error _ -> failwithf "cmdliner error"
  | `Version | `Help -> exit 0
