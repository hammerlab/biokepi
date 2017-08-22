open Biokepi_run_environment
open Common

module Remove = Workflow_utilities.Remove

(* To convert (old) 64-based phred scores to (new) 33-based ones. *)
let shift_phred_scores ~(run_with:Machine.t)
    ?(output_postfix=".phred33.fastq")
    ~input_fastq
    ~output_folder
  =
  let open KEDSL in
  let seqtk = Machine.get_tool run_with Machine.Tool.Default.seqtk in
  let output_path rpath = 
    let fqr_basename =
        rpath |> Filename.basename |> Filename.chop_extension
    in
    output_folder // (sprintf "%s%s" fqr_basename output_postfix)
  in
  let r1_path, r2_path_opt = input_fastq#product#paths in
  let cmds =
    let shift_cmd rpath = 
      sprintf "seqtk seq -VQ64 %s > %s" rpath (output_path rpath)
    in
    match r2_path_opt with
    | Some r2_path -> [ shift_cmd r1_path; shift_cmd r2_path; ]
    | None -> [shift_cmd r1_path;]
  in
  let out_fastq = 
    transform_fastq_reads
      input_fastq#product
      (output_path r1_path)
      (Option.map ~f:output_path r2_path_opt)
  in
  let name =
    sprintf "PHRED 64->31: %s" input_fastq#product#sample_name
  in
  let make =
    Machine.(run_program run_with
      ~requirements:[
        `Self_identification ["seqtk:seq"; "fastq"; "phred_fix"; ]
      ]
      Program.(
        Tool.init seqtk
        && shf "mkdir -p %s" output_folder
        && chain (List.map ~f:sh cmds)
      )
    )
  in
  workflow_node ~name ~make out_fastq
    ~edges: [
      on_failure_activate (Remove.directory ~run_with output_folder);
      depends_on (Machine.Tool.ensure seqtk);
      depends_on input_fastq;
    ]

