
open Biokepi_run_environment
open Common

let vcf ~name ~path = object method name = name method path = path end

let run ~(run_with:Machine.t) ~reference_genome
    ~normal_bam_path
    ~tumor_bam_path
    ~run_id
    ?rna_bam_path
    ~vcfs
    ~output_path
    () =
  let open KEDSL in
  let open Ketrew_pure.Target.Volume in
  let igvxml = Machine.get_tool run_with Machine.Tool.Default.igvxml in
  let name = sprintf "Create %s" (Filename.basename output_path) in
  let make =
    Machine.quick_run_program run_with
      ~name
      ~requirements:[`Self_identification ["igvxml"]]
      Program.(
        Machine.Tool.(init igvxml)
        && exec ["mkdir"; "-p"; (Filename.dirname output_path)]
        && exec (
          ["igvxml";
           "-G"; reference_genome;
           "--output"; output_path;
           "-N"; normal_bam_path;
           "-T"; tumor_bam_path;
           "--run-id"; run_id;]
          @ (Option.value_map ~default:[] rna_bam_path ~f:(fun p ->
              ["-R"; p]))
          @ ["--vcfs";
             List.map vcfs ~f:(fun v ->
                 sprintf "%s=%s" v#name v#path)
             |> String.concat ~sep:","]
        )
      )
  in
  workflow_node ~name ~make
    (single_file output_path ~host:(Machine.as_host run_with))
    ~edges: [
      on_failure_activate (Workflow_utilities.Remove.file ~run_with output_path);
      depends_on (Machine.Tool.ensure igvxml);
    ]
