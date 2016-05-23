open Biokepi_run_environment
open Common

let run ~(run_with: Machine.t) 
  ?(min_reads=2) ?(protein_sequence_length=30)
  ~vcf ~bam ~reference_build ~output_file =
  let open KEDSL in
  let isovar_tool =
    Machine.get_tool run_with Machine.Tool.Definition.(create "isovar")
  in
  let genome = Machine.(get_reference_genome run_with reference_build)
    |> Reference_genome.name
  in
  let name = sprintf "isovar_%s" (Filename.basename output_file) in
  let isovar_cmd = sprintf "isovar-protein-sequences.py \
                            --vcf %s \
                            --bam %s \
                            --genome %s \
                            --min-reads %d \
                            --protein-sequence-length %d \
                            --output %s"
                    vcf bam genome min_reads protein_sequence_length output_file
  in
  workflow_node (single_file output_file ~host:Machine.(as_host run_with))
    ~name
    ~edges:[
      depends_on Machine.Tool.(ensure isovar_tool);
    ]
    ~make:(
      Machine.run_program run_with ~name
        Program.(
          Machine.Tool.(init isovar_tool)
          && sh isovar_cmd
        )
    )