open Biokepi_run_environment
open Common

module Remove = Workflow_utilities.Remove

type product = <
  host : Ketrew_pure.Host.t;
  is_done : Ketrew_pure.Target.Condition.t option;
  csv_stats : KEDSL.single_file;
  html_stats : KEDSL.single_file;
  workdir_path : string;
  annotated_vcf : KEDSL.vcf_file;
  path : string; >


let default_region_size = 10000 (* 10kb as suggested by the authors *)

let access 
    ~(run_with:Machine.t) 
    ?(region_size=default_region_size)
    ~reference_build
  =
  let open KEDSL in
  let cnvkit = Machine.get_tool run_with Machine.Tool.Default.cnvkit in
  let host = Machine.as_host run_with in
  let reference_fasta =
    Machine.get_reference_genome run_with reference_build
    |> Reference_genome.fasta
  in
  let refpath = reference_fasta#product#path in
  let idxpath =
    (Filename.basename refpath) //
      (sprintf "cnvkit-access-%d-%s.bed" region_size reference_build)
  in
  let name =
    sprintf "cnvkit-idx-accessible-%s-%d" reference_build region_size
  in
  let access_idx_cmd =
    sprintf "cnvkit.py access %s -s %d -o %s"
      refpath region_size idxpath
  in
  let make =
    Machine.(run_program run_with
      Program.(Tool.init cnvkit && sh access_idx_cmd))
  in
  workflow_node (single_file ~host idxpath)
    ~name ~make
    ~edges: [
      on_failure_activate (Remove.file ~run_with idxpath);
      depends_on (Machine.Tool.ensure cnvkit);
      depends_on reference_fasta;
    ]


let prepare_targets ~(run_with:Machine.t) ~reference_build =
  let open KEDSL in
  let host = Machine.as_host run_with in
  let reference_transcripts =
    Machine.get_reference_genome run_with reference_build
    |> Reference_genome.cdna_exn
  in
  let txpath = reference_transcripts#product#path in
  let target_path = (Filename.chop_extension txpath) ^ "-merged.bed" in
  let name = "%s: generate target regions from reference transcript" in
  let bedtools = Machine.get_tool run_with Machine.Tool.Default.bedtools in
  let gff2bed_cmd =
    sprintf "bedtools sort -i %s | bedtools merge -bed > %s"
      txpath target_path
  in
  let make =
    Machine.(run_program run_with
      Program.(Tool.init bedtools && sh gff2bed_cmd))
  in
  workflow_node (single_file ~host target_path)
    ~name ~make
    ~edges: [
      on_failure_activate (Remove.file ~run_with target_path);
      depends_on (Machine.Tool.ensure bedtools);
      depends_on reference_transcripts;
    ]



let batch ~(run_with:Machine.t)
    ?(region_size=default_region_size)
    ~normal_bams ~tumor_bams
    ~reference_build
    ~run_name
    ~output_folder
  =
  let open KEDSL in
  let cnvkit = Machine.get_tool run_with Machine.Tool.Default.cnvkit in
  let host = Machine.as_host run_with in
  let reference_fasta =
    Machine.get_reference_genome run_with reference_build
    |> Reference_genome.fasta
  in
  let refpath = reference_fasta#product#path in
  let name = sprintf "Infer CNV: %s (%s)" run_name reference_build in
  let accesswf = access ~run_with ~region_size ~reference_build in
  let targetwf = prepare_targets ~run_with ~reference_build in
  let batch_cmd =
    let q = Filename.quote in
    let bams_to_paths bams =
      bams
      |> List.map ~f:(fun b -> b#product#path)
      |> List.map ~f:q
      |> String.concat ~sep:" "
    in
    let nbampaths = bams_to_paths normal_bams in
    let tbampaths = bams_to_paths tumor_bams in
    let tpath = targetwf#product#path in
    let apath = accesswf#product#path in
    sprintf "cnvkit.py batch %s --normal %s \
             --targets %s --fasta %s --access %s \
             --output-dir %s --diagram --scatter"
            nbampaths tbampaths
            (q tpath) (q refpath) (q apath)
            output_folder
  in
  let make =
    Machine.(run_program run_with
      Program.(
        Tool.init cnvkit
        && shf "mkdir -p %s" output_folder
        && sh batch_cmd
      )
    )
  in
  workflow_node (single_file ~host (output_folder // "reference.cnn"))
    ~name ~make
    ~edges:([
      on_failure_activate (Remove.directory ~run_with output_folder);
      depends_on (Machine.Tool.ensure cnvkit);
      depends_on reference_fasta;
      depends_on accesswf;
      depends_on targetwf;
    ] @ List.map ~f:(fun b -> depends_on b) (normal_bams @ tumor_bams))

