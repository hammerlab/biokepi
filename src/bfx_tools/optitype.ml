open Biokepi_run_environment
open Common


type product = <
  host: Ketrew_pure.Host.t;
  is_done : Ketrew_pure.Target.Condition.t option ;
  path: string >

let move_optitype_product ?host ~path o =
  let host = match host with
  | None -> o#host
  | Some h -> h
  in
  let vol =
    let open Ketrew_pure.Target.Volume in
    create (dir (Filename.basename path) []) ~host
      ~root:(Ketrew_pure.Path.absolute_directory_exn (Filename.dirname path))
  in
  object
    method host = host
    method is_done = Some (`Volume_exists vol)
    method path = path
  end

let get_optitype_data_folder =
  let tname, tversion =
    let tool_def = Machine.Tool.Default.optitype in
    Machine.Tool.Definition.(get_name tool_def, get_version tool_def)
  in
  sprintf "${CONDA_PREFIX}/share/%s-%s/"
    tname (match tversion with None -> "*" | Some v -> v)

(* copy sample config file and the required data over;
   then, adjust the razers3 path in the config *)
let prepare_optidata = 
  let open KEDSL.Program in
  let optidata_path = get_optitype_data_folder in
  shf "cp -r %s/data ." optidata_path && (* HLA reference data *)
  shf "cp -r %s/config.ini.example config.ini" optidata_path && 
  sh "sed -i.bak \"s|\\/path\\/to\\/razers3|$(which razers3)|g\" config.ini"

  
(**
   Run OptiType in [`RNA] or [`DNA] mode.

   Please provide a fresh [work_dir] directory, it will be deleted in case of
   failure.
*)
let hla_type ~work_dir ~run_with ~fastq ~run_name nt
  : product KEDSL.workflow_node
  =
  let tool = Machine.get_tool run_with Machine.Tool.Default.optitype in
  let r1_path, r2_path_opt = fastq#product#paths in
  let name = sprintf "optitype-%s" run_name in
  let make =
    Machine.run_big_program run_with ~name
      ~self_ids:["optitype"]
      KEDSL.Program.(
        Machine.Tool.init tool
        && exec ["mkdir"; "-p"; work_dir]
        && exec ["cd"; work_dir]
        && prepare_optidata
        && shf "OptiTypePipeline.py --verbose -c ./config.ini \
                --input %s %s %s -o %s"
            (Filename.quote r1_path)
            (Option.value_map ~default:"" r2_path_opt ~f:Filename.quote)
            (match nt with | `DNA -> "--dna" | `RNA -> "--rna")
            run_name)
  in
  let product =
    let host = Machine.as_host run_with in
    let vol =
      let open Ketrew_pure.Target.Volume in
      create (dir run_name []) ~host
        ~root:(Ketrew_pure.Path.absolute_directory_exn work_dir)
    in
    object
      method is_done = Some (`Volume_exists vol)
      method path = work_dir
      method host = host
    end
  in
  KEDSL.workflow_node product ~name ~make
    ~edges:(
      [
        KEDSL.depends_on (Machine.Tool.ensure tool);
        KEDSL.depends_on fastq;
        KEDSL.on_failure_activate
          (Workflow_utilities.Remove.directory ~run_with work_dir);
      ]
    )

(*
  Optitype depends on alignment of reads onto the HLA-locus
  as a preliminary filtering step, but the default aligner, razers3,
  is so memory hungry that, the run fails on a ~50G memory machine
  when the number of reads per FASTQ is approximately more than 200M.

  The following variation of optitype run makes use of `bwa mem` instead
  of `razers3` to do the initial filtering and some experimentation
  with real patient data proved that this doesn't affect the results
  despite their worrisome warning on the site.

  This is only tested on the DNA arm of the pipeline, so is restricted
  to that use case.
*)
let dna_hla_type_with_bwamem
    ?(configuration = Bwa.Configuration.Mem.default)
    ~work_dir ~run_with ~fastq ~run_name
  =
  let open KEDSL in
  (* We need to pull in bwa mem and samtools to get some help *)
  let optitype = Machine.get_tool run_with Machine.Tool.Default.optitype in
  let bwa = Machine.get_tool run_with Machine.Tool.Default.bwa in
  let samtools = Machine.get_tool run_with Machine.Tool.Default.samtools in
  let bwa_wd = work_dir // "bwamem" in
  let dna_hla_ref_path = 
      get_optitype_data_folder // "data/hla_reference_dna.fasta"
  in
  (* Step 1: prepare hla reference indexes *)
  let index_hla_wf =
    let name = "Index OptiType's DNA-based HLA reference with bwa" in
    let edges = [
        depends_on (Machine.Tool.ensure bwa);
        depends_on (Machine.Tool.ensure optitype);
        on_failure_activate
          (Workflow_utilities.Remove.directory ~run_with bwa_wd);]
    in
    let make = 
      Machine.run_big_program run_with ~name
        ~self_ids:["optitype"; "hla"; "dna"; "bwa index"]
        Program.(
          Machine.Tool.init bwa
          && Machine.Tool.init optitype
          && shf "mkdir -p %s" bwa_wd
          && shf "bwa index %s" dna_hla_ref_path
        )
    in
    let product = 
      Workflow_utilities.Variable_tool_paths.single_file 
        ~run_with ~tool:optitype (dna_hla_ref_path ^ ".bwt")
    in
    workflow_node product ~name ~make ~edges
  in
  (* Step 2: Map the input fastq to this pseudo reference genome via
     `bwa mem` and turn the alignment back into fastq while keeping
     only the reads that mapped to reduce OptiType's future memory
     consumption.
  *)
  let filter_hla_reads_wf =
    let name = sprintf "Filter out non-HLA-mapping reads: %s" run_name in
    let edges = [
        depends_on (Machine.Tool.ensure bwa);
        depends_on (Machine.Tool.ensure samtools);
        depends_on index_hla_wf; (* No indexing, no mapping *)
        depends_on fastq;
        on_failure_activate
          (Workflow_utilities.Remove.directory ~run_with bwa_wd);]
    in
    let bwa2sam2fastq fqpath = (* the whole pipeline *)
      let outfq_path = 
        let fqbase = fqpath |> Filename.basename |> Filename.chop_extension in
        bwa_wd // (sprintf "%s-hla_mapping.fastq" fqbase)
      in
      let processors = Machine.max_processors run_with in
      let bwamem_part = Bwa.(
        sprintf "bwa mem -t %d -O %d -E %d -B %d %s %s"
          processors
          configuration.Configuration.Mem.gap_open_penalty
          configuration.Configuration.Mem.gap_extension_penalty
          configuration.Configuration.Mem.mismatch_penalty
          (Filename.quote dna_hla_ref_path)
          (Filename.quote fqpath))
      in
      let samtools_part =
        (* -F4 filters *out* all reads that do not map. 
           See SAM/BAM flags for more information:
              $ samtools flags
        *)
        sprintf "samtools fastq -F4 - -0 %s" outfq_path
      in
      String.concat ~sep:" | " [bwamem_part; samtools_part],
      outfq_path
    in
    let in_r1, in_r2_opt = fastq#product#paths in
    let filter_r1, out_r1 = bwa2sam2fastq in_r1 in
    let filter_r2, out_r2 = 
      match in_r2_opt with
      | None -> "echo 'Second pair is missing'", None
      | Some r2p -> let (f, o) = bwa2sam2fastq r2p in (f, Some o)
    in
    let make =
      Machine.run_big_program run_with ~name
        ~self_ids:["optitype"; "hla"; "dna"; "filtering"]
        Program.(
          Machine.Tool.init bwa &&
          Machine.Tool.init samtools &&
          shf "mkdir -p %s" bwa_wd &&
          sh filter_r1 && sh filter_r2
        )
    in
    let product = transform_fastq_reads fastq#product out_r1 out_r2 in
    workflow_node product ~name ~make ~edges
  in
  (* Step 3: Run OptiType as usual on the new filtered down FASTQ(s) *)
  hla_type ~work_dir ~run_with ~fastq:filter_hla_reads_wf ~run_name `DNA