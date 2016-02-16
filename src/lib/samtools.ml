
open Common

open Run_environment
open Workflow_utilities


let sam_to_bam ~(run_with : Machine.t) file_t =
  let open KEDSL in
  let samtools = Machine.get_tool run_with Tool.Default.samtools in
  let src = file_t#product#path in
  let dest = sprintf "%s.%s" (Filename.chop_suffix src ".sam") "bam" in
  let program =
    Program.(Tool.(init samtools) && exec ["samtools"; "view"; "-b"; "-o"; dest; src])
  in
  let name = sprintf "sam-to-bam-%s" (Filename.chop_suffix src ".sam") in
  let make = Machine.run_program ~name run_with program in
  let host = Machine.(as_host run_with) in
  workflow_node ~name
    (bam_file dest ~host)
    ~make
    ~edges:[
      depends_on file_t;
      depends_on Tool.(ensure samtools);
      on_failure_activate (Remove.file ~run_with dest);
      on_success_activate (Remove.file ~run_with src);
    ]

let faidx ~(run_with:Machine.t) fasta =
  let open KEDSL in
  let samtools = Machine.get_tool run_with Tool.Default.samtools in
  let src = fasta#product#path in
  let dest = sprintf "%s.%s" src "fai" in
  let program =
    Program.(Tool.(init samtools) && exec ["samtools"; "faidx"; src]) in
  let name = sprintf "samtools-faidx-%s" Filename.(basename src) in
  let make = Machine.run_program ~name run_with program in
  let host = Machine.(as_host run_with) in
  workflow_node
    (single_file dest ~host) ~name ~make
    ~edges:[
      depends_on fasta;
      depends_on Tool.(ensure samtools);
      on_failure_activate (Remove.file ~run_with dest);
    ]

let bgzip ~run_with input_file output_path =
  let open KEDSL in
  let samtools = Machine.get_tool run_with Tool.Default.samtools in
  let program =
    Program.(Tool.(init samtools)
             && shf "bgzip %s -c > %s" input_file#product#path output_path) in
  let name =
    sprintf "samtools-bgzip-%s" Filename.(basename input_file#product#path) in
  let make = Machine.run_program ~name run_with program in
  let host = Machine.(as_host run_with) in
  workflow_node
    (single_file output_path ~host) ~name ~make
    ~edges:[
      depends_on input_file;
      depends_on Tool.(ensure samtools);
      on_failure_activate (Remove.file ~run_with output_path);
    ]

let tabix ~run_with ~tabular_format input_file =
  let open KEDSL in
  let samtools = Machine.get_tool run_with Tool.Default.samtools in
  let output_path = input_file#product#path ^ ".tbi" in
  let minus_p_argument =
    match tabular_format with
    | `Gff -> "gff"
    | `Bed -> "bed"
    | `Sam -> "sam"
    | `Vcf -> "vcf"
    | `Psltab -> "psltab" in
  let program =
    Program.(
      Tool.(init samtools)
      && shf "tabix -p %s %s"
        minus_p_argument
        input_file#product#path
    ) in
  let name =
    sprintf "samtools-tabix-%s" Filename.(basename input_file#product#path) in
  let make = Machine.run_program ~name run_with program in
  let host = Machine.(as_host run_with) in
  workflow_node
    (single_file output_path ~host) ~name ~make
    ~edges:[
      depends_on input_file;
      depends_on Tool.(ensure samtools);
      on_failure_activate (Remove.file ~run_with output_path);
    ]


let do_on_bam
    ~(run_with:Machine.t)
    ?(more_depends_on=[]) ~name input_bam ~product ~make_command =
  let open KEDSL in
  let samtools = Machine.get_tool run_with Tool.Default.samtools in
  let src = input_bam#product#path in
  let sub_command = make_command src product#path in
  let program =
    Program.(Tool.(init samtools) && exec ("samtools" :: sub_command)) in
  let make = Machine.run_program ~name run_with program in
  workflow_node product ~name ~make
    ~edges:(
      depends_on Tool.(ensure samtools)
      :: depends_on input_bam
      :: on_failure_activate (Remove.file ~run_with product#path)
      :: more_depends_on)

let sort_bam_no_check ~(run_with:Machine.t) ?(processors=1) ~by input_bam =
  let source = input_bam#product#path in
  let dest_suffix =
    match by with
    | `Coordinate -> "sorted"
    | `Read_name -> "read-name-sorted"
  in
  let dest_prefix =
    sprintf "%s-%s" (Filename.chop_suffix source ".bam") dest_suffix in
  let product =
    KEDSL.bam_file ~sorting:by
      ~host:Machine.(as_host run_with)
      (sprintf "%s.%s" dest_prefix "bam") in
  let make_command src des =
    let command = ["-@"; Int.to_string processors; src; dest_prefix] in
    match by with
    | `Coordinate -> "sort" :: command
    | `Read_name -> "sort" :: "-n" :: command
  in
  do_on_bam ~run_with input_bam ~product ~make_command
    ~name:(sprintf "Samtools-sort %s"
             Filename.(basename input_bam#product#path))

(**
   Uses ["samtools sort"] by coordinate if the [input_bam] is not tagged as
   “sorted by coordinate.”
   If it is indeed sorted the function returns the [input_bam] node as is.
*)
let sort_bam_if_necessary ~(run_with:Machine.t) ?(processors=1) input_bam =
  match input_bam#product#sorting with
  | Some `Coordinate -> input_bam
  | other ->
    sort_bam_no_check ~run_with input_bam ~processors ~by:`Coordinate

let index_to_bai ~(run_with:Machine.t) input_bam =
  let product =
    KEDSL.single_file  ~host:(Machine.as_host run_with)
      (sprintf "%s.%s" input_bam#product#path "bai") in
  let make_command src des = ["index"; "-b"; src] in
  do_on_bam ~run_with input_bam ~product ~make_command
    ~name:(sprintf "Samtools-index %s"
             Filename.(basename input_bam#product#path))

let mpileup ~run_with ~reference_build ?adjust_mapq ~region input_bam =
  let open KEDSL in
  let samtools = Machine.get_tool run_with Tool.Default.samtools in
  let src = input_bam#product#path in
  let adjust_mapq_option = 
    match adjust_mapq with | None -> "" | Some n -> sprintf "-C%d" n in
  let samtools_region_option = Region.to_samtools_option region in
  let reference_genome = Machine.get_reference_genome run_with reference_build in
  let fasta = Reference_genome.fasta reference_genome in
  let pileup =
    Filename.chop_suffix src ".bam" ^
    sprintf "-%s%s.mpileup" (Region.to_filename region) adjust_mapq_option
  in
  let sorted_bam =
    sort_bam_if_necessary ~run_with input_bam ~by:`Coordinate in
  let program =
    Program.(
      Tool.(init samtools)
      && shf
        "samtools mpileup %s %s -Bf %s %s > %s"
        adjust_mapq_option samtools_region_option 
        fasta#product#path
        sorted_bam#product#path
        pileup
    ) in
  let name =
    sprintf "samtools-mpileup-%s" Filename.(basename pileup |> chop_extension)
  in
  let make = Machine.run_program ~name run_with program in
  let host = Machine.(as_host run_with) in
  let edges = [
    depends_on Tool.(ensure samtools);
    depends_on sorted_bam;
    depends_on fasta;
    index_to_bai ~run_with sorted_bam |> depends_on;
    on_failure_activate (Remove.file ~run_with pileup);
  ] in
  workflow_node ~name (single_file pileup ~host) ~make ~edges 
