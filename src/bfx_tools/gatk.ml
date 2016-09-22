open Biokepi_run_environment
open Common

module Remove = Workflow_utilities.Remove


module Configuration = struct

  module Gatk_config () = struct
    type t = {
      (** The name of the configuration, specific to Biokepi. *)
      name: string;

      (** MalformedReadFilter options.

         This filter is applied automatically by all GATK tools in order to protect them
         from crashing on reads that are grossly malformed. There are a few
         issues (such as the absence of sequence bases) that will cause the run
         to fail with an error, but these cases can be preempted by setting
         flags that cause the problem reads to also be filtered. *)
      (** Ignore reads with CIGAR containing the N operator, instead of failing
         with an error *)
      filter_reads_with_n_cigar: bool;
      (** Ignore reads with mismatching numbers of bases and base qualities,
         instead of failing with an error.*)
      filter_mismatching_base_and_quals: bool;
      (** Ignore reads with no stored bases (i.e. '*' where the sequence should
         be), instead of failing with an error *)
      filter_bases_not_stored: bool;

      (** Other parameters: *)
      parameters: (string * string) list;
    }

    let name t = t.name

    let to_json t: Yojson.Basic.json =
      let {name;
           filter_reads_with_n_cigar;
           filter_mismatching_base_and_quals;
           filter_bases_not_stored;
           parameters} = t in
      `Assoc [
        "name", `String name;
        "filter_reads_with_N_cigar", `Bool filter_reads_with_n_cigar;
        "filter_mismatching_base_and_quals", `Bool filter_mismatching_base_and_quals;
        "filter_bases_not_stored", `Bool filter_bases_not_stored;
        "parameters",
        `Assoc (List.map parameters ~f:(fun (a, b) -> a, `String b));
      ]

    let render {name;
                filter_reads_with_n_cigar;
                filter_mismatching_base_and_quals;
                filter_bases_not_stored;
                parameters} =
      (if filter_reads_with_n_cigar
       then "--filter_reads_with_N_cigar" else "") ::
      (if filter_mismatching_base_and_quals
       then "--filter_mismatching_base_and_quals" else "") ::
      (if filter_bases_not_stored
       then "--filter_bases_not_stored" else "") ::
      List.concat_map parameters ~f:(fun (a, b) -> [a; b])
      |> List.filter ~f:(fun s -> not (String.is_empty s))

    let default =
      {name = "default";
       filter_reads_with_n_cigar = false;
       filter_mismatching_base_and_quals = false;
       filter_bases_not_stored = false;
       parameters = []}
  end

  module Indel_realigner = struct
    include Gatk_config ()
  end

  module Realigner_target_creator = struct
    include Gatk_config ()
  end

  module Bqsr = struct
    include Gatk_config ()
  end

  module Print_reads = struct
    include Gatk_config ()
  end

  type indel_realigner = (Indel_realigner.t * Realigner_target_creator.t)
  type bqsr = (Bqsr.t * Print_reads.t)

  let default_indel_realigner = (Indel_realigner.default, Realigner_target_creator.default)
  let default_bqsr = (Bqsr.default, Print_reads.default)


  module Mutect2 = struct
    type t = {
      name: string;
      use_dbsnp: bool;
      use_cosmic: bool;
      additional_arguments: string list;
    }
    let create
        ?(use_dbsnp = true) ?(use_cosmic = true) name additional_arguments =
      {name; use_dbsnp; use_cosmic; additional_arguments}

    let to_json {name; use_dbsnp; use_cosmic; additional_arguments}
      : Yojson.Basic.json =
    `Assoc [
      "name", `String name;
      "use-cosmic", `Bool use_cosmic;
      "use-dbsnp", `Bool use_dbsnp;
      "additional-arguments",
      `List (List.map additional_arguments ~f:(fun s -> `String s));
    ]

    let default = create "default" []
    
    let default_without_cosmic =
      create ~use_cosmic:false ~use_dbsnp:true
      "default_without_cosmic" []

    let compile ~reference {name; use_dbsnp; use_cosmic; additional_arguments} =
      let with_db use opt_name get_exn =
        if not use then None
        else
          let node = get_exn reference in
          Some ( [opt_name; node#product#path], [KEDSL.depends_on node])
      in
      let args, edges =
        List.filter_opt [
          with_db use_dbsnp  "--dbsnp" Reference_genome.dbsnp_exn;
          with_db use_cosmic  "--cosmic" Reference_genome.cosmic_exn;
        ]
        |> List.split
      in
      (`Arguments (List.concat args @ additional_arguments),
       `Edges (List.concat edges))

    let name t = t.name
  end


end

  (*
     For now we have the two steps in the same target but this could
     be split in two.
     c.f. https://www.broadinstitute.org/gatk/guide/tooldocs/org_broadinstitute_gatk_tools_walkers_indels_IndelRealigner.php

     We want to be able to run the indel-realigner on mutliple bams, so we
     cannot use the usual `~result_prefix` argument:
     See the documentation for the `--nWayOut` option:
     https://www.broadinstitute.org/gatk/guide/tooldocs/org_broadinstitute_gatk_tools_walkers_indels_IndelRealigner.php#--nWayOut
     See also
     http://gatkforums.broadinstitute.org/gatk/discussion/5588/best-practice-for-multi-sample-non-human-indel-realignment

     Also, the documentation is incomplete (or buggy), the option `--nWayOut`
     will output the Bam files in the current directory (i.e. the one GATK is
     running in).
     So, unless the user uses the `?run_directory` option, we extract that
     directory from the input-bams; if they do not coincide we consider this an
     error.


     On top of that we use the GADT `_ KEDSL.bam_orf_bams` to return have 2
     possible return types:
     bam_file workflow_node or bam_list workflow_node
  *)
open Configuration

(* We limit this to 20 characters to attempt to keep the length of the resulting
   filenames below the common maximum length of 255. *)
let indel_realigner_output_filename_tag
    ~configuration:(ir_config, target_config)
    ?region input_bams =
  let digest_of_input =
    (List.map input_bams ~f:(fun o -> o#product#path)
     @ [ir_config.Configuration.Indel_realigner.name;
        target_config.Configuration.Realigner_target_creator.name;
        Option.value_map ~f:(fun r -> "-" ^ Region.to_filename r) region ~default:""])
    |> String.concat ~sep:""
    (* we make this file “unique” with an MD5 sum of the input paths *)
    |> Digest.string |> Digest.to_hex in
  (String.take digest_of_input ~index:11) ^ "-indelreal"

let indel_realigner :
  type a.
  ?compress:bool ->
  ?on_region: Region.t ->
  configuration:(Indel_realigner.t * Realigner_target_creator.t) ->
  run_with:Machine.t ->
  ?run_directory: string ->
  a KEDSL.bam_or_bams ->
  a =
  fun ?(compress=false)
    ?(on_region = `Full)
    ~configuration ~run_with ?run_directory
    input_bam_or_bams ->
    let open KEDSL in
    let input_bam_1, more_input_bams = (* this an at-least-length-1 list :)  *)
      match input_bam_or_bams with
      | Single_bam bam -> bam, []
      | Bam_workflow_list [] ->
        failwithf "Empty bam-list in Gatk.indel_realigner`"
      | Bam_workflow_list (one :: more) -> (one, more)
    in
    let run_directory =
      match run_directory with
      | None ->
        let dir = Filename.dirname input_bam_1#product#path in
        List.iter more_input_bams ~f:(fun bam ->
            if Filename.dirname bam#product#path <> dir then
              failwithf "These two BAMS are not in the same directory:\n\
                        \    %s\n\
                        \    %s\n\
                         GATK.indel_realigner when running on multiple bams \
                         requires a proper run-directory, clean-up your bams \
                         or provide the option ~run_directory
                      " input_bam_1#product#path bam#product#path
          );
        dir
      | Some rundir -> rundir
    in
    let indel_config, target_config = configuration in
    let input_sorted_bam_1 =
      Samtools.sort_bam_if_necessary
        ~run_with ~by:`Coordinate input_bam_1 in
    let more_input_sorted_bams =
      List.map more_input_bams
        ~f:(Samtools.sort_bam_if_necessary
              ~run_with ~by:`Coordinate) in
    let more_input_bams = `Use_the_sorted_ones_please in
    let input_bam_1 = `Use_the_sorted_ones_please in
    ignore (more_input_bams, input_bam_1);
    let name =
      sprintf "Indel Realignment on %s"
        (Filename.basename input_sorted_bam_1#product#path)
    in
    let gatk = Machine.get_tool run_with Machine.Tool.Default.gatk in
    let reference_genome =
      let reference_build = input_sorted_bam_1#product#reference_build in
      Machine.get_reference_genome run_with reference_build in
    let fasta = Reference_genome.fasta reference_genome in
    let output_suffix =
      indel_realigner_output_filename_tag
        ~configuration ~region:on_region
        (input_sorted_bam_1 :: more_input_sorted_bams)
    in
    let intervals_file =
      Filename.chop_suffix input_sorted_bam_1#product#path ".bam"
      ^ output_suffix ^ ".intervals" in
    (* This function encodes how IndelRealign's nWayOut names the output BAMs,
       including the directory it'll end up placing them in. *)
    let output_bam_path input =
      run_directory // (
        Filename.chop_extension input#product#path ^ output_suffix ^ ".bam"
        |> Filename.basename)
    in
    let processors = Machine.max_processors run_with in
    let make =
      let target_creation_args =
        [
          "-R"; Filename.quote fasta#product#path;
          "-I"; Filename.quote input_sorted_bam_1#product#path;
          "-o"; Filename.quote intervals_file;
          "-nt"; Int.to_string processors;
        ]
        @ Realigner_target_creator.render target_config
        @ List.concat_map more_input_sorted_bams
          ~f:(fun bam -> ["-I"; Filename.quote bam#product#path])
      in
      let indel_real_args =
        [ "-R"; fasta#product#path;
          "-I"; input_sorted_bam_1#product#path;
          "-targetIntervals"; intervals_file;
        ] @ Indel_realigner.render indel_config @
        begin match more_input_sorted_bams with
        | [] ->
          ["-o"; output_bam_path input_sorted_bam_1]
        | more ->
          List.concat_map more
            ~f:(fun b -> ["-I"; Filename.quote b#product#path])
          @ ["--nWayOut"; output_suffix ^ ".bam"]
        end
      in
      let intervals_option = Region.to_gatk_option on_region in
      Machine.run_big_program run_with ~name ~processors
        ~self_ids:["gatk"; "indel-realigner"]
        Program.(
          Machine.Tool.(init gatk)
          && shf "cd %s" (Filename.quote run_directory)
          && shf "java -jar $GATK_JAR -T RealignerTargetCreator %s %s"
            intervals_option
            (String.concat ~sep:" " target_creation_args)
          && sh ("java -jar $GATK_JAR -T IndelRealigner "
                 ^ intervals_option
                 ^ (if compress then " " else " -compress 0 ")
                 ^ (String.concat ~sep:" " indel_real_args)))
    in
    let edges =
      let sequence_dict = (* implicit dependency *)
        Picard.create_dict ~run_with fasta in
      [
        depends_on Machine.Tool.(ensure gatk);
        depends_on fasta;
        (* RealignerTargetCreator wants the `.fai`: *)
        depends_on (Samtools.faidx ~run_with fasta);
        depends_on sequence_dict;
        on_failure_activate (Remove.file ~run_with intervals_file);
      ]
      @ List.concat_map (input_sorted_bam_1 :: more_input_sorted_bams) ~f:(fun b -> [
            depends_on b;
            depends_on (Samtools.index_to_bai ~run_with b);
            on_failure_activate (Remove.file ~run_with (output_bam_path b));
          ])
    in
    let node : type a. a bam_or_bams -> a =
      (* we need a function to force `type a.` *)
      function
      | Single_bam _ ->
        (* This is what we give to the `-o` option: *)
        workflow_node  ~name ~make ~edges
          (transform_bam input_sorted_bam_1#product
             (output_bam_path input_sorted_bam_1))
      | Bam_workflow_list _ ->
        workflow_node  ~name ~make ~edges
          (bam_list
             (List.map (input_sorted_bam_1 :: more_input_sorted_bams)
                ~f:(fun b ->
                    (* This is what the documentation says it will to
                       with the `--nWayOut` option *)
                    transform_bam b#product (output_bam_path b))))
    in
    node input_bam_or_bams

let indel_realigner_map_reduce :
  type a.
  ?compress:bool ->
  configuration:(Indel_realigner.t * Realigner_target_creator.t) ->
  run_with:Machine.t ->
  ?run_directory: string ->
  a KEDSL.bam_or_bams ->
  a =
  fun ?compress
    ~configuration ~run_with
    ?run_directory
    input_bam_or_bams ->
    let open KEDSL in
    begin match input_bam_or_bams with
    | Single_bam bam_node ->
      let all_nodes =
        let f on_region =
          indel_realigner ?compress
            ~on_region
            ~configuration ~run_with ?run_directory
            input_bam_or_bams
        in
        let reference =
          Machine.get_reference_genome run_with
            bam_node#product#reference_build in
        List.map ~f (Reference_genome.major_contigs reference)
      in
      let result_path =
        Filename.chop_extension bam_node#product#path
        ^ indel_realigner_output_filename_tag
          ~configuration [bam_node]
        ^ "-merged.bam"
      in
      Samtools.merge_bams ~run_with all_nodes result_path
    | Bam_workflow_list bams ->
      let all_nodes =
        (* A list of lists that looks like:
           [
             [bam1_reg1; bam2_reg1; bam3_reg1];
             [bam1_reg2; bam2_reg2; bam3_reg2];
             [bam1_reg3; bam2_reg3; bam3_reg3];
             [bam1_reg4; bam2_reg4; bam3_reg4];
           ]
        *)
        let f on_region =
          let bam_list_node =
            indel_realigner ?compress
              ~on_region
              ~configuration ~run_with ?run_directory
              input_bam_or_bams
          in
          let exploded = KEDSL.explode_bam_list_node bam_list_node in
          exploded
        in
        let reference =
          Machine.get_reference_genome run_with
            (List.hd_exn bams)#product#reference_build in
        List.map ~f (Reference_genome.major_contigs reference)
      in
      let merged_bams =
        List.mapi bams ~f:(fun index bam ->
            let all_regions_for_this_bam =
              List.map all_nodes ~f:(fun region_n ->
                  List.nth region_n index |>
                  Option.value_exn ~msg:"bug in Gatk.indel_realigner_map_reduce")
            in
            let result_path =
              Filename.chop_extension bam#product#path
              ^ sprintf "-%d-" index (* the index is there as debug/witness *)
              ^ indel_realigner_output_filename_tag
                ~configuration bams
              ^ "-merged.bam"
            in
            Samtools.merge_bams ~run_with all_regions_for_this_bam result_path
          )
      in
      workflow_node ~name:"Indel-realigner-map-reduce"
        ~edges:(List.map merged_bams ~f:depends_on)
        (bam_list (List.map merged_bams ~f:(fun n -> n#product)))
    end



(* Again doing two steps in one target for now:
   http://gatkforums.broadinstitute.org/discussion/44/base-quality-score-recalibrator
   https://www.broadinstitute.org/gatk/guide/tooldocs/org_broadinstitute_gatk_tools_walkers_bqsr_BaseRecalibrator.php
*)
let call_gatk ~analysis ?(region=`Full) args =
  let open KEDSL.Program in
  let escaped_args = List.map ~f:Filename.quote args in
  let intervals_option = Region.to_gatk_option region in
  sh (String.concat ~sep:" "
        ("java -jar $GATK_JAR -T " :: analysis :: intervals_option :: escaped_args))

let base_quality_score_recalibrator
    ~configuration:(bqsr_configuration, print_reads_configuration)
    ~run_with
    ~input_bam ~output_bam =
  let open KEDSL in
  let name = sprintf "gatk-%s" (Filename.basename output_bam) in
  let gatk = Machine.get_tool run_with Machine.Tool.Default.gatk in
  let reference_genome =
    Machine.get_reference_genome run_with input_bam#product#reference_build in
  let fasta = Reference_genome.fasta reference_genome in
  let db_snp = Reference_genome.dbsnp_exn reference_genome in
  let sorted_bam =
    Samtools.sort_bam_if_necessary
      ~run_with ~by:`Coordinate input_bam in
  let input_bam = `Please_use_the_sorted_one in ignore input_bam;
  let recal_data_table =
    Name_file.from_path ~readable_suffix:"bqsr_recal.table"
      sorted_bam#product#path [] in
  let processors = Machine.max_processors run_with in
  let make =
    Machine.run_big_program run_with ~name ~processors
      ~self_ids:["gatk"; "bqsr"]
      Program.(
        Machine.Tool.(init gatk)
        && call_gatk ~analysis:"BaseRecalibrator" ([
          "-nct"; Int.to_string processors;
          "-I"; sorted_bam#product#path;
          "-R"; fasta#product#path;
          "-knownSites"; db_snp#product#path;
          "-o"; recal_data_table;
        ] @ Configuration.Bqsr.render bqsr_configuration)
        && call_gatk ~analysis:"PrintReads" ([
          "-nct"; Int.to_string processors;
          "-R"; fasta#product#path;
          "-I"; sorted_bam#product#path;
          "-BQSR"; recal_data_table;
          "-o"; output_bam;
        ] @ Configuration.Print_reads.render print_reads_configuration)
      ) in
  workflow_node ~name (transform_bam sorted_bam#product ~path:output_bam)
    ~make
    ~edges:[
      depends_on Machine.Tool.(ensure gatk);
      depends_on fasta; depends_on db_snp;
      depends_on sorted_bam;
      depends_on (Samtools.index_to_bai ~run_with sorted_bam);
      on_failure_activate (Remove.file ~run_with output_bam);
      on_failure_activate (Remove.file ~run_with recal_data_table);
    ]


let haplotype_caller
    ?(more_edges = [])
    ~run_with ~input_bam ~result_prefix how =
  let open KEDSL in
  let reference =
    Machine.get_reference_genome run_with input_bam#product#reference_build in
  let run_on_region ~add_edges region =
    let result_file suffix =
      let region_name = Region.to_filename region in
      sprintf "%s-%s%s" result_prefix region_name suffix in
    let output_vcf = result_file "-germline.vcf" in
    let gatk = Machine.get_tool run_with Machine.Tool.Default.gatk in
    let run_path = Filename.dirname output_vcf in
    let reference_fasta = Reference_genome.fasta reference in
    let reference_dot_fai = Samtools.faidx ~run_with reference_fasta in
    let sequence_dict = Picard.create_dict ~run_with reference_fasta in
    let dbsnp = Reference_genome.dbsnp_exn reference in
    let sorted_bam =
      Samtools.sort_bam_if_necessary ~run_with ~by:`Coordinate input_bam in
    let run_gatk_haplotype_caller =
      let name = sprintf "%s" (Filename.basename output_vcf) in
      let make =
        Machine.run_big_program run_with ~name
          ~self_ids:["gatk"; "haplotype-caller"]
          Program.(
            Machine.Tool.(init gatk)
            && shf "mkdir -p %s" run_path
            && shf "cd %s" run_path
            && call_gatk ~region ~analysis:"HaplotypeCaller" [
              "-I"; sorted_bam#product#path;
              "-R"; reference_fasta#product#path;
              "--dbsnp"; dbsnp#product#path;
              "-o"; output_vcf;
              "--filter_reads_with_N_cigar";
            ]
          )
      in
      workflow_node ~name ~make
        (vcf_file output_vcf ~reference_build:input_bam#product#reference_build
           ~host:Machine.(as_host run_with))
        ~tags:[Target_tags.variant_caller]
        ~edges:(add_edges @ [
            depends_on Machine.Tool.(ensure gatk);
            depends_on sorted_bam;
            depends_on reference_fasta;
            depends_on dbsnp;
            depends_on reference_dot_fai;
            depends_on sequence_dict;
            depends_on (Samtools.index_to_bai ~run_with sorted_bam);
            on_failure_activate (Remove.file ~run_with output_vcf);
          ])
    in
    run_gatk_haplotype_caller
  in
  match how with
  | `Region region -> run_on_region ~add_edges:more_edges region
  | `Map_reduce ->
    let targets =
      List.map (Reference_genome.major_contigs reference)
        ~f:(run_on_region ~add_edges:[]) (* we add edges only to the last step *)
    in
    let final_vcf = result_prefix ^ "-merged.vcf" in
    Vcftools.vcf_concat ~run_with targets ~final_vcf ~more_edges

(** Call somatic variants with Mutect2.

    Mutect2 comes within the GATK (as opposed to {!Mutect}).

    Cf. also
    https://www.broadinstitute.org/gatk/guide/tooldocs/org_broadinstitute_gatk_tools_walkers_cancer_m2_MuTect2.php
*)
let mutect2
    ?(more_edges = [])
    ~configuration
    ~run_with
    ~input_normal_bam ~input_tumor_bam (* The doc says only one of each *)
    ~result_prefix how =
  let open KEDSL in
  let reference =
    Machine.get_reference_genome run_with
      input_normal_bam#product#reference_build in
  let run_on_region ~add_edges region =
    let result_file suffix =
      let region_name = Region.to_filename region in
      sprintf "%s-%s%s" result_prefix region_name suffix in
    let output_vcf = result_file "-mutect2.vcf" in
    let gatk = Machine.get_tool run_with Machine.Tool.Default.gatk in
    let run_path = Filename.dirname output_vcf in
    let reference_fasta = Reference_genome.fasta reference in
    let reference_dot_fai = Samtools.faidx ~run_with reference_fasta in
    let sequence_dict = Picard.create_dict ~run_with reference_fasta in
    let sorted_normal_bam =
      Samtools.sort_bam_if_necessary ~run_with ~by:`Coordinate input_normal_bam
    in
    let sorted_tumor_bam =
      Samtools.sort_bam_if_necessary ~run_with ~by:`Coordinate input_tumor_bam
    in
    let `Arguments config_arguments, `Edges confg_edges =
      Configuration.Mutect2.compile ~reference configuration in
    let run_caller =
      let name = sprintf "%s" (Filename.basename output_vcf) in
      let make =
        Machine.run_big_program run_with ~name
          ~self_ids:["gatk"; "mutect2"]
          Program.(
            Machine.Tool.(init gatk)
            && shf "mkdir -p %s" run_path
            && shf "cd %s" run_path
            && call_gatk ~region ~analysis:"MuTect2"
              ([ "-I:normal"; sorted_normal_bam#product#path;
                 "-I:tumor"; sorted_tumor_bam#product#path;
                 "-R"; reference_fasta#product#path;
                 "-o"; output_vcf; ]
               @ config_arguments)
          )
      in
      workflow_node ~name ~make
        (vcf_file output_vcf
           ~reference_build:input_normal_bam#product#reference_build
           ~host:Machine.(as_host run_with))
        ~tags:[Target_tags.variant_caller]
        ~edges:(add_edges @ confg_edges @ [
            depends_on Machine.Tool.(ensure gatk);
            depends_on sorted_normal_bam;
            depends_on sorted_tumor_bam;
            depends_on reference_fasta;
            depends_on reference_dot_fai;
            depends_on sequence_dict;
            depends_on (Samtools.index_to_bai ~run_with sorted_normal_bam);
            depends_on (Samtools.index_to_bai ~run_with sorted_tumor_bam);
            on_failure_activate (Remove.file ~run_with output_vcf);
          ])
    in
    run_caller
  in
  match how with
  | `Region region -> run_on_region ~add_edges:more_edges region
  | `Map_reduce ->
    let targets =
      List.map (Reference_genome.major_contigs reference)
        ~f:(run_on_region ~add_edges:[]) (* we add edges only to the last step *)
    in
    let final_vcf = result_prefix ^ "-merged.vcf" in
    Vcftools.vcf_concat ~run_with targets ~final_vcf ~more_edges
