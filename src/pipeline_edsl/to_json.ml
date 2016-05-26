open Nonstd
module Tools = Biokepi_bfx_tools

type json = Yojson.Basic.json

type 'a repr = var_count : int -> json

type 'a observation = json

let lambda f =
  fun ~var_count ->
    let var_name = sprintf "var%d" var_count in
    let var_repr = fun ~var_count -> `String var_name in
    let applied = f var_repr ~var_count:(var_count + 1) in
    `Assoc [
      "lambda", `String var_name;
      "body", applied;
    ]

let apply f v =
  fun ~var_count ->
    let func = f ~var_count in
    let arg = v ~var_count in
    `Assoc [
      "function", func;
      "argument", arg;
    ]

let observe f = f () ~var_count:0

let to_unit j = j


let list l =
  fun ~var_count ->
    `List (List.map ~f:(fun a -> a ~var_count) l)

let list_map l ~f =
  fun ~var_count ->
    `Assoc [
      "list-map", f ~var_count;
      "argument", l ~var_count;
    ]

module Make_serializer (How : sig
    type t
    val input_value :
      string -> (string * string) list -> var_count:int -> t
    val function_call :
      string -> (string * t) list -> t
    val string : string -> t
  end) = struct
  open How

  let fastq
      ~sample_name ?fragment_id ~r1 ?r2 () =
    input_value "fastq" [
      "sample_name", sample_name;
      "fragment_id", Option.value ~default:"NONE" fragment_id;
      "R1", r1;
      "R2", Option.value ~default:"NONE" r2;
    ]

  let fastq_gz
      ~sample_name ?fragment_id ~r1 ?r2 () =
    input_value "fastq.gz" [
      "sample_name", sample_name;
      "fragment_id", Option.value ~default:"NONE" fragment_id;
      "R1", r1;
      "R2", Option.value ~default:"NONE" r2;
    ]

  let bam ~path ?sorting ~reference_build () =
    input_value "bam" [
      "path", path;
      "sorting",
      Option.value_map ~default:"NONE" sorting
        ~f:(function `Coordinate -> "Coordinate" | `Read_name -> "Read-name");
      "reference_build", reference_build;
    ]

  let pair a b ~(var_count : int) =
    function_call "make-pair" [
      "first", a ~var_count;
      "second", b ~var_count;
    ]
  let pair_first p ~(var_count : int) =
    function_call "pair-first" [
      "pair", p ~var_count;
    ]
  let pair_second p ~(var_count : int) =
    function_call "pair-second" [
      "pair", p ~var_count;
    ]

  let aligner name conf_name ~reference_build fq : var_count: int -> How.t =
    fun ~var_count ->
      let fq_compiled = fq ~var_count in
      function_call name [
        "configuration", string conf_name;
        "reference_build", string reference_build;
        "input", fq_compiled;
      ]
  let one_to_one name conf_name bam =
    fun ~(var_count : int) ->
      let bamc = bam ~var_count in
      function_call name [
        "configuration", string conf_name;
        "input", bamc;
      ]

  let bwa_aln
      ?(configuration = Tools.Bwa.Configuration.Aln.default) =
    aligner "bwa-aln" (Tools.Bwa.Configuration.Aln.name configuration)

  let bwa_mem
      ?(configuration = Tools.Bwa.Configuration.Mem.default) =
    aligner "bwa-mem" (Tools.Bwa.Configuration.Mem.name configuration)

  let gunzip gz ~(var_count : int) =
    function_call "gunzip" ["input", gz ~var_count]

  let gunzip_concat gzl ~(var_count : int) =
    function_call "gunzip-concat" ["input-list", gzl ~var_count]

  let concat l ~(var_count : int) =
    function_call "concat" ["input-list", l ~var_count]

  let merge_bams bl ~(var_count : int) =
    function_call "merge-bams" ["input-list", bl ~var_count]

  let star ?(configuration = Tools.Star.Configuration.Align.default) =
    aligner "star" (Tools.Star.Configuration.Align.name configuration)

  let hisat ?(configuration = Tools.Hisat.Configuration.default_v1) =
    aligner "hisat" (configuration.Tools.Hisat.Configuration.name)

  let mosaik =
    aligner "mosaik" "default"

  let stringtie ?(configuration = Tools.Stringtie.Configuration.default) =
    one_to_one "stringtie" configuration.Tools.Stringtie.Configuration.name


  let indel_real_config (indel, target) =
    (sprintf "I%s-TC%s"
       indel.Tools.Gatk.Configuration.Indel_realigner.name
       target.Tools.Gatk.Configuration.Realigner_target_creator.name)

  let gatk_indel_realigner
      ?(configuration = Tools.Gatk.Configuration.default_indel_realigner) =
    one_to_one "gatk_indel_realigner" (indel_real_config configuration)


  let gatk_indel_realigner_joint
      ?(configuration = Tools.Gatk.Configuration.default_indel_realigner) =
    one_to_one "gatk_indel_realigner_joint" (indel_real_config configuration)

  let picard_mark_duplicates
      ?(configuration = Tools.Picard.Mark_duplicates_settings.default) =
    one_to_one
      "picard_mark_duplicates"
      configuration.Tools.Picard.Mark_duplicates_settings.name

  let gatk_bqsr ?(configuration = Tools.Gatk.Configuration.default_bqsr) =
    let (bqsr, preads) = configuration in
    one_to_one "gatk_bqsr"
      (sprintf "B%s-PR%s"
         bqsr.Tools.Gatk.Configuration.Bqsr.name
         preads.Tools.Gatk.Configuration.Print_reads.name)

  let seq2hla =
    one_to_one "seq2hla" "default"

  let fastqc =
    one_to_one "fastqc" "default"

  let vcf_annotate_polyphen reference_build =
    one_to_one "vcf_annotate_polyphen" "default"

  let isovar ?(configuration = Tools.Isovar.Configuration.default) reference_build vcf =
    one_to_one "isovar" "default"

  let topiary 
    ?(configuration = Tools.Topiary.Configuration.default) 
    reference_build vcf predictor =
    one_to_one "topiary" "default"

  let optitype how =
    one_to_one "optitype" (match how with `DNA -> "DNA" | `RNA -> "RNA")

  let gatk_haplotype_caller =
    one_to_one "gatk_haplotype_caller" "default"

  let bam_to_fastq ~sample_name ?fragment_id se_or_pe bam =
    fun ~(var_count : int) ->
      let bamc = bam ~var_count in
      function_call "bam_to_fastq" [
        "sample_name", string sample_name;
        "fragment_id",
        Option.value_map ~f:string ~default:(string "N/A") fragment_id;
        "endness", string (
          match se_or_pe with
          | `SE -> "SE"
          | `PE -> "PE"
        );
        "input", bamc;
      ]

  let variant_caller name conf_name ~normal ~tumor () ~(var_count : int) =
    function_call name [
      "configuration", string conf_name;
      "normal", normal ~var_count;
      "tumor", tumor ~var_count;
    ]

  let mutect ?(configuration = Tools.Mutect.Configuration.default) =
    variant_caller "mutect"
      configuration.Tools.Mutect.Configuration.name

  let mutect2 ?(configuration = Tools.Gatk.Configuration.Mutect2.default) =
    variant_caller "mutect2"
      configuration.Tools.Gatk.Configuration.Mutect2.name

  let somaticsniper ?(configuration = Tools.Somaticsniper.Configuration.default) =
    variant_caller "somaticsniper"
      configuration.Tools.Somaticsniper.Configuration.name

  let strelka ?(configuration = Tools.Strelka.Configuration.default) =
    variant_caller "strelka"
      configuration.Tools.Strelka.Configuration.name

  let varscan_somatic ?adjust_mapq =
    variant_caller "varscan_somatic"
      (Option.value_map ~f:(sprintf "amap%d") ~default:"default" adjust_mapq)

  let muse ?(configuration = Tools.Muse.Configuration.default `WGS) =
    variant_caller "muse"
      configuration.Tools.Muse.Configuration.name

  let virmid ?(configuration = Tools.Virmid.Configuration.default) =
    variant_caller "virmid"
      configuration.Tools.Virmid.Configuration.name

end

include Make_serializer (struct
    type t = json
    let input_value name kv =
      fun ~var_count ->
        `Assoc (
          ("input-value", `String name)
          :: List.map kv ~f:(fun (k, v) -> k, `String v)
        )
    let function_call name params =
      `Assoc ( ("function-call", `String name) :: params)
    let string s = `String s
  end)

