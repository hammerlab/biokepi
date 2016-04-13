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


module List_repr = struct
  let make l =
    fun ~var_count ->
      `List (List.map ~f:(fun a -> a ~var_count) l)

  let map l ~f =
    fun ~var_count ->
      `Assoc [
        "list-map", f ~var_count;
        "argument", l ~var_count;
      ]
end

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


  let bwa_aln
      ?(configuration = Tools.Bwa.Configuration.Aln.default)
      ~reference_build fq ~var_count =
    let fq_compiled = fq ~var_count in
    function_call "bwa-aln" [
      "configuration",
      Tools.Bwa.Configuration.Aln.name configuration |> string;
      "reference_build", string reference_build;
      "input", fq_compiled;
    ]

  let gunzip gz ~var_count =
    function_call "gunzip" ["input", gz ~var_count]

  let gunzip_concat gzl ~var_count =
    function_call "gunzip-concat" ["input-list", gzl ~var_count]

  let concat l ~var_count =
    function_call "concat" ["input-list", l ~var_count]

  let merge_bams bl ~var_count =
    function_call "merge-bams" ["input-list", bl ~var_count]

  let mutect ~configuration ~normal ~tumor ~var_count =
    function_call "mutect" [
      "configuration", string configuration.Tools.Mutect.Configuration.name;
      "normal", normal ~var_count;
      "tumor", tumor ~var_count;
    ]
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

