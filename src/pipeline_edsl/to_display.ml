
open Nonstd

module Tools = Biokepi_bfx_tools

module SP = SmartPrint
module OCaml = SmartPrint.OCaml
let entity sp = SP.(nest (parens  (sp)))

type 'a repr = var_count: int -> SP.t

type 'a observation = SP.t

let lambda f =
  fun ~var_count ->
    let var_name = sprintf "var%d" var_count in
    let var_repr = fun ~var_count -> SP.string var_name in
    let applied = f var_repr ~var_count:(var_count + 1) in
    entity SP.(string "λ" ^^ string var_name ^^ string "→" ^^ applied)

let apply f v =
  fun ~var_count ->
    entity (SP.separate SP.space [f ~var_count; v ~var_count])

let observe f = f () ~var_count:0

module List_repr = struct
  let make l =
    fun ~var_count ->
      SP.nest (OCaml.list (fun a -> a ~var_count) l)

  let map l ~f =
    let open SP in
    fun ~var_count ->
      entity (
        string "List.map"
        ^^ nest (string "~f:" ^^ f ~var_count)
        ^^ l ~var_count
      )
end

let input_value name kv =
  let open SP in
  fun ~var_count ->
    entity (
      ksprintf string "input-%s" name
      ^^ (OCaml.list 
                 (fun (k, v) -> parens (string k ^-^ string ":" ^^ string v))
                 kv)
    )

let function_call name params =
  let open SP in
  entity (
    ksprintf string "%s" name
    ^^ (OCaml.list
               (fun (k, v) -> parens (string k ^-^ string ":" ^^ v))
               params)
  )

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
    Tools.Bwa.Configuration.Aln.name configuration |> SP.string;
    "reference_build", SP.string reference_build;
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
    "configuration", SP.string  
      configuration.Tools.Mutect.Configuration.name;
    "normal", normal ~var_count;
    "tumor", tumor ~var_count;
  ]
