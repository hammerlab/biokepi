
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

let to_unit x = fun ~var_count -> entity SP.(x ~var_count ^^ string ":> unit")

let list l =
  fun ~var_count ->
    SP.nest (OCaml.list (fun a -> a ~var_count) l)

let list_map l ~f =
  let open SP in
  fun ~var_count ->
    entity (
      string "List.map"
      ^^ nest (string "~f:" ^^ f ~var_count)
      ^^ l ~var_count
    )

include To_json.Make_serializer (struct
    type t = SP.t
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
    let string = SP.string
  end)


