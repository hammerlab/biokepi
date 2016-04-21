
open Nonstd
let failwithf fmt = ksprintf failwith fmt

module Tree = struct
  type box = { id: string; name : string; attributes: (string * string) list}
  type arrow = {
    label: string;
    points_to: t
  } and t = [
    | `Variable of string * string
    | `Lambda of string * string * t
    | `Apply of string * t * t
    | `String of string
    | `Input_value of box
    | `Node of box * arrow list
  ]
  let node_count = ref 0
  let id_style = `Hash
  let make_id a =
    match a, id_style with
    | _, `Unique
    | `Unique, _ ->
      incr node_count; sprintf "node%03d" !node_count
    | `Of v, `Hash ->
      Hashtbl.hash v |> sprintf "id%d"

  let arrow label points_to = {label; points_to}

  let variable name = `Variable (make_id `Unique, name)
  let lambda varname expr = `Lambda (make_id `Unique, varname, expr)
  let apply f v = `Apply (make_id `Unique, f, v)
  let string s = `String s


  let node ?id ?(a = []) name l : t =
    let id =
      match id with
      | Some i -> i
      | None -> make_id `Unique
    in
    `Node ({id; name; attributes = a}, l)

  let input_value ?(a = []) name : t =
    let id = make_id (`Of (name, a)) in
    `Input_value {id; name; attributes = a}

  let to_dot t =
    let open SmartPrint in
    let semicolon = string ";" in
    let sentence sp = sp ^-^ semicolon ^-^ newline in
    let dot_attributes l =
      brakets (
        List.map l ~f:(fun (k, v) ->
            string k ^^ string "=" ^^ v) |> separate (semicolon)
      ) in
    let in_quotes s = ksprintf string "\"%s\"" s in
    let label_attribute lab = ("label", in_quotes lab) in
    let font_name `Mono =
      ("fontname", in_quotes "monospace") in
    let font_size =
      function
      | `Small -> ("fontsize", in_quotes "12")
      | `Default -> ("fontsize", in_quotes "16")
    in
    let dot_arrow src dest = string src ^^ string "->" ^^ string dest in
    let id_of =
      function
      | `Lambda (id, _, _) -> id
      | `Apply (id, _, _) -> id
      | `Variable (id, _) -> id
      | `String s -> assert false
      | `Input_value {id; _} -> id
      | `Node ({id; _}, _) -> id in
    let label name attributes =
      match attributes with
      | [] -> name
      | _ ->
        sprintf "{<f0>%s |<f1> %s\\l }" name
          (List.map attributes ~f:(fun (k,v) -> sprintf "%s: %s" k v)
           |> String.concat "\\l")
    in
    let rec go =
      function
      | `Variable (id, s) ->
        sentence (
          string id ^^ dot_attributes [
            label_attribute (label s []);
            font_name `Mono;
            "shape", in_quotes "hexagon";
          ]
        )
      | `Lambda (id, v, expr) ->
        go (node ~id (sprintf "Lambda %s" v) [arrow "Expr" expr])
      | `Apply (id, f, v) ->
        go (node ~id "Apply F(X)" [arrow "F" f; arrow "X" v])
      | `String s ->
        failwithf "`String %S -> should have been eliminated" s
      | `Input_value {id; name; attributes} ->
        sentence (
          string id ^^ dot_attributes [
            label_attribute (label name attributes);
            font_name `Mono;
            "shape", in_quotes "Mrecord";
          ]
        )
      | `Node ({id; name; attributes}, trees) ->
        sentence (
          string id ^^ dot_attributes [
            label_attribute (label name attributes);
            font_name `Mono;
            "shape", in_quotes "Mrecord";
          ]
        )
        ^-^ separate empty (
          List.map trees ~f:(fun {label; points_to} ->
              sentence (
                dot_arrow (id_of points_to) id ^^ dot_attributes [
                  label_attribute label;
                  font_size `Small;
                ]
              )
              ^-^ go points_to)
        )
    in
    let dot =
      string "digraph target_graph" ^^ braces (nest (
          sentence (
            string "graph" ^^ dot_attributes [
              "rankdir", in_quotes "LR";
              font_size `Default;
            ]
          )
          ^-^ go t
        ))
    in
    dot
end


type 'a repr = var_count: int -> Tree.t

type 'a observation = SmartPrint.t

let lambda f =
  fun ~var_count ->
    let var_name = sprintf "Var%d" var_count in
    let var_repr = fun ~var_count -> Tree.variable var_name in
    let applied = f var_repr ~var_count:(var_count + 1) in
    Tree.lambda var_name applied

let apply f v =
  fun ~var_count ->
    let func = f ~var_count in
    let arg = v ~var_count in
    Tree.apply func arg

let observe f = f () ~var_count:0 |> Tree.to_dot

let to_unit x = x

let list l =
  fun ~var_count ->
    Tree.node "List.make"
      (List.mapi ~f:(fun i a -> Tree.arrow (sprintf "L%d" i) (a ~var_count)) l)

let list_map l ~f =
  fun ~var_count ->
    Tree.node "List.map" [
      Tree.arrow "list" (l ~var_count);
      Tree.arrow "function" (f ~var_count);
    ]

include To_json.Make_serializer (struct
    type t = Tree.t

    let input_value name a ~var_count =
      Tree.input_value name ~a

    let function_call name params =
      let a, arrows =
        List.partition_map params ~f:(fun (k, v) ->
            match v with
            | `String s -> `Fst (k, s)
            | _ -> `Snd (k, v)
          ) in
      Tree.node ~a name (List.map ~f:(fun (k,v) -> Tree.arrow k v) arrows)

    let string s = Tree.string s
  end)
