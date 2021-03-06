
open Nonstd
let failwithf fmt = ksprintf failwith fmt

type parameters = {
  color_input: name: string -> attributes: (string * string) list -> string option;
}
let default_parameters = {
  color_input = (fun ~name ~attributes -> None);
}

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
  let id_style = `Structural

  module Index_anything = struct
    (** This an implementation of an almost bijection: 'a -> int
        It compares values structurally (with [(=)]), and assigns an
        integer, unique over the execution of the program.
        
        It replaces [Hashtbl.hash] for which we were hitting annoying
        collisions.
    *)

    type e = E: 'a -> e
    let count = ref 0
    let nodes : (e * int) list ref = ref []
    let get v =
      match List.find !nodes ~f:(fun (ee, _) -> ee = E v) with
      | Some (_, i) -> i
      | None ->
        incr count;
        nodes := (E v, !count) :: !nodes;
        !count
  end

  let make_id a =
    match a, id_style with
    | _, `Unique
    | `Unique, _ ->
      incr node_count; sprintf "node%03d" !node_count
    | `Of v, `Structural ->
      Index_anything.get v |> sprintf "id%d"

  let arrow label points_to = {label; points_to}

  (* [id] is just an argument used for hashing an identifier *)
  let variable id name = `Variable (make_id (`Of id), name)
  let lambda varname expr = `Lambda (make_id (`Of (varname, expr)), varname, expr)
  let apply f v = `Apply (make_id (`Of (f,v)), f, v)
  let string s = `String s


  let node ?id ?(a = []) name l : t =
    let id =
      match id with
      | Some i -> i
      | None -> make_id (`Of (name, a, l))
    in
    `Node ({id; name; attributes = a}, l)

  let input_value ?(a = []) name : t =
    let id = make_id (`Of (name, a)) in
    `Input_value {id; name; attributes = a}

  let to_dot t ~parameters =
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
    let one o = [o] in
    let rec go =
      function
      | `Variable (_, s) as v ->
        let id = id_of v in
        sentence (
          string id ^^ dot_attributes [
            label_attribute (label s []);
            font_name `Mono;
            "shape", in_quotes "hexagon";
          ]
        )
        |> one
      | `Lambda (id, v, expr) ->
        [
          (* To be displayed subgraphs need to be called “clusterSomething” *)
          string "subgraph" ^^ ksprintf string "cluster_%s" id ^^ braces (
            (
              sentence (string "color=grey")
              :: sentence (string "style=rounded")
              :: sentence (string "penwidth=4")
              :: go (node ~id (sprintf "Lambda %s" v) [arrow "Expr" expr])
            )
            |> List.dedup |> separate empty
          ) ^-^ newline
        ]
      | `Apply (id, f, v) ->
        go (node ~id "Apply F(X)" [arrow "F" f; arrow "X" v])
      | `String s ->
        failwithf "`String %S -> should have been eliminated" s
      | `Input_value {id; name; attributes} ->
        let color =
          parameters.color_input ~name ~attributes
          |> Option.value ~default:"black"
        in
        sentence (
          string id ^^ dot_attributes [
            label_attribute (label name attributes);
            font_name `Mono;
            "shape", in_quotes "Mrecord";
            "color", in_quotes color;
          ]
        )
        |> one
      | `Node ({id; name; attributes}, trees) ->
        sentence (
          string id ^^ dot_attributes [
            label_attribute (label name attributes);
            font_name `Mono;
            "shape", in_quotes "Mrecord";
          ]
        )
        :: List.concat_map trees ~f:(fun {label; points_to} ->
            sentence (
              dot_arrow (id_of points_to) id ^^ dot_attributes [
                label_attribute label;
                font_size `Small;
              ]
            )
            :: go points_to
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
          ^-^ (go t |> List.dedup |> separate empty)
        ))
    in
    dot
end


type 'a repr = var_count: int -> Tree.t

type 'a observation = parameters: parameters -> SmartPrint.t

let lambda f =
  fun ~var_count ->
    let var_name = sprintf "Var_%d" var_count in
    (*
       Here is the hack that makes nodes containing variables “semi-unique.”

       [var_repr_fake] is a first version of the variable representation,
       that we feed to the the function [f] a first time.
       This resulting tree is used to create the real variable representation,
       the first argument [applied_once] will be hashed to create a
       unique-for-this-subtree identifier.

       Finally get the normal [applied] subtree and feed it to [Tree.lambda].
    *)
    let var_repr_fake = fun ~var_count -> Tree.string var_name in
    let applied_once = f var_repr_fake ~var_count:(var_count + 1) in
    let var_repr = fun ~var_count -> Tree.variable applied_once var_name in
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
