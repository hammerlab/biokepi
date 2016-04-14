
open Nonstd

module Tree = struct
  type box = { id: string; name : string; attributes: (string * string) list}
  (* type action = string *)
  type arrow = {
    label: string;
    points_to: t
  } and t = [
    | `Leaf of box
    | `Node of box * arrow list
  ]
  let node_count = ref 0
  let make_id () = incr node_count; sprintf "node%03d" !node_count
  let arrow label points_to = {label; points_to}
  let node name l : t = `Node ({id = make_id (); name; attributes = []}, l)
  let leaf ?(a = []) name : t = `Leaf {id = make_id (); name; attributes = a}

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
      | `Leaf {id; _} -> id
      | `Node ({id; _}, _) -> id in
    let rec go =
      function
      | `Leaf {id; name; attributes} ->
        let label =
          match attributes with
          | [] -> name
          | _ ->
            sprintf "{<f0>%s |<f1> %s\\l }" name
              (List.map attributes ~f:(fun (k,v) -> sprintf "%s: %s" k v)
               |> String.concat "\\l")
        in
        sentence (
          string id ^^ dot_attributes [
            label_attribute label;
            font_name `Mono;
            "shape", in_quotes "Mrecord";
          ]
        )
      | `Node ({id; name; attributes}, trees) ->
        sentence (
          string id ^^ dot_attributes [
            label_attribute name;
            font_name `Mono;
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
    SmartPrint.to_string 80 2 dot
end


type 'a repr = var_count: int -> Tree.t

type 'a observation = string

let lambda f =
  fun ~var_count ->
    let var_name = sprintf "Var%d" var_count in
    let var_repr = fun ~var_count -> Tree.leaf var_name in
    let applied = f var_repr ~var_count:(var_count + 1) in
    Tree.node "Lambda" [Tree.arrow var_name applied]

let apply f v =
  fun ~var_count ->
    let func = f ~var_count in
    let arg = v ~var_count in
    Tree.node "Apply" [Tree.arrow "f" func; Tree.arrow "arg" arg]

let observe f = f () ~var_count:0 |> Tree.to_dot

module List_repr = struct
  let make l =
    fun ~var_count ->
      Tree.node "List.make"
        (List.mapi ~f:(fun i a -> Tree.arrow (sprintf "L%d" i) (a ~var_count)) l)

  let map l ~f =
    fun ~var_count ->
      Tree.node "List.map" [
        Tree.arrow "list" (l ~var_count);
        Tree.arrow "function" (f ~var_count);
      ]
end

include To_json.Make_serializer (struct
    type t = Tree.t
    let input_value name a ~var_count =
      Tree.leaf name ~a
    let function_call name params =
      Tree.node name (List.map ~f:(fun (k,v) -> Tree.arrow k v) params)
    let string s = Tree.leaf s
  end)
