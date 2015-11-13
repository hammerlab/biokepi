open Common


type t = {
  name: string;
  parameters: (string * string) list;
}

let to_json t: Yojson.Basic.json =
  let {name; parameters} = t in
  `Assoc [
    "name", `String name;
    "parameters",
    `Assoc (List.map parameters ~f:(fun (a, b) -> a, `String b));
  ]

let render {parameters; _} =
  List.concat_map parameters ~f:(fun (a,b) -> [a; b])
