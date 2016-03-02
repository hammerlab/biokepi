(* A workflow to make sure that we have opam. *)

let destination = "/local/stable/rozenl02/"

let () =
  let machine = Biokepi.Build_machine.create destination in
  let workflow = Biokepi.Opam.version machine in
  Ketrew.Client.submit_workflow workflow
