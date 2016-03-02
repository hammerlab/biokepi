(* A workflow to make sure that we have opam. *)

let destination = "/local/stable/rozenl02/"

let () =
  let conf = Ketrew.Configuration.load_exn ~profile:"dev3" `Guess in
  (*let you_dont_need_this () = failwith "You don't need this" in *)
  let machine =
    Biokepi.Build_machine.create 
      (*~gatk_jar_location:you_dont_need_this
      ~mutect_jar_location:you_dont_need_this*)
        destination
  in
  let workflow = Biokepi.Opam.version machine in
  Ketrew.Client.submit_workflow ~override_configuration:conf workflow
