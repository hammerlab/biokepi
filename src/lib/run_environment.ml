(**************************************************************************)
(*  Copyright 2014, Sebastien Mondet <seb@mondet.org>                     *)
(*                                                                        *)
(*  Licensed under the Apache License, Version 2.0 (the "License");       *)
(*  you may not use this file except in compliance with the License.      *)
(*  You may obtain a copy of the License at                               *)
(*                                                                        *)
(*      http://www.apache.org/licenses/LICENSE-2.0                        *)
(*                                                                        *)
(*  Unless required by applicable law or agreed to in writing, software   *)
(*  distributed under the License is distributed on an "AS IS" BASIS,     *)
(*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or       *)
(*  implied.  See the License for the specific language governing         *)
(*  permissions and limitations under the License.                        *)
(**************************************************************************)


open Common

open KEDSL


module Tool = struct
  module Definition = struct
    include struct
      (* hack to remove warning from generated code, cf.
         https://github.com/whitequark/ppx_deriving/issues/41 *)
      [@@@ocaml.warning "-11"]
      type t = [
        | `Bwa of [ `V_0_7_10 ]
        (* A tool that is manually installed by retrieving source or binary.*)
        | `Custom of string * string      
      ] [@@deriving yojson, show, eq]
    end
    let custom name ~version = `Custom (name, version)
  end
  module Default = struct
    open Definition
    let bwa = `Bwa `V_0_7_10
    let samtools = custom "samtools" ~version:"1.1"
    let vcftools = custom "vcftools" ~version:"0.1.12b"
    let bedtools = custom "bedtools" ~version:"2.23.0"
    let somaticsniper = custom "somaticsniper" ~version:"1.0.3"
    let varscan = custom "varscan" ~version:"2.3.5"
    let picard = custom "picard" ~version:"1.127"
    let mutect = custom "mutect" ~version:"unknown"
    let gatk = custom "gatk" ~version:"unknown"
    let strelka = custom "strelka" ~version:"1.0.14"
    let virmid = custom "virmid" ~version:"1.1.1"
    let muse = custom "muse" ~version:"1.0b"
    let star = custom "star" ~version:"2.4.1d"
    let stringtie = custom "stringtie" ~version:"1.0.4"
    let cufflinks = custom "cufflinks" ~version:"2.2.1"
    let hisat = custom "hisat" ~version:"0.1.6-beta"
    let mosaik = custom "mosaik" ~version:"2.2.3"
    let kallisto = custom "kallisto" ~version:"0.42.3"
    let opam = custom "opam" ~version:"1.2.2"
  end
  type t = {
    definition: Definition.t;
    init: Program.t;
    ensure: phony_workflow;
  }
  let create ?init ?ensure definition = {
    definition;
    init =
      Option.value init
        ~default:(Program.shf "echo '%s: default init'"
                    (Definition.show definition));
    ensure =
      Option.value_map
        ensure
        ~f:KEDSL.forget_product
        ~default:(workflow_node nothing
                    ~name:(sprintf "%s-ensured" (Definition.show definition)));
  }
  let init t = t.init
  let ensure t = t.ensure

  module Kit = struct
    type tool = t
    type t = {
      tools: tool list;
    }
    let create tools = {tools}
    let get_exn t def =
      List.find t.tools
        ~f:(fun {definition; _ } -> Definition.equal definition def)
      |> Option.value_exn ~msg:(sprintf "Can't find tool %s"
                                  Definition.(show def))

  end
end

module Machine = struct

  type run_function = ?name:string -> ?processors:int -> Program.t ->
    Ketrew_pure.Target.Build_process.t

  type t = {
    name: string;
    ssh_name: string;
    host: Host.t;
    get_reference_genome: string -> Reference_genome.t;
    toolkit: Tool.Kit.t;
    quick_command: Program.t -> Ketrew_pure.Target.Build_process.t;
    run_program: run_function;
    work_dir: string;
  }
  let create
      ~ssh_name ~host ~get_reference_genome ~toolkit
      ~quick_command ~run_program ~work_dir  name =
    {name; ssh_name; toolkit; get_reference_genome; host;
     quick_command; run_program; work_dir}

  let name t = t.name
  let ssh_name t = t.ssh_name
  let as_host t = t.host
  let get_reference_genome t = t.get_reference_genome
  let get_tool t =
    Tool.Kit.get_exn t.toolkit
  let quick_command t = t.quick_command
  let run_program t = t.run_program
  let work_dir t = t.work_dir

end
