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


open Biokepi_common

(** Positions are 1-based *)
type t = [
  | `Chromosome of string
  | `Chromosome_interval of string * int * int
  | `Full
]

(** Make a filename-compliant string out of a region specification. *)
let to_filename = function
| `Full -> "Full"
| `Chromosome s -> sprintf "Chr%s" s
| `Chromosome_interval (s, b, e) -> sprintf "Chr%s_%d-%d" s b e

let to_samtools_specification = function
| `Full -> None
| `Chromosome s -> Some s
| `Chromosome_interval (s, b, e) -> Some (sprintf "%s:%d-%d" s b e)

let to_samtools_option r =
  match to_samtools_specification r with
  | Some s -> sprintf "-r %s" s
  | None -> ""

let to_mutect_option r = 
  match to_samtools_specification r with
  | Some s -> sprintf "--intervals %s" s
  | None -> ""

let parse_samtools s =
  match String.split ~on:(`Character ':') s with
  | [] -> assert false
  | [one] -> `Chromosome one 
  | [one; two] -> 
    begin match String.split ~on:(`Character '-') two with
    | [left; right] -> 
      begin match Int.of_string left, Int.of_string right with
      | Some b, Some e -> `Chromosome_interval (one, b, e)
      | _ -> failwithf "Cannot parse %S into 2 loci" two
      end
    | _ -> failwithf "Not one '-' in %S" two
    end 
  | _ -> failwithf "Not one or zero ':' in %S" s


let cmdliner_term () =
  let open Cmdliner in
  Term.(
    pure (function
      | None -> `Full 
      | Some s -> parse_samtools s)
    $ Arg.(
        value & opt (some string) None
        & info ["R"; "region"] ~docv:"REGION"
          ~doc:"Specify a region; using samtools' format"
      )
  )

let all_chromosomes_b37 = [
  `Chromosome "1";
  `Chromosome "2";
  `Chromosome "3";
  `Chromosome "4";
  `Chromosome "5";
  `Chromosome "6";
  `Chromosome "7";
  `Chromosome "8";
  `Chromosome "9";
  `Chromosome "10";
  `Chromosome "11";
  `Chromosome "12";
  `Chromosome "13";
  `Chromosome "14";
  `Chromosome "15";
  `Chromosome "16";
  `Chromosome "17";
  `Chromosome "18";
  `Chromosome "19";
  `Chromosome "20";
  `Chromosome "21";
  `Chromosome "22";
  `Chromosome "X";
  `Chromosome "Y";
  `Chromosome "MT";
]

